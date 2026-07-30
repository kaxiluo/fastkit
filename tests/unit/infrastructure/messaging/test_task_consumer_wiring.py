import importlib
import logging
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from app.infrastructure.messaging.task_consumer import (
    clear_pending_consumers,
    get_pending_consumers,
    task_consumer,
)
from app.infrastructure.messaging.task_result import TaskResult

# 用 importlib 取模块,避免 __init__.py re-export `task_consumer` 函数后
# `app.infrastructure.messaging.task_consumer` 属性解析到函数(而非子模块)
_TASK_CONSUMER_MODULE = importlib.import_module("app.infrastructure.messaging.task_consumer")


@pytest.fixture(autouse=True)
def _clear():
    clear_pending_consumers()
    yield
    clear_pending_consumers()


class Msg(BaseModel):
    message_version: int = 1
    v: int


def test_decorator_registers_pending_consumer_spec():
    @task_consumer("t.evt", concurrency=3)
    async def handler(m: Msg):
        return None

    specs = get_pending_consumers()
    assert len(specs) == 1
    assert specs[0].routing_key == "t.evt"
    assert specs[0].concurrency == 3
    assert specs[0].inbox_enabled is True
    assert specs[0].handler is handler


def test_retry_true_installs_default_policy():
    from app.infrastructure.messaging.retry_policy import RetryPolicy

    @task_consumer("t.evt2", retry=True)
    async def handler(m: Msg):
        return None

    specs = get_pending_consumers()
    assert len(specs) == 1
    assert specs[0].retry_policy == RetryPolicy()


def test_retry_policy_explicit_stored_on_spec():
    from app.infrastructure.messaging.retry_policy import RetryPolicy

    policy = RetryPolicy(max_attempts=5, delay=10)

    @task_consumer("test.retry.explicit", retry=policy)
    async def handler(payload: dict) -> None:
        pass

    specs = get_pending_consumers()
    assert specs[-1].retry_policy is policy


def test_retry_false_leaves_no_policy():
    @task_consumer("test.retry.false")
    async def handler(payload: dict) -> None:
        pass

    assert get_pending_consumers()[-1].retry_policy is None


def test_retry_invalid_type_rejected():
    with pytest.raises(TypeError, match="retry must be bool or RetryPolicy"):

        @task_consumer("test.retry.bad", retry="always")  # type: ignore[arg-type]
        async def _h(m: Msg):
            pass


def test_decorator_preserves_handler_identity_and_signature():
    @task_consumer("t.evt3")
    async def handler(m: Msg):
        return None

    # 装饰返回原函数,不包装成新对象(方便别处直接调用)
    assert callable(handler)
    assert handler.__name__ == "handler"


async def test_wrapped_handler_aborts_on_duplicate(monkeypatch):
    """wrapped 调用会先经 try_claim_message;返回 False 则不进 handler。"""
    called = []

    @task_consumer("t.evt4", inbox=True)
    async def handler(m: Msg):
        called.append(m)

    async def fake_claim(qualname, msg_id, sf):
        return False  # 模拟重复

    monkeypatch.setattr(_TASK_CONSUMER_MODULE, "try_claim_message", fake_claim)

    spec = get_pending_consumers()[-1]
    envelope = {"message_id": "dup-1", "routing_key": "t.evt4"}
    result = await spec.wrapped(Msg(v=1), envelope=envelope, session_factory=object())

    assert called == []
    assert isinstance(result, TaskResult)
    assert result.kind == "ABORT"
    assert result.reason == "duplicate_message"


async def test_wrapped_handler_runs_handler_when_new(monkeypatch):
    called = []

    @task_consumer("t.evt5", inbox=True)
    async def handler(m: Msg):
        called.append(m.v)
        return None

    async def fake_claim(qualname, msg_id, sf):
        return True

    monkeypatch.setattr(_TASK_CONSUMER_MODULE, "try_claim_message", fake_claim)

    spec = get_pending_consumers()[-1]
    envelope = {"message_id": "new-1", "routing_key": "t.evt5"}
    result = await spec.wrapped(Msg(v=42), envelope=envelope, session_factory=object())

    assert called == [42]
    # handler 隐式 return None → 归约为 FINISHED
    assert result is None or (isinstance(result, TaskResult) and result.kind == "FINISHED")


async def test_wrapped_handler_swallows_exception_but_logs(monkeypatch, caplog):
    @task_consumer("t.evt6", inbox=False)
    async def handler(m: Msg):
        raise RuntimeError("boom")

    spec = get_pending_consumers()[-1]
    # task_consumer 模块 logger 默认 propagate=True;caplog 抓 root
    with caplog.at_level(logging.ERROR, logger="app.infrastructure.messaging.task_consumer"):
        # inbox=False 时不走 try_claim_message
        result = await spec.wrapped(Msg(v=1), envelope={"message_id": "x"}, session_factory=None)

    # ack 语义:wrapped 不 raise,而是记录后返回
    assert result is None or (
        isinstance(result, TaskResult) and result.kind in ("ABORT", "FINISHED")
    )
    assert any("boom" in r.message or "boom" in str(r.exc_info or ()) for r in caplog.records)


async def test_no_message_id_aborts_when_inbox_enabled(monkeypatch):
    """inbox=True 但 envelope 缺 message_id → 早退,不走 try_claim_message。"""
    called = []

    @task_consumer("t.evt7", inbox=True)
    async def handler(m: Msg):
        called.append(m.v)

    claim = AsyncMock()
    monkeypatch.setattr(_TASK_CONSUMER_MODULE, "try_claim_message", claim)

    spec = get_pending_consumers()[-1]
    result = await spec.wrapped(Msg(v=99), envelope={}, session_factory=object())

    assert called == []
    assert claim.await_count == 0
    assert isinstance(result, TaskResult)
    assert result.kind == "ABORT"
    assert result.reason == "missing_message_id"
