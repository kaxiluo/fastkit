"""handler 超时行为单测:超时 → 取消 → 走 retry/DLQ;timeout=None 不启用;自抛 TimeoutError 不误判。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass
class _FakeDispatcher:
    republish_calls: list[dict] = field(default_factory=list)
    dead_letter_calls: list[dict] = field(default_factory=list)

    async def republish_delayed(self, payload: Any, *, original_queue: str, envelope: dict) -> None:
        self.republish_calls.append({"original_queue": original_queue, "envelope": envelope})

    async def dead_letter(self, payload: Any, *, envelope: dict) -> None:
        self.dead_letter_calls.append({"envelope": envelope})


def _spec(routing_key, handler, *, retry_policy):
    from app.infrastructure.messaging.task_consumer import _ConsumerSpec

    return _ConsumerSpec(
        routing_key=routing_key,
        handler=handler,
        wrapped=None,  # type: ignore[arg-type]
        concurrency=1,
        inbox_enabled=False,
        retry_policy=retry_policy,
    )


async def test_timeout_with_retry_policy_republishes():
    from app.infrastructure.messaging.retry_policy import RetryPolicy
    from app.infrastructure.messaging.task_consumer import _build_wrapped

    async def handler(payload: dict) -> None:
        await asyncio.sleep(1.0)

    dispatcher = _FakeDispatcher()
    spec = _spec("t.to.retry", handler, retry_policy=RetryPolicy(max_attempts=3, delay=30))
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher, timeout=0.05)

    await wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "t.to.retry", "attempts": 1},
        session_factory=None,
    )
    assert len(dispatcher.republish_calls) == 1
    assert dispatcher.republish_calls[0]["envelope"]["attempts"] == 2
    assert dispatcher.dead_letter_calls == []


async def test_timeout_without_retry_dead_letters_as_task_timeout():
    from app.infrastructure.messaging.task_consumer import _build_wrapped

    async def handler(payload: dict) -> None:
        await asyncio.sleep(1.0)

    dispatcher = _FakeDispatcher()
    spec = _spec("t.to.dlq", handler, retry_policy=None)
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher, timeout=0.05)

    await wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "t.to.dlq", "attempts": 1},
        session_factory=None,
    )
    assert dispatcher.republish_calls == []
    assert len(dispatcher.dead_letter_calls) == 1
    failure = dispatcher.dead_letter_calls[0]["envelope"]["failure"]
    assert failure["type"].endswith("TaskTimeout")
    assert "0.05" in failure["message"]


async def test_under_timeout_finishes():
    from app.infrastructure.messaging.task_consumer import _build_wrapped
    from app.infrastructure.messaging.task_result import TaskResult

    async def handler(payload: dict) -> None:
        await asyncio.sleep(0.01)

    dispatcher = _FakeDispatcher()
    spec = _spec("t.to.fast", handler, retry_policy=None)
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher, timeout=1.0)

    result = await wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "t.to.fast", "attempts": 1},
        session_factory=None,
    )
    assert isinstance(result, TaskResult) and result.kind == "FINISHED"
    assert dispatcher.republish_calls == [] and dispatcher.dead_letter_calls == []


async def test_timeout_none_disables_timeout():
    from app.infrastructure.messaging.task_consumer import _build_wrapped
    from app.infrastructure.messaging.task_result import TaskResult

    async def handler(payload: dict) -> None:
        await asyncio.sleep(0.1)

    dispatcher = _FakeDispatcher()
    spec = _spec("t.to.off", handler, retry_policy=None)
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher, timeout=None)

    result = await wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "t.to.off", "attempts": 1},
        session_factory=None,
    )
    assert isinstance(result, TaskResult) and result.kind == "FINISHED"
    assert dispatcher.dead_letter_calls == []


async def test_handler_raised_timeout_error_not_misjudged():
    """handler 自抛 builtin TimeoutError(未到 deadline)→ 走普通异常路径,failure.type 非 TaskTimeout。"""
    from app.infrastructure.messaging.task_consumer import _build_wrapped

    async def handler(payload: dict) -> None:
        raise TimeoutError("handler's own")

    dispatcher = _FakeDispatcher()
    spec = _spec("t.to.selfraise", handler, retry_policy=None)
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher, timeout=5.0)

    await wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "t.to.selfraise", "attempts": 1},
        session_factory=None,
    )
    assert len(dispatcher.dead_letter_calls) == 1
    ftype = dispatcher.dead_letter_calls[0]["envelope"]["failure"]["type"]
    assert ftype.endswith("TimeoutError")
    assert not ftype.endswith("TaskTimeout")
