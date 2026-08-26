"""过载豁免重投:overload_exceptions 命中时重投不烧 attempts,独立计数有上限。"""

import pytest
from pydantic import BaseModel

from app.infrastructure.messaging.retry_policy import RetryPolicy
from app.infrastructure.messaging.task_consumer import (
    _build_wrapped,
    clear_pending_consumers,
    get_pending_consumers,
    task_consumer,
)


class _OverloadError(Exception):
    """模拟上游限流类异常。"""


class Msg(BaseModel):
    v: int


class _FakeDispatcher:
    def __init__(self):
        self.republished: list[tuple[str, dict]] = []
        self.dead_lettered: list[dict] = []

    async def republish_delayed(self, payload, *, original_queue, envelope):
        self.republished.append((original_queue, envelope))

    async def dead_letter(self, payload, *, envelope):
        self.dead_lettered.append(envelope)


@pytest.fixture(autouse=True)
def _clear():
    clear_pending_consumers()
    yield
    clear_pending_consumers()


def _register_handler(routing_key: str, policy: RetryPolicy):
    @task_consumer(routing_key, retry=policy, inbox=False)
    async def handler(m: Msg):
        raise _OverloadError("upstream throttling")

    spec = get_pending_consumers()[-1]
    return spec


async def _run(spec, envelope):
    dispatcher = _FakeDispatcher()
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher)
    result = await wrapped(Msg(v=1), envelope=envelope, session_factory=None)
    return result, dispatcher


async def test_overload_exception_does_not_burn_attempts():
    spec = _register_handler(
        "t.overload", RetryPolicy(max_attempts=3, overload_exceptions=(_OverloadError,))
    )
    result, dispatcher = await _run(spec, {"message_id": "m-1", "attempts": 1})

    assert result.kind == "ABORT"
    assert result.reason == "overload_retry_scheduled"
    assert len(dispatcher.republished) == 1
    queue, new_env = dispatcher.republished[0]
    assert queue == "t.overload"
    assert new_env["attempts"] == 1  # 未递增
    assert new_env["overload_retries"] == 1
    assert new_env["message_id"] != "m-1"
    assert new_env["original_message_id"] == "m-1"


async def test_overload_retries_accumulate_across_republishes():
    spec = _register_handler(
        "t.overload2", RetryPolicy(max_attempts=3, overload_exceptions=(_OverloadError,))
    )
    _, dispatcher = await _run(spec, {"message_id": "m-2", "attempts": 1, "overload_retries": 41})

    _, new_env = dispatcher.republished[0]
    assert new_env["overload_retries"] == 42
    assert new_env["attempts"] == 1


async def test_overload_cap_reached_falls_back_to_attempts_semantics():
    spec = _register_handler(
        "t.overload3",
        RetryPolicy(max_attempts=3, overload_exceptions=(_OverloadError,)),
    )
    result, dispatcher = await _run(
        spec,
        {"message_id": "m-3", "attempts": 1, "overload_retries": 100},  # 已达默认上限
    )

    # 回落常规分支 B:烧 attempts,不再递增 overload_retries
    assert result.reason == "retry_scheduled"
    _, new_env = dispatcher.republished[0]
    assert new_env["attempts"] == 2
    assert new_env["overload_retries"] == 100


async def test_overload_takes_precedence_over_attempts_exhaustion():
    """attempts 已达 max 但过载预算未尽:分支 O 优先于 DLQ,消息不因风暴被烧。"""
    spec = _register_handler(
        "t.overload7",
        RetryPolicy(max_attempts=3, overload_exceptions=(_OverloadError,)),
    )
    result, dispatcher = await _run(
        spec,
        {"message_id": "m-7", "attempts": 3, "overload_retries": 5},
    )

    assert result.reason == "overload_retry_scheduled"
    assert len(dispatcher.dead_lettered) == 0
    _, new_env = dispatcher.republished[0]
    assert new_env["attempts"] == 3  # attempts 耗尽也不 DLQ,靠豁免续命
    assert new_env["overload_retries"] == 6


async def test_overload_cap_and_attempts_exhausted_goes_to_dlq():
    spec = _register_handler(
        "t.overload4",
        RetryPolicy(max_attempts=3, overload_exceptions=(_OverloadError,)),
    )
    result, dispatcher = await _run(
        spec,
        {"message_id": "m-4", "attempts": 3, "overload_retries": 100},
    )

    assert result.reason == "dead_lettered"
    assert len(dispatcher.dead_lettered) == 1
    assert dispatcher.dead_lettered[0]["failure"]["type"].endswith("_OverloadError")


async def test_non_overload_exception_keeps_normal_retry():
    @task_consumer(
        "t.overload5",
        retry=RetryPolicy(max_attempts=3, overload_exceptions=(_OverloadError,)),
        inbox=False,
    )
    async def handler(m: Msg):
        raise ValueError("unrelated")

    spec = get_pending_consumers()[-1]
    result, dispatcher = await _run(spec, {"message_id": "m-5", "attempts": 1})

    assert result.reason == "retry_scheduled"
    _, new_env = dispatcher.republished[0]
    assert new_env["attempts"] == 2
    assert "overload_retries" not in new_env


async def test_no_overload_configured_behaves_as_before():
    spec = _register_handler("t.overload6", RetryPolicy(max_attempts=3))
    result, dispatcher = await _run(spec, {"message_id": "m-6", "attempts": 1})

    # 未配置 overload_exceptions:过载型异常也走常规 attempts 语义
    assert result.reason == "retry_scheduled"
    assert dispatcher.republished[0][1]["attempts"] == 2


async def test_original_message_id_chain_survives_parse_roundtrip():
    """断链修复回归:第二跳 envelope 经 parse_envelope 回读后追溯链仍在。"""
    from app.infrastructure.messaging.envelope import parse_envelope

    spec = _register_handler(
        "t.chain", RetryPolicy(max_attempts=3, overload_exceptions=(_OverloadError,))
    )
    # 第一跳:原始消息
    _, dispatcher = await _run(spec, {"message_id": "m-orig", "attempts": 1})
    first_env = dispatcher.republished[0][1]
    # 第二跳:模拟 broker 回流,headers 经 parse_envelope 解析
    second_in = parse_envelope(dict(first_env))
    assert second_in["original_message_id"] == "m-orig"
    _, dispatcher2 = await _run(spec, second_in)
    second_env = dispatcher2.republished[0][1]
    assert second_env["original_message_id"] == "m-orig"  # 第三跳不写错


async def test_task_timeout_not_matched_by_timeout_error_overload():
    """框架合成的 TaskTimeout 不应被 overload_exceptions=(TimeoutError,) 匹配:
    它是 handler 执行超时(业务失败),不是外部过载,须烧 attempts 走重试。"""
    from app.infrastructure.messaging.task_consumer import TaskTimeout

    @task_consumer(
        "t.overload8",
        retry=RetryPolicy(max_attempts=3, overload_exceptions=(TimeoutError,)),
        inbox=False,
    )
    async def handler(m: Msg):
        raise TaskTimeout("handler exceeded 1s")

    spec = get_pending_consumers()[-1]
    result, dispatcher = await _run(spec, {"message_id": "m-8", "attempts": 1})

    assert result.reason == "retry_scheduled"
    assert dispatcher.republished[0][1]["attempts"] == 2


async def test_plain_timeout_error_still_matches_overload():
    """业务抛的普通 TimeoutError 仍被 overload_exceptions=(TimeoutError,) 匹配。"""

    @task_consumer(
        "t.overload9",
        retry=RetryPolicy(max_attempts=3, overload_exceptions=(TimeoutError,)),
        inbox=False,
    )
    async def handler(m: Msg):
        raise TimeoutError("upstream call timed out")

    spec = get_pending_consumers()[-1]
    result, dispatcher = await _run(spec, {"message_id": "m-9", "attempts": 1})

    assert result.reason == "overload_retry_scheduled"
    assert dispatcher.republished[0][1]["attempts"] == 1
    assert dispatcher.republished[0][1]["overload_retries"] == 1
