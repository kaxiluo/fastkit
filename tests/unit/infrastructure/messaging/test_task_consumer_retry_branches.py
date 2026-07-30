"""wrapper 异常三分支单测:成功 / 重投 / DLQ。用 fake dispatcher + fake session。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class _FakeDispatcher:
    republish_calls: list[dict] = field(default_factory=list)
    dead_letter_calls: list[dict] = field(default_factory=list)

    async def republish_delayed(self, payload: Any, *, original_queue: str, envelope: dict) -> None:
        self.republish_calls.append(
            {
                "payload": payload,
                "original_queue": original_queue,
                "envelope": envelope,
            }
        )

    async def dead_letter(self, payload: Any, *, envelope: dict) -> None:
        self.dead_letter_calls.append({"payload": payload, "envelope": envelope})


class _NoOpSessionFactory:
    """inbox 被禁用时无需真正 session_factory。"""


async def test_success_returns_finished():
    from app.infrastructure.messaging.task_consumer import _build_wrapped, _ConsumerSpec
    from app.infrastructure.messaging.task_result import TaskResult

    async def handler(payload: dict) -> None:
        return None

    dispatcher = _FakeDispatcher()
    spec = _ConsumerSpec(
        routing_key="q",
        handler=handler,
        wrapped=None,  # type: ignore[arg-type]
        concurrency=1,
        inbox_enabled=False,
        retry_policy=None,
    )
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher)
    result = await wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "q", "attempts": 1},
        session_factory=_NoOpSessionFactory(),
    )
    assert isinstance(result, TaskResult) and result.kind == "FINISHED"
    assert dispatcher.republish_calls == []
    assert dispatcher.dead_letter_calls == []


async def test_failure_below_max_attempts_republishes():
    from app.infrastructure.messaging.retry_policy import RetryPolicy
    from app.infrastructure.messaging.task_consumer import _build_wrapped, _ConsumerSpec

    async def handler(payload: dict) -> None:
        raise ValueError("boom")

    dispatcher = _FakeDispatcher()
    spec = _ConsumerSpec(
        routing_key="test.retry.below_max",
        handler=handler,
        wrapped=None,  # type: ignore[arg-type]
        concurrency=1,
        inbox_enabled=False,
        retry_policy=RetryPolicy(max_attempts=3),
    )
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher)
    await wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "test.retry.below_max", "attempts": 1},
        session_factory=_NoOpSessionFactory(),
    )
    assert len(dispatcher.republish_calls) == 1
    call = dispatcher.republish_calls[0]
    assert call["original_queue"] == "test.retry.below_max"
    assert call["envelope"]["attempts"] == 2
    assert dispatcher.dead_letter_calls == []


async def test_failure_at_max_attempts_dead_letters():
    from app.infrastructure.messaging.retry_policy import RetryPolicy
    from app.infrastructure.messaging.task_consumer import _build_wrapped, _ConsumerSpec

    async def handler(payload: dict) -> None:
        raise ValueError("boom")

    dispatcher = _FakeDispatcher()
    spec = _ConsumerSpec(
        routing_key="test.retry.at_max",
        handler=handler,
        wrapped=None,  # type: ignore[arg-type]
        concurrency=1,
        inbox_enabled=False,
        retry_policy=RetryPolicy(max_attempts=3),
    )
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher)
    await wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "test.retry.at_max", "attempts": 3},
        session_factory=_NoOpSessionFactory(),
    )
    assert dispatcher.republish_calls == []
    assert len(dispatcher.dead_letter_calls) == 1
    dl = dispatcher.dead_letter_calls[0]
    assert dl["envelope"]["attempts"] == 3
    assert dl["envelope"]["failure"]["type"].endswith("ValueError")
    assert "boom" in dl["envelope"]["failure"]["message"]
    assert dl["envelope"]["failure"]["at"]  # ISO8601 非空


async def test_failure_without_retry_policy_dead_letters_immediately():
    from app.infrastructure.messaging.task_consumer import _build_wrapped, _ConsumerSpec

    async def handler(payload: dict) -> None:
        raise RuntimeError("no retry")

    dispatcher = _FakeDispatcher()
    spec = _ConsumerSpec(
        routing_key="test.retry.none",
        handler=handler,
        wrapped=None,  # type: ignore[arg-type]
        concurrency=1,
        inbox_enabled=False,
        retry_policy=None,
    )
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher)
    await wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "test.retry.none", "attempts": 1},
        session_factory=_NoOpSessionFactory(),
    )
    assert dispatcher.republish_calls == []
    assert len(dispatcher.dead_letter_calls) == 1


async def test_dispatcher_none_falls_back_to_ack_only(caplog):
    """dispatcher=None(装饰阶段)→ handler 异常降级为仅 ack + ABORT。"""
    import logging

    from app.infrastructure.messaging.task_consumer import _build_wrapped, _ConsumerSpec
    from app.infrastructure.messaging.task_result import TaskResult

    async def handler(payload: dict) -> None:
        raise RuntimeError("test.fallback mode")

    spec = _ConsumerSpec(
        routing_key="test.fallback",
        handler=handler,
        wrapped=None,  # type: ignore[arg-type]
        concurrency=1,
        inbox_enabled=False,
        retry_policy=None,
    )
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=None)
    with caplog.at_level(logging.ERROR, logger="app.infrastructure.messaging.task_consumer"):
        result = await wrapped(
            {"a": 1},
            envelope={"message_id": "m1", "routing_key": "test.fallback", "attempts": 1},
            session_factory=_NoOpSessionFactory(),
        )
    assert isinstance(result, TaskResult) and result.kind == "ABORT"
    assert result.reason == "handler_exception"
