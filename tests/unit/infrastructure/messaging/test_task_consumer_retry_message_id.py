"""wrapper retry 时应换新 message_id,保留 original_message_id 作追溯。

避免 inbox=True + retry 不兼容(retry 共用 message_id 被判 duplicate)。
DLQ 走终态,沿用原 message_id 不变。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.infrastructure.messaging.retry_policy import RetryPolicy
from app.infrastructure.messaging.task_consumer import _build_wrapped, _ConsumerSpec


@dataclass
class _FakeDispatcher:
    republish_calls: list[dict] = field(default_factory=list)
    dead_letter_calls: list[dict] = field(default_factory=list)

    async def republish_delayed(self, payload: Any, *, original_queue: str, envelope: dict) -> None:
        self.republish_calls.append(
            {"payload": payload, "original_queue": original_queue, "envelope": envelope}
        )

    async def dead_letter(self, payload: Any, *, envelope: dict) -> None:
        self.dead_letter_calls.append({"payload": payload, "envelope": envelope})


class _NoOpSessionFactory:
    pass


async def test_retry_uses_fresh_message_id_and_preserves_original():
    async def handler(payload: dict) -> None:
        raise ValueError("boom")

    dispatcher = _FakeDispatcher()
    spec = _ConsumerSpec(
        routing_key="q.rot",
        handler=handler,
        wrapped=None,  # type: ignore[arg-type]
        concurrency=1,
        inbox_enabled=False,
        retry_policy=RetryPolicy(max_attempts=3, delay=30),
    )
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher)
    await wrapped(
        {"a": 1},
        envelope={"message_id": "orig-1", "routing_key": "q.rot", "attempts": 1},
        session_factory=_NoOpSessionFactory(),
    )

    assert len(dispatcher.republish_calls) == 1
    new_env = dispatcher.republish_calls[0]["envelope"]
    assert new_env["message_id"] != "orig-1"
    assert new_env["original_message_id"] == "orig-1"
    assert new_env["attempts"] == 2


async def test_retry_chain_accumulates_original_message_id():
    """第二次 retry 的 original_message_id 仍指向最初原始 message_id,不被覆盖。"""

    call_count = {"n": 0}

    async def handler(payload: dict) -> None:
        call_count["n"] += 1
        raise ValueError("always fails")

    dispatcher = _FakeDispatcher()
    spec = _ConsumerSpec(
        routing_key="q.chain",
        handler=handler,
        wrapped=None,  # type: ignore[arg-type]
        concurrency=1,
        inbox_enabled=False,
        retry_policy=RetryPolicy(max_attempts=5, delay=30),
    )
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher)

    # 第一次失败(attempts=1, 无 original_message_id)
    await wrapped(
        {"a": 1},
        envelope={"message_id": "root", "routing_key": "q.chain", "attempts": 1},
        session_factory=_NoOpSessionFactory(),
    )
    first_env = dispatcher.republish_calls[0]["envelope"]
    assert first_env["original_message_id"] == "root"

    # 第二次失败(envelope 已带 original_message_id="root",不该被新 message_id 覆盖)
    await wrapped(
        {"a": 1},
        envelope=first_env,  # attempts=2, message_id=新 UUID, original_message_id="root"
        session_factory=_NoOpSessionFactory(),
    )
    second_env = dispatcher.republish_calls[1]["envelope"]
    assert second_env["original_message_id"] == "root"
    assert second_env["message_id"] != first_env["message_id"]
    assert second_env["attempts"] == 3


async def test_dlq_keeps_latest_message_id_no_fresh_rotation() -> None:
    """DLQ 终态不再生成新 message_id,沿用入参 envelope 的最新值。"""

    async def handler(payload: dict) -> None:
        raise ValueError("boom")

    dispatcher = _FakeDispatcher()
    spec = _ConsumerSpec(
        routing_key="q.dlq",
        handler=handler,
        wrapped=None,  # type: ignore[arg-type]
        concurrency=1,
        inbox_enabled=False,
        retry_policy=RetryPolicy(max_attempts=3, delay=30),
    )
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=dispatcher)
    await wrapped(
        {"a": 1},
        envelope={"message_id": "last-attempt", "routing_key": "q.dlq", "attempts": 3},
        session_factory=_NoOpSessionFactory(),
    )

    assert len(dispatcher.dead_letter_calls) == 1
    dl = dispatcher.dead_letter_calls[0]["envelope"]
    assert dl["message_id"] == "last-attempt"
