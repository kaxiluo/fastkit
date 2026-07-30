"""engine 绑定阶段:UNSET 用全局默认、显式值覆盖;经 spec.wrapped 端到端验证超时生效。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from app.infrastructure.messaging.engine import Messaging
from app.infrastructure.messaging.settings import MessagingSettings
from app.infrastructure.messaging.task_consumer import (
    clear_pending_consumers,
    get_pending_consumers,
    task_consumer,
)

_BROKER = "amqp://guest:guest@localhost/"


@pytest.fixture(autouse=True)
def _clear():
    clear_pending_consumers()
    yield
    clear_pending_consumers()


class _FakeBroker:
    """只实现 _bind_pending_consumers 用到的 subscriber();不连真 broker。"""

    def subscriber(self, queue, channel=None):
        def _register(fn):
            return fn

        return _register


@dataclass
class _FakeDispatcher:
    dead_letter_calls: list[dict] = field(default_factory=list)

    async def dead_letter(self, payload: Any, *, envelope: dict) -> None:
        self.dead_letter_calls.append({"envelope": envelope})

    async def republish_delayed(self, payload: Any, *, original_queue: str, envelope: dict) -> None:  # noqa: E501
        raise AssertionError("不应走到 republish")


def _make_messaging(settings: MessagingSettings) -> Messaging:
    m = Messaging(broker=_FakeBroker(), session_factory=object(), settings=settings)  # type: ignore[arg-type]
    m._dispatcher = _FakeDispatcher()  # type: ignore[attr-defined]
    return m


async def test_unset_binds_global_default_timeout():
    @task_consumer("t.bind.default", inbox=False)
    async def h(payload: dict) -> None:
        await asyncio.sleep(1.0)

    settings = MessagingSettings(
        broker_url=_BROKER, app_name="fastkit", consumer_timeout_seconds=0.05
    )
    m = _make_messaging(settings)
    m._bind_pending_consumers()  # type: ignore[attr-defined]

    spec = get_pending_consumers()[-1]
    await spec.wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "t.bind.default", "attempts": 1},
        session_factory=None,
    )
    assert len(m._dispatcher.dead_letter_calls) == 1  # type: ignore[attr-defined]
    assert m._dispatcher.dead_letter_calls[0]["envelope"]["failure"]["type"].endswith("TaskTimeout")  # type: ignore[attr-defined]


async def test_explicit_override_beats_global_default():
    @task_consumer("t.bind.override", inbox=False, timeout=0.05)
    async def h(payload: dict) -> None:
        await asyncio.sleep(1.0)

    # 全局默认很大;若覆盖未生效则不会超时
    settings = MessagingSettings(
        broker_url=_BROKER, app_name="fastkit", consumer_timeout_seconds=999.0
    )
    m = _make_messaging(settings)
    m._bind_pending_consumers()  # type: ignore[attr-defined]

    spec = get_pending_consumers()[-1]
    await spec.wrapped(
        {"a": 1},
        envelope={"message_id": "m1", "routing_key": "t.bind.override", "attempts": 1},
        session_factory=None,
    )
    assert len(m._dispatcher.dead_letter_calls) == 1  # type: ignore[attr-defined]
