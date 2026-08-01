"""验证 _build_wrapped 按 handler signature 传 session_factory / envelope。"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from app.infrastructure.messaging.task_consumer import (
    _build_wrapped,
    _ConsumerSpec,
    clear_pending_consumers,
    task_consumer,
)


class _Payload(BaseModel):
    message_version: int = 1
    v: int


@pytest.fixture(autouse=True)
def _clear():
    clear_pending_consumers()
    yield
    clear_pending_consumers()


async def test_handler_receives_only_payload_when_signature_narrow():
    seen = {}

    @task_consumer("t.narrow", inbox=False, retry=False)
    async def h(msg: _Payload) -> None:
        seen["v"] = msg.v

    spec = _get_spec("t.narrow")
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=None)
    await wrapped(
        {"message_version": 1, "v": 42},
        envelope={"attempts": 1, "message_id": "x"},
        session_factory=object(),
    )
    assert seen == {"v": 42}


async def test_handler_receives_session_factory_when_declared():
    seen = {}

    @task_consumer("t.wide", inbox=False, retry=False)
    async def h(msg: _Payload, *, session_factory) -> None:
        seen["sf"] = session_factory
        seen["v"] = msg.v

    sentinel = object()
    spec = _get_spec("t.wide")
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=None)
    await wrapped(
        {"message_version": 1, "v": 7},
        envelope={"attempts": 1, "message_id": "y"},
        session_factory=sentinel,
    )
    assert seen == {"sf": sentinel, "v": 7}


async def test_handler_receives_envelope_when_declared():
    seen = {}

    @task_consumer("t.env", inbox=False, retry=False)
    async def h(msg: _Payload, *, envelope) -> None:
        seen["env"] = envelope
        seen["v"] = msg.v

    spec = _get_spec("t.env")
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=None)
    env = {"attempts": 1, "message_id": "z"}
    await wrapped(
        {"message_version": 1, "v": 9},
        envelope=env,
        session_factory=object(),
    )
    assert seen["env"] == env
    assert seen["v"] == 9


async def test_handler_receives_integrations_when_declared():
    seen = {}

    @task_consumer("t.integ", inbox=False, retry=False)
    async def h(msg: _Payload, *, integrations) -> None:
        seen["integrations"] = integrations
        seen["v"] = msg.v

    sentinel = object()
    spec = _get_spec("t.integ")
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=None)
    await wrapped(
        {"message_version": 1, "v": 3},
        envelope={"attempts": 1, "message_id": "i"},
        session_factory=object(),
        integrations=sentinel,
    )
    assert seen == {"integrations": sentinel, "v": 3}


async def test_spec_detects_integrations_param():
    @task_consumer("t.integ.detect", inbox=False, retry=False)
    async def h(msg: _Payload, *, integrations) -> None:
        pass

    assert _get_spec("t.integ.detect").accepts_integrations is True


def _get_spec(routing_key: str) -> _ConsumerSpec:
    from app.infrastructure.messaging.task_consumer import get_pending_consumers

    for s in get_pending_consumers():
        if s.routing_key == routing_key:
            return s
    raise AssertionError(f"spec {routing_key!r} not registered")
