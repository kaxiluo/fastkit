from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from app.infrastructure.messaging.event import (
    EventConflictError,
    EventMeta,
    EventPublisher,
    EventRegistry,
    event,
    get_registered_events,
)


class FakeSession:
    """占位 session;EventPublisher 只把参数往 publisher 转发,不需要真 session。"""


@pytest.fixture(autouse=True)
def _clear_registry():
    from app.infrastructure.messaging.event import _EVENT_REGISTRY

    _EVENT_REGISTRY.clear()
    yield
    _EVENT_REGISTRY.clear()


def test_event_decorator_registers_meta():
    @event("canary.ping")
    class Ping(BaseModel):
        message_version: int = 1
        id: str

    meta = get_registered_events()["canary.ping"]
    assert isinstance(meta, EventMeta)
    assert meta.routing_key == "canary.ping"
    assert meta.aggregate == "canary"
    assert meta.schema is Ping
    assert meta.schema_version == 1


def test_event_uses_schema_default_version():
    @event("orders.paid")
    class Paid(BaseModel):
        message_version: int = 3
        order_id: str

    assert get_registered_events()["orders.paid"].schema_version == 3


def test_event_conflict_raises_at_decoration():
    @event("x.y")
    class First(BaseModel):
        message_version: int = 1

    with pytest.raises(EventConflictError, match="x.y"):

        @event("x.y")
        class Second(BaseModel):
            message_version: int = 1


def test_registry_attribute_access_converts_dots_to_underscores():
    @event("orders.line_created")
    class LC(BaseModel):
        message_version: int = 1

    publisher_stub = AsyncMock()
    reg = EventRegistry(publisher_stub)
    reg.register(get_registered_events()["orders.line_created"])

    ep = reg.orders_line_created
    assert isinstance(ep, EventPublisher)
    assert ep.meta.routing_key == "orders.line_created"


def test_registry_unknown_attribute_raises():
    reg = EventRegistry(AsyncMock())
    with pytest.raises(AttributeError, match="no event"):
        _ = reg.nonexistent


async def test_publisher_forwards_to_backend():
    @event("t.evt")
    class E(BaseModel):
        message_version: int = 1
        v: int

    publisher_stub = AsyncMock()
    reg = EventRegistry(publisher_stub)
    reg.register(get_registered_events()["t.evt"])

    session = FakeSession()
    payload = E(v=42)
    await reg.t_evt.publish(session, payload, correlation_id="cid")

    publisher_stub.publish.assert_awaited_once()
    call_kwargs = publisher_stub.publish.await_args.kwargs
    call_args = publisher_stub.publish.await_args.args
    # publisher.publish(session, meta, payload, *, correlation_id, causation_id)
    assert call_args[0] is session
    assert call_args[1].routing_key == "t.evt"
    assert call_args[2] is payload
    assert call_kwargs == {"correlation_id": "cid", "causation_id": None}
