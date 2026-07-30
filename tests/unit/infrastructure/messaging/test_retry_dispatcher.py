"""RetryDispatcher 单测:验证 publish 到正确 exchange + routing_key。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class _FakeBroker:
    calls: list[dict] = field(default_factory=list)

    async def publish(self, payload, *, exchange=None, routing_key=None, headers=None, **kw):
        self.calls.append(
            {
                "payload": payload,
                "exchange": exchange,
                "routing_key": routing_key,
                "headers": headers,
            }
        )


@dataclass
class _FakeSettings:
    retry_exchange: str = "messaging.retry"
    dlq_exchange: str = "dlx"


async def test_republish_delayed_targets_retry_exchange_with_original_queue():
    from app.infrastructure.messaging.retry.dispatcher import RetryDispatcher

    broker = _FakeBroker()
    dispatcher = RetryDispatcher(broker, _FakeSettings())
    envelope = {"message_id": "abc", "attempts": 2}
    await dispatcher.republish_delayed(
        {"foo": "bar"},
        original_queue="test.q",
        envelope=envelope,
    )
    assert len(broker.calls) == 1
    call = broker.calls[0]
    assert call["exchange"] == "messaging.retry"
    assert call["routing_key"] == "test.q"
    assert call["headers"] == envelope
    assert call["payload"] == {"foo": "bar"}


async def test_dead_letter_targets_dlx_with_empty_routing_key():
    from app.infrastructure.messaging.retry.dispatcher import RetryDispatcher

    broker = _FakeBroker()
    dispatcher = RetryDispatcher(broker, _FakeSettings())
    envelope = {
        "message_id": "abc",
        "attempts": 3,
        "failure": {"type": "ValueError", "message": "boom", "at": "..."},
    }
    await dispatcher.dead_letter({"payload": 1}, envelope=envelope)
    assert len(broker.calls) == 1
    call = broker.calls[0]
    assert call["exchange"] == "dlx"
    assert call["routing_key"] == ""
    assert call["headers"] == envelope
