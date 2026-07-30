"""RetryDispatcher 单测:验证 publish 到正确 exchange + routing_key。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class _FakeBroker:
    calls: list[dict] = field(default_factory=list)

    async def publish(
        self,
        payload,
        *,
        exchange=None,
        routing_key=None,
        headers=None,
        expiration=None,
        **kw,
    ):
        self.calls.append(
            {
                "payload": payload,
                "exchange": exchange,
                "routing_key": routing_key,
                "headers": headers,
                "expiration": expiration,
            }
        )


@dataclass
class _FakeSettings:
    retry_exchange: str = "messaging.retry"
    dlq_exchange: str = "dlx"
    retry_ttl_ms: int = 30000


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


async def test_republish_delayed_applies_jittered_expiration_in_range():
    """P1.a:每条消息施加 [0.9*ttl, ttl] 秒的 per-message expiration,防惊群。
    RabbitMQ 对 queue x-message-ttl 与 per-message expiration 取小,
    所以 expiration 上限不能超过 ttl(超过会被截断失去 jitter 意义)。
    FastStream/aio-pika 的 expiration 单位是秒(int/float 会 ×1000 转为 AMQP ms 字符串)。"""
    from app.infrastructure.messaging.retry.dispatcher import RetryDispatcher

    broker = _FakeBroker()
    settings = _FakeSettings(retry_ttl_ms=30000)
    dispatcher = RetryDispatcher(broker, settings)

    for _ in range(50):
        await dispatcher.republish_delayed(
            {"foo": "bar"},
            original_queue="test.q",
            envelope={"message_id": "abc", "attempts": 1},
        )

    expirations = [c["expiration"] for c in broker.calls]
    # 范围约束:[0.9*ttl/1000, ttl/1000] = [27.0, 30.0](秒)
    assert all(27.0 <= e <= 30.0 for e in expirations), expirations
    # 真的抖动了:50 次里至少 >1 个不同值
    assert len(set(expirations)) > 1


async def test_republish_delayed_expiration_scales_with_ttl():
    """改 retry_ttl_ms 后 expiration 范围随之缩放,不是硬编码 30s。"""
    from app.infrastructure.messaging.retry.dispatcher import RetryDispatcher

    broker = _FakeBroker()
    settings = _FakeSettings(retry_ttl_ms=10000)  # 10s
    dispatcher = RetryDispatcher(broker, settings)

    await dispatcher.republish_delayed(
        {"foo": "bar"},
        original_queue="test.q",
        envelope={"message_id": "abc", "attempts": 1},
    )

    expiration = broker.calls[0]["expiration"]
    # [0.9*10000/1000, 10000/1000] = [9.0, 10.0](秒)
    assert 9.0 <= expiration <= 10.0
