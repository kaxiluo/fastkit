"""验证 retry 拓扑声明后可用。"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


async def test_declare_retry_creates_exchange_and_queue(broker, test_messaging_settings):
    """声明后 exchange + queue 应存在(幂等 declare 不抛 + 返回 robust 对象)。"""
    from app.infrastructure.messaging.retry.topology import declare_retry

    await declare_retry(broker, test_messaging_settings)

    # exchange:幂等再 declare,失败抛异常
    from faststream.rabbit import ExchangeType, RabbitExchange

    retry_ex = RabbitExchange(
        test_messaging_settings.retry_exchange,
        type=ExchangeType.FANOUT,
        durable=True,
    )
    robust_ex = await broker.declare_exchange(retry_ex)
    assert robust_ex is not None

    # queue:幂等再 declare
    from faststream.rabbit import RabbitQueue

    retry_q = RabbitQueue(
        test_messaging_settings.retry_queue,
        durable=True,
        arguments={
            "x-message-ttl": test_messaging_settings.retry_ttl_ms,
            "x-dead-letter-exchange": "",
        },
    )
    robust_q = await broker.declare_queue(retry_q)
    assert robust_q is not None
