"""Retry 拓扑:共享单档 TTL 延迟队列。

消息 publish 到 `messaging.retry` (fanout) → 落入 `retry.30s`;
TTL 到期后 dead-letter,消息保留原 routing_key,DLX="" 让消息经 default exchange
精准回投到原业务 queue。
"""

from __future__ import annotations

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.infrastructure.messaging.settings import MessagingSettings


async def declare_retry(broker: RabbitBroker, settings: MessagingSettings) -> None:
    retry_ex = RabbitExchange(
        settings.retry_exchange,
        type=ExchangeType.FANOUT,
        durable=True,
    )
    retry_q = RabbitQueue(
        settings.retry_queue,
        durable=True,
        arguments={
            "x-message-ttl": settings.retry_ttl_ms,
            "x-dead-letter-exchange": "",
            # 不设 x-dead-letter-routing-key → 消息保留原 routing_key,
            # dead-letter 后经 default exchange 回投到原业务 queue
        },
    )
    robust_ex = await broker.declare_exchange(retry_ex)
    robust_q = await broker.declare_queue(retry_q)
    await robust_q.bind(robust_ex, routing_key="")
