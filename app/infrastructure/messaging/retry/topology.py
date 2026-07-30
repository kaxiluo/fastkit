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
            # queue-level TTL 作为消息在 retry queue 里的最大停留时间。
            # 注意:RabbitMQ 对 queue x-message-ttl 和 publish 时的 per-message
            # expiration 取小。RetryDispatcher.republish_delayed 会施加
            # [0.9, 1.0] * ttl 的 per-message expiration 做 jitter,这里始终是
            # 兜底上限 —— 即使 publish 端忘了传 expiration,消息也最迟在 ttl 后回流。
            "x-message-ttl": settings.retry_ttl_ms,
            "x-dead-letter-exchange": "",
            # 不设 x-dead-letter-routing-key → 消息保留原 routing_key,
            # dead-letter 后经 default exchange 回投到原业务 queue
        },
    )
    robust_ex = await broker.declare_exchange(retry_ex)
    robust_q = await broker.declare_queue(retry_q)
    await robust_q.bind(robust_ex, routing_key="")
