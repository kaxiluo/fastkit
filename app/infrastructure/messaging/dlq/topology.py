"""DLQ 拓扑:一份共享 DLX(fanout) + 一份共享 DLQ。

业务队列在 @task_consumer 绑定 broker.subscriber 时通过 arguments
{"x-dead-letter-exchange": settings.dlq_exchange} 关联。
"""

from __future__ import annotations

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.infrastructure.messaging.settings import MessagingSettings


async def declare_dlq(broker: RabbitBroker, settings: MessagingSettings) -> None:
    dlx = RabbitExchange(
        settings.dlq_exchange,
        type=ExchangeType.FANOUT,
        durable=True,
    )
    dlq = RabbitQueue(settings.dlq_queue, durable=True)
    robust_dlx = await broker.declare_exchange(dlx)
    robust_dlq = await broker.declare_queue(dlq)
    # fanout DLX:queue 直接 bind,routing_key 无效但 API 要求
    await robust_dlq.bind(robust_dlx, routing_key="")
