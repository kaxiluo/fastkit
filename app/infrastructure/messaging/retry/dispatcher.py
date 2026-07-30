"""RetryDispatcher:consumer wrapper 用来把消息重投到 retry queue 或 DLX。

不做重试兜底、不静默异常:publish 失败自然抛出,由 FastStream 层处理。
"""

from __future__ import annotations

import random
from typing import Any

from faststream.rabbit import RabbitBroker

from app.infrastructure.messaging.settings import MessagingSettings

# jitter 下限系数:实际 expiration = uniform(JITTER_FLOOR, 1.0) * retry_ttl_ms / 1000 秒。
# 不能 >1.0:RabbitMQ 对 queue x-message-ttl 与 per-message expiration 取小,
# >ttl 会被 queue TTL 截断失去 jitter 意义。
# 注意单位:FastStream/aio-pika publish 的 expiration 期望秒(int/float × 1000 → AMQP ms 字符串)。
JITTER_FLOOR = 0.9


class RetryDispatcher:
    def __init__(self, broker: RabbitBroker, settings: MessagingSettings):
        self._broker = broker
        self._settings = settings

    async def republish_delayed(
        self,
        payload: Any,
        *,
        original_queue: str,
        envelope: dict,
    ) -> None:
        """publish 到 messaging.retry(fanout);消息保留 routing_key=original_queue,
        per-message expiration 到期后 dead-letter,经 default exchange 回投原队列。

        每条消息独立施加 ``[JITTER_FLOOR, 1.0] * retry_ttl_ms / 1000`` 秒的 expiration,
        让一批同时进入 retry 的消息分散回流,避免下游被同一秒的 retry 风暴打爆。
        FastStream/aio-pika 的 expiration 单位是秒(int/float 会 ×1000 转为 AMQP ms 字符串)。
        """
        ttl_ms = self._settings.retry_ttl_ms
        expiration_s = random.uniform(JITTER_FLOOR, 1.0) * ttl_ms / 1000
        await self._broker.publish(
            payload,
            exchange=self._settings.retry_exchange,
            routing_key=original_queue,
            headers=envelope,
            expiration=expiration_s,
        )

    async def dead_letter(self, payload: Any, *, envelope: dict) -> None:
        """publish 到共享 DLX(fanout);envelope 中已带 failure.* 元信息。"""
        await self._broker.publish(
            payload,
            exchange=self._settings.dlq_exchange,
            routing_key="",
            headers=envelope,
        )
