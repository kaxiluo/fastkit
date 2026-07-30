"""RetryDispatcher:consumer wrapper 用来把消息重投到 retry queue 或 DLX。

不做重试兜底、不静默异常:publish 失败自然抛出,由 FastStream 层处理。
"""

from __future__ import annotations

from typing import Any

from faststream.rabbit import RabbitBroker

from app.infrastructure.messaging.settings import MessagingSettings


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
        30s TTL 后 dead-letter 经 default exchange 回投原队列。"""
        await self._broker.publish(
            payload,
            exchange=self._settings.retry_exchange,
            routing_key=original_queue,
            headers=envelope,
        )

    async def dead_letter(self, payload: Any, *, envelope: dict) -> None:
        """publish 到共享 DLX(fanout);envelope 中已带 failure.* 元信息。"""
        await self._broker.publish(
            payload,
            exchange=self._settings.dlq_exchange,
            routing_key="",
            headers=envelope,
        )
