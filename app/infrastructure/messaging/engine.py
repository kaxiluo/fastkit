"""Messaging 引擎门面:装配 broker + registry + relay + consumer 绑定。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from faststream.rabbit import Channel, RabbitBroker, RabbitMessage, RabbitQueue
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.messaging.dlq.topology import declare_dlq
from app.infrastructure.messaging.envelope import parse_envelope
from app.infrastructure.messaging.event import EventRegistry, get_registered_events
from app.infrastructure.messaging.outbox.publisher import TransactionalPublisher
from app.infrastructure.messaging.outbox.relay import relay_loop
from app.infrastructure.messaging.retry.dispatcher import RetryDispatcher
from app.infrastructure.messaging.retry.topology import declare_retry
from app.infrastructure.messaging.settings import MessagingSettings
from app.infrastructure.messaging.task_consumer import (
    _build_wrapped,
    _resolve_timeout,
    get_pending_consumers,
)

log = logging.getLogger(__name__)


class Messaging:
    def __init__(
        self,
        *,
        broker: RabbitBroker,
        session_factory: async_sessionmaker,
        settings: MessagingSettings,
    ):
        self._broker = broker
        self._session_factory = session_factory
        self._settings = settings

        self._publisher = TransactionalPublisher(service_name=settings.app_name)
        self._registry = EventRegistry(self._publisher)
        for meta in get_registered_events().values():
            self._registry.register(meta)

        self._relay_task: asyncio.Task | None = None
        self._shutdown = asyncio.Event()
        self._dispatcher: RetryDispatcher | None = None

    @property
    def registry(self) -> EventRegistry:
        return self._registry

    async def start_publishing_only(self) -> None:
        """API/scheduler 进程:只连 broker,不起 consumer/relay。"""
        await self._broker.connect()
        log.info("messaging.publisher_ready")

    async def start_consumers(self) -> None:
        """Worker 进程:声明 DLQ + retry 拓扑 → 绑定 consumer → start broker → 起 relay。"""
        await self._broker.connect()
        await declare_dlq(self._broker, self._settings)
        await declare_retry(self._broker, self._settings)
        self._dispatcher = RetryDispatcher(self._broker, self._settings)
        self._bind_pending_consumers()
        await self._broker.start()
        self._relay_task = asyncio.create_task(
            relay_loop(self._session_factory, self._broker, self._settings, self._shutdown),
            name="messaging.relay_loop",
        )
        log.info(
            "messaging.consumers_ready",
            extra={"count": len(get_pending_consumers())},
        )

    async def stop(self) -> None:
        self._shutdown.set()
        if self._relay_task is not None:
            try:
                await asyncio.wait_for(self._relay_task, timeout=10.0)
            except TimeoutError:
                self._relay_task.cancel()
                log.warning("messaging.relay_stop_timeout")
        await self._broker.stop()
        log.info("messaging.stopped")

    def _bind_pending_consumers(self) -> None:
        """把 _PENDING_CONSUMERS 里的 spec 逐个绑到 broker.subscriber;
        wrapper 重建以注入真实 dispatcher。"""
        dlx = self._settings.dlq_exchange
        assert self._dispatcher is not None  # start_consumers 中已构造
        for spec in get_pending_consumers():
            effective_timeout = _resolve_timeout(
                spec.timeout_override, self._settings.consumer_timeout_seconds
            )
            spec.wrapped = _build_wrapped(
                spec,
                inbox_enabled=spec.inbox_enabled,
                dispatcher=self._dispatcher,
                timeout=effective_timeout,
            )
            queue = RabbitQueue(
                spec.routing_key,
                durable=True,
                arguments={"x-dead-letter-exchange": dlx},
            )
            self._broker.subscriber(
                queue,
                channel=Channel(prefetch_count=spec.concurrency),
            )(
                _make_entry(spec.wrapped, self._session_factory)
            )
            log.info(
                "messaging.consumer_bound",
                extra={"routing_key": spec.routing_key, "concurrency": spec.concurrency},
            )


def _make_entry(
    wrapped: Callable,
    session_factory: async_sessionmaker,
):
    """FastStream subscriber 层入口:从 msg 拿 headers → parse_envelope → 调 wrapped。"""

    async def _entry(payload: dict, msg: RabbitMessage) -> None:
        envelope = parse_envelope(dict(msg.headers or {}))
        await wrapped(payload, envelope=envelope, session_factory=session_factory)

    return _entry
