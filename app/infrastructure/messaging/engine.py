"""Messaging 引擎门面:装配 broker + registry + relay + consumer 绑定。"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable

import structlog
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage, RabbitQueue
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.concurrency.capacity_announcer import CapacityAnnouncer
from app.infrastructure.concurrency.redis_semaphore import RedisSemaphore
from app.infrastructure.messaging.dlq.topology import declare_dlq
from app.infrastructure.messaging.envelope import parse_envelope
from app.infrastructure.messaging.event import EventRegistry, get_registered_events
from app.infrastructure.messaging.outbox.publisher import TransactionalPublisher
from app.infrastructure.messaging.outbox.relay import supervised_relay_loop
from app.infrastructure.messaging.retry.dispatcher import RetryDispatcher
from app.infrastructure.messaging.retry.topology import declare_retry
from app.infrastructure.messaging.settings import MessagingSettings
from app.infrastructure.messaging.task_consumer import (
    SLOT_LEASE_SECONDS,
    SLOT_POLL_INTERVAL_SECONDS,
    _build_wrapped,
    _resolve_timeout,
    _resolve_wait_timeout,
    get_pending_consumers,
)

log = structlog.get_logger(__name__)


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
        self._integrations: object = None
        self._databases: object = None
        self._redis: object = None
        self._semaphores: dict[str, RedisSemaphore] = {}
        self._announcer: CapacityAnnouncer | None = None

    @property
    def registry(self) -> EventRegistry:
        return self._registry

    async def start_publishing_only(self) -> None:
        """API/scheduler 进程:只连 broker,不起 consumer/relay。"""
        await self._broker.connect()
        log.info("messaging.publisher_ready")

    async def start_consumers(
        self,
        integrations: object = None,
        databases: object = None,
        redis: object = None,
        *,
        start_broker: bool = True,
    ) -> None:
        """Worker 进程:探活 Redis 闸 → 声明 DLQ + retry 拓扑 → 绑定 consumer
        → 容量声明 → start broker → 起 relay。

        全局并发闸硬依赖 Redis(fail-closed):redis 缺失或探针失败都拒绝启动,
        不给假活。start_broker=False 供 ASGI 组合路径用(见原注释,保留)。
        """
        self._integrations = integrations
        self._databases = databases
        self._redis = redis
        if self._redis is None:
            raise ValueError("start_consumers requires a redis client (global concurrency gate)")
        await self._probe_semaphore_support()
        await self._broker.connect()
        await declare_dlq(self._broker, self._settings)
        await declare_retry(self._broker, self._settings)
        self._dispatcher = RetryDispatcher(self._broker, self._settings)
        self._bind_pending_consumers()
        self._announcer = CapacityAnnouncer(
            self._redis,  # type: ignore[arg-type]
            capacities={
                f"{self._settings.app_name}.concurrency.{spec.routing_key}": spec.concurrency
                for spec in get_pending_consumers()
            },
        )
        await self._announcer.declare_and_check()
        self._announcer.start()
        if start_broker:
            await self._broker.start()
        self._relay_task = asyncio.create_task(
            supervised_relay_loop(
                self._session_factory, self._broker, self._settings, self._shutdown
            ),
            name="messaging.relay_supervisor",
        )
        log.info(
            "messaging.consumers_ready",
            count=len(get_pending_consumers()),
        )

    async def _probe_semaphore_support(self) -> None:
        """fail-closed 启动探活:专用探针前缀 acquire+renew+release 一轮,覆盖
        EVAL/EVALSHA/SET/EXPIRE/DEL 与写权限(Redis ACL 只放行 PING 的假活会被
        抓住)。探针前缀与业务 slot 池隔离,业务满载不误判。失败抛异常拒绝启动。"""
        probe = RedisSemaphore(
            self._redis,  # type: ignore[arg-type]
            key_prefix=f"{self._settings.app_name}.concurrency.__probe__",
            capacity=1,
            lease_seconds=2,
            poll_interval=0.1,
        )
        await probe.probe()

    async def stop(self) -> None:
        self._shutdown.set()
        if self._relay_task is not None:
            try:
                # 这个 timeout 与 broker.shutdown_grace_seconds(给消费者 drain)
                # 语义不同:这里只是等 relay 协程在 _shutdown event 触发后自然退出。
                # relay 主循环每次 poll 间隔 outbox_poll_interval_seconds(默认 3s),
                # 给 10s 是 generous 兜底;正常情况下百毫秒级就会结束。超时则 cancel,
                # 让上层 broker.stop() / lifespan 接管。
                await asyncio.wait_for(self._relay_task, timeout=10.0)
            except TimeoutError:
                self._relay_task.cancel()
                log.warning("messaging.relay_stop_timeout")
        if self._announcer is not None:
            await self._announcer.stop()
            self._announcer = None
        await self._broker.stop()
        log.info("messaging.stopped")

    def _bind_pending_consumers(self) -> None:
        """把 _PENDING_CONSUMERS 里的 spec 逐个绑到 broker.subscriber;
        wrapper 重建以注入真实 dispatcher。"""
        dlx = self._settings.dlq_exchange
        assert self._dispatcher is not None  # start_consumers 中已构造
        seen: set[str] = set()
        for spec in get_pending_consumers():
            if spec.routing_key in seen:
                raise ValueError(
                    f"duplicate @task_consumer registration for routing_key "
                    f"{spec.routing_key!r}: same-queue double binding splits "
                    f"deliveries; concurrency gate and capacity declaration "
                    f"are keyed by routing_key"
                )
            seen.add(spec.routing_key)
            effective_timeout = _resolve_timeout(
                spec.timeout_override, self._settings.consumer_timeout_seconds
            )
            effective_wait_timeout = _resolve_wait_timeout(
                spec.wait_timeout_override,
                spec.timeout_override,
                self._settings.consumer_timeout_seconds,
            )
            semaphore = RedisSemaphore(
                self._redis,  # type: ignore[arg-type]
                key_prefix=f"{self._settings.app_name}.concurrency.{spec.routing_key}",
                capacity=spec.concurrency,
                lease_seconds=SLOT_LEASE_SECONDS,
                poll_interval=SLOT_POLL_INTERVAL_SECONDS,
            )
            self._semaphores[spec.routing_key] = semaphore
            spec.wrapped = _build_wrapped(
                spec,
                inbox_enabled=spec.inbox_enabled,
                dispatcher=self._dispatcher,
                timeout=effective_timeout,
                wait_timeout=effective_wait_timeout,
                semaphore=semaphore,
            )
            queue = RabbitQueue(
                spec.routing_key,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": dlx,
                    # 冻结 20min 换投与"无限排队"均以 classic 队列为前提;
                    # 显式声明防 vhost 默认漂移(quorum 的 delivery-limit 语义
                    # 会使两者失效),与现存 classic 队列重声明等价
                    "x-queue-type": "classic",
                },
            )
            self._broker.subscriber(
                queue,
                # prefetch 按副本数摊薄到 ceil(concurrency/replica_count)+1:
                # "+1" 让每副本本地多留 1 条缓冲,槽位释放时本地有货,免 broker
                # round-trip 空转;集群总本地 buffer ≈ concurrency + 副本数。真正
                # 的全局上限仍由 RedisSemaphore 闸保证。replica_count 缺省 1 →
                # prefetch=concurrency+1。
                channel=Channel(
                    prefetch_count=_resolve_prefetch(spec.concurrency, self._settings.replica_count)
                ),
            )(
                _make_entry(
                    spec.wrapped,
                    self._session_factory,
                    self._integrations,
                    self._databases,
                    self._redis,
                )
            )
            log.info(
                "messaging.consumer_bound",
                routing_key=spec.routing_key,
                concurrency=spec.concurrency,
            )


def _resolve_prefetch(concurrency: int, replica_count: int) -> int:
    """单副本 prefetch = ceil(concurrency / replica_count) + 1。

    集群总本地 buffer ≈ concurrency + 副本数:"喂饱闸"之外每副本多留 1 条
    缓冲,槽位释放时本地有货,免 broker round-trip 空转。多 pre 的幅度受控
    (每副本 +1),远小于"每副本 pre 到 concurrency"的闸外排队放大。
    replica_count 由 WORKER_REPLICAS 派生,缺省 1。
    """
    return max(1, math.ceil(concurrency / replica_count)) + 1


def _make_entry(
    wrapped: Callable,
    session_factory: async_sessionmaker,
    integrations: object,
    databases: object,
    redis: object,
):
    """FastStream subscriber 层入口:从 msg 拿 headers → parse_envelope → 调 wrapped。"""

    async def _entry(payload: dict, msg: RabbitMessage) -> None:
        envelope = parse_envelope(dict(msg.headers or {}))
        await wrapped(
            payload,
            envelope=envelope,
            session_factory=session_factory,
            integrations=integrations,
            databases=databases,
            redis=redis,
            # 窄回调而非透传整个 msg:冻结上限换投需要 nack(requeue),
            # 其余确认语义仍交给 FastStream auto-ack。已验证手动 nack 后
            # auto-ack 触发的 MessageProcessError 会被 acknowledgement middleware
            # 捕获仅记日志,不会二次 ack
            nack=msg.nack,
        )

    return _entry
