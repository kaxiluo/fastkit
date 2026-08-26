"""容量存活声明:per-worker 键声明每个并发闸的容量,支撑滚动发布一致性告警。

全局并发调小时的生效语义是"旧副本全部退出后生效":slot 池按 key 名共享,
新旧副本容量不同时全局上界 = max(新旧)。存活声明让启动期能发现不一致并
warning(不拒绝启动——拒绝会死锁 K8s rollout:新容量起不来、旧容量未退)。
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from uuid import uuid4

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

log = structlog.get_logger(__name__)

DECLARATION_TTL_SECONDS = 90
REDECLARE_INTERVAL_SECONDS = 30


class CapacityAnnouncer:
    """每副本为每个 semaphore 前缀写 ``{prefix}.capacity.{worker_id}``(值 = 容量)。

    SET EX 90s,运行期每 30s 重申,优雅退出 DEL;进程 kill -9 靠 TTL 回收。
    worker_id 为进程随机短 id(同机多进程也不撞)。
    """

    def __init__(self, redis: Redis, *, capacities: dict[str, int]) -> None:
        self._redis = redis
        self._capacities = capacities
        self._worker_id = uuid4().hex[:8]
        self._task: asyncio.Task | None = None

    def _key(self, prefix: str) -> str:
        return f"{prefix}.capacity.{self._worker_id}"

    async def declare_and_check(self) -> None:
        """启动:写自己的声明 + 扫描同前缀存活声明,容量不一致 warning。"""
        for prefix, capacity in self._capacities.items():
            await self._redis.set(self._key(prefix), capacity, ex=DECLARATION_TTL_SECONDS)
        for prefix, capacity in self._capacities.items():
            live: set[str] = set()
            async for key in self._redis.scan_iter(match=f"{prefix}.capacity.*", count=100):
                value = await self._redis.get(key)
                if value is not None:
                    live.add(value.decode() if isinstance(value, bytes) else str(value))
            others = live - {str(capacity)}
            if others:
                log.warning(
                    "concurrency.capacity_mismatch",
                    key_prefix=prefix,
                    local_capacity=capacity,
                    other_capacities=sorted(others),
                )

    def start(self) -> None:
        self._task = asyncio.create_task(
            self._redeclare_loop(), name="concurrency.capacity_announcer"
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        for prefix in self._capacities:
            try:
                await self._redis.delete(self._key(prefix))
            except RedisError:
                # 优雅退出 best-effort;键靠 TTL 过期兜底
                log.warning("concurrency.capacity_declare_delete_failed")

    async def _redeclare_loop(self) -> None:
        while True:
            await asyncio.sleep(REDECLARE_INTERVAL_SECONDS)
            for prefix, capacity in self._capacities.items():
                try:
                    await self._redis.set(self._key(prefix), capacity, ex=DECLARATION_TTL_SECONDS)
                except RedisError:
                    # 网络类故障不放弃:本轮跳过,下一轮重申(TTL 90s 内自愈)
                    log.error("concurrency.capacity_redeclare_failed", key_prefix=prefix)
