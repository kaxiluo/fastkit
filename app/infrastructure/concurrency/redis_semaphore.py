"""跨副本并发信号量:Redis N 槽位 + Lua 原子 acquire/renew/release + 续租协程。

通用并发原语,不依赖 messaging;可被 consumer / scheduler / clients 复用。
仅支持单实例/主从 Redis:Lua 以 ``prefix .. ':' .. i`` 动态拼 key,Cluster 下跨 slot 非法。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from uuid import uuid4

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

log = structlog.get_logger(__name__)

# KEYS[1]=prefix; ARGV[1]=capacity ARGV[2]=token ARGV[3]=lease
_ACQUIRE_LUA = """
local prefix = KEYS[1]
local n = tonumber(ARGV[1])
local token = ARGV[2]
local lease = tonumber(ARGV[3])
for i = 1, n do
    if redis.call('SET', prefix .. ':' .. i, token, 'NX', 'EX', lease) then
        return i
    end
end
return -1
"""

# KEYS[1]=slot key; ARGV[1]=token ARGV[2]=lease
_RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return 0
"""

# KEYS[1]=slot key; ARGV[1]=token
_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class RedisSemaphore:
    def __init__(
        self,
        redis: Redis,
        *,
        key_prefix: str,
        capacity: int,
        lease_seconds: int,
        poll_interval: float,
    ) -> None:
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        self._redis = redis
        self._prefix = key_prefix
        self._capacity = capacity
        self._lease = lease_seconds
        self._poll = poll_interval
        self._renew_interval = max(lease_seconds / 2, 1)
        self._acquire_script = redis.register_script(_ACQUIRE_LUA)
        self._renew_script = redis.register_script(_RENEW_LUA)
        self._release_script = redis.register_script(_RELEASE_LUA)

    @property
    def poll_interval(self) -> float:
        return self._poll

    async def try_acquire(self, token: str) -> int:
        """单次尝试抢槽:>=0 为槽位号,-1 为满;RedisError 自然抛出(调用方区分
        quota 满与 Redis 故障两种信号)。"""
        result = await self._acquire_script(
            keys=[self._prefix], args=[self._capacity, token, self._lease]
        )
        return int(result)

    @asynccontextmanager
    async def slot(self, *, timeout: float | None = None) -> AsyncGenerator[int]:
        """申请一个槽位,成功后 yield 槽位号,退出时释放 + 取消续租。

        Args:
            timeout: 等待槽位的最大秒数;None 表示无上限(等死),传入则超时抛 TimeoutError。
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        token = uuid4().hex
        slot_i = await self.try_acquire(token)
        while slot_i < 0:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"semaphore slot timeout: {self._prefix}")
            await asyncio.sleep(self._poll)
            slot_i = await self.try_acquire(token)
        async with self.hold(slot_i, token):
            yield slot_i

    @asynccontextmanager
    async def hold(self, slot_i: int, token: str) -> AsyncGenerator[int]:
        """接管 try_acquire 已抢到的槽:启动续租,yield 槽位号,退出时停续租并释放。

        release 失败(RedisError)suppress + log:不能顶替业务异常、改变失败路由,
        配额靠 slot TTL 自然回收。
        """
        key = f"{self._prefix}:{slot_i}"
        renew = asyncio.create_task(self._renew_loop(key, token))
        try:
            yield slot_i
        finally:
            renew.cancel()
            with suppress(asyncio.CancelledError):
                await renew
            try:
                await self._release_script(keys=[key], args=[token])
            except RedisError:
                log.error("redis_semaphore.release_failed", key=key)

    async def probe(self) -> None:
        """启动探活:acquire + renew + release 完整一轮,覆盖 EVAL/EVALSHA/SET/EXPIRE/DEL
        与写权限(Redis ACL 只放行 PING 的"假活"会被这里抓住)。失败自然抛出。
        前几任进程的探针 key 未过期时等待至多一个 lease 再放弃(概率极低:crash 后
        lease 秒内重启)。"""
        token = uuid4().hex
        deadline = time.monotonic() + self._lease
        slot_i = await self.try_acquire(token)
        while slot_i < 0:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"semaphore probe acquire timeout: {self._prefix}")
            await asyncio.sleep(self._poll)
            slot_i = await self.try_acquire(token)
        key = f"{self._prefix}:{slot_i}"
        ok = await self._renew_script(keys=[key], args=[token, self._lease])
        if not ok:
            raise RuntimeError(f"semaphore probe renew failed: {key}")
        await self._release_script(keys=[key], args=[token])

    async def _renew_loop(self, key: str, token: str) -> None:
        while True:
            await asyncio.sleep(self._renew_interval)
            try:
                ok = await self._renew_script(keys=[key], args=[token, self._lease])
            except RedisError:
                # 网络类瞬时故障:本轮放弃、下一轮重试,lease TTL 内自愈不丢槽
                log.error("redis_semaphore.renew_redis_error", key=key)
                continue
            if not ok:
                # Lua 返回 0:key 过期或被他人抢占,租约真正丢失,告警后放弃
                log.error("redis_semaphore.lease_lost", key=key)
                return
