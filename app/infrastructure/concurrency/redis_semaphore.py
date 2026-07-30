"""跨副本并发信号量:Redis N 槽位 + Lua 原子 acquire/renew/release + 续租协程。

通用并发原语,不依赖 messaging;可被 consumer / scheduler / clients 复用。
机制源自 industrious_framework ConcurrencyLimiter,新增 token 归属守卫。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from uuid import uuid4

from redis.asyncio import Redis

log = logging.getLogger(__name__)

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
        self._acquire_script = redis.register_script(_ACQUIRE_LUA)
        self._renew_script = redis.register_script(_RENEW_LUA)
        self._release_script = redis.register_script(_RELEASE_LUA)

    async def _try_acquire(self, token: str) -> int:
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
        slot_i = await self._try_acquire(token)
        while slot_i < 0:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"semaphore slot timeout: {self._prefix}")
            await asyncio.sleep(self._poll)
            slot_i = await self._try_acquire(token)
        key = f"{self._prefix}:{slot_i}"
        renew = asyncio.create_task(self._renew_loop(key, token))
        try:
            yield slot_i
        finally:
            renew.cancel()
            with suppress(asyncio.CancelledError):
                await renew
            await self._release_script(keys=[key], args=[token])

    async def _renew_loop(self, key: str, token: str) -> None:
        interval = max(self._lease / 2, 1)
        while True:
            await asyncio.sleep(interval)
            ok = await self._renew_script(keys=[key], args=[token, self._lease])
            if not ok:
                log.warning("redis_semaphore.lease_lost", extra={"key": key})
                return
