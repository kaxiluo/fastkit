"""限流组件:limits MovingWindow 薄封装。

两种语义:
  allow()   —— 超限即拒(429 语义),不阻塞。
  acquire() —— 满了就等,可选超时(对外 QPS 语义),用 reset_time 智能等待。
"""

from __future__ import annotations

import asyncio
import time

from limits import RateLimitItem
from limits.aio.strategies import MovingWindowRateLimiter


class RateLimiter:
    def __init__(
        self,
        strategy: MovingWindowRateLimiter,
        *,
        namespace: str,
        default_poll: float,
        max_wait: float,
    ) -> None:
        self._strategy = strategy
        self._namespace = namespace
        self._default_poll = default_poll
        self._max_wait = max_wait

    async def allow(self, item: RateLimitItem, *keys: str) -> bool:
        return await self._strategy.hit(item, self._namespace, *keys)

    async def acquire(
        self,
        item: RateLimitItem,
        *keys: str,
        timeout: float | None = None,
        poll: float | None = None,
    ) -> bool:
        deadline = None if timeout is None else time.monotonic() + timeout
        while not await self._strategy.hit(item, self._namespace, *keys):
            if deadline is not None and time.monotonic() >= deadline:
                return False
            stats = await self._strategy.get_window_stats(item, self._namespace, *keys)
            # reset_time 是 wall clock,wait 也按 wall clock 算;再封顶 max_wait 防时钟漂移睡过头
            wait = min(max(0.0, stats.reset_time - time.time()), self._max_wait)
            if deadline is not None:
                wait = min(wait, max(0.0, deadline - time.monotonic()))
            await asyncio.sleep(wait if wait > 0 else (poll or self._default_poll))
        return True
