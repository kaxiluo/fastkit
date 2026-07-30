"""RateLimiter:limits MovingWindow 薄封装(真 redis)。"""

from __future__ import annotations

import time
from uuid import uuid4

import pytest
from limits import RateLimitItemPerSecond
from limits.aio.strategies import MovingWindowRateLimiter
from limits.storage import storage_from_string

from app.infrastructure.ratelimit.limiter import RateLimiter
from app.infrastructure.redis.settings import RedisSettings

pytestmark = pytest.mark.integration


def _limiter(test_redis_settings: RedisSettings) -> RateLimiter:
    url = test_redis_settings.url.get_secret_value()
    storage = storage_from_string(f"async+{url}", implementation="redispy")
    return RateLimiter(
        MovingWindowRateLimiter(storage), namespace="rltest", default_poll=0.02, max_wait=5.0
    )


async def test_allow_rejects_over_limit(test_redis_settings):
    rl = _limiter(test_redis_settings)
    item = RateLimitItemPerSecond(1, 1)
    key = uuid4().hex
    assert await rl.allow(item, key) is True
    assert await rl.allow(item, key) is False


async def test_every_n_seconds_x_times(test_redis_settings):
    rl = _limiter(test_redis_settings)
    item = RateLimitItemPerSecond(2, 2)  # 每 2 秒 2 次
    key = uuid4().hex
    assert await rl.allow(item, key) is True
    assert await rl.allow(item, key) is True
    assert await rl.allow(item, key) is False


async def test_acquire_timeout_returns_false(test_redis_settings):
    rl = _limiter(test_redis_settings)
    item = RateLimitItemPerSecond(1, 1)
    key = uuid4().hex
    assert await rl.acquire(item, key) is True  # 消耗掉唯一名额
    assert await rl.acquire(item, key, timeout=0.1) is False  # 0.1s 内拿不到


async def test_acquire_waits_then_succeeds(test_redis_settings):
    rl = _limiter(test_redis_settings)
    item = RateLimitItemPerSecond(1, 1)
    key = uuid4().hex
    assert await rl.acquire(item, key) is True
    start = time.monotonic()
    assert await rl.acquire(item, key, timeout=3.0) is True  # 窗口重置后拿到
    assert time.monotonic() - start >= 0.5


async def test_namespace_and_keys_isolated(test_redis_settings):
    rl = _limiter(test_redis_settings)
    item = RateLimitItemPerSecond(1, 1)
    assert await rl.allow(item, "key-A") is True
    assert await rl.allow(item, "key-B") is True  # 不同 key 独立
