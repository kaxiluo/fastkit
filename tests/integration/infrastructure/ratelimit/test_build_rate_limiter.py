"""build_rate_limiter:从 settings 造出可用的 RateLimiter(真 redis)。"""

from __future__ import annotations

from uuid import uuid4

import pytest
from limits import RateLimitItemPerSecond

from app.infrastructure.ratelimit import RateLimiter, build_rate_limiter
from app.infrastructure.ratelimit.settings import RateLimitSettings

pytestmark = pytest.mark.integration


async def test_build_returns_working_limiter(
    test_settings,
    test_redis_settings,
):
    rl = build_rate_limiter(
        redis_settings=test_redis_settings,
        app_settings=test_settings,
        ratelimit_settings=RateLimitSettings(),
    )
    assert isinstance(rl, RateLimiter)
    item = RateLimitItemPerSecond(1, 1)
    key = uuid4().hex
    assert await rl.allow(item, key) is True
    assert await rl.allow(item, key) is False
