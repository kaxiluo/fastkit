from __future__ import annotations

from app.infrastructure.ratelimit.settings import RateLimitSettings


def test_ratelimit_poll_default():
    assert RateLimitSettings().poll_interval_seconds == 0.05


def test_ratelimit_max_wait_default():
    assert RateLimitSettings().max_wait_seconds == 5.0
