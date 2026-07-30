"""RetryPolicy 单元测试:接口边界。"""

from __future__ import annotations

import pytest

from app.infrastructure.messaging.retry_policy import RetryPolicy


def test_default_policy():
    p = RetryPolicy()
    assert p.max_attempts == 3
    assert p.delay == 30
    assert p.backoff == "fixed"


def test_explicit_max_attempts_and_delay():
    p = RetryPolicy(max_attempts=5, delay=10)
    assert p.max_attempts == 5
    assert p.delay == 10


def test_max_attempts_zero_rejected():
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=0)


def test_max_attempts_negative_rejected():
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=-1)


def test_exponential_backoff_not_implemented():
    with pytest.raises(NotImplementedError, match="exponential"):
        RetryPolicy(backoff="exponential")


def test_policy_is_frozen():
    p = RetryPolicy()
    with pytest.raises((AttributeError, TypeError)):
        p.max_attempts = 5  # type: ignore[misc]
