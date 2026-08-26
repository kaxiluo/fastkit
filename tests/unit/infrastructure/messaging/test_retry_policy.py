"""RetryPolicy 单元测试:接口边界。"""

from __future__ import annotations

import inspect

import pytest

from app.infrastructure.messaging.retry_policy import RetryPolicy


def test_default_policy():
    p = RetryPolicy()
    assert p.max_attempts == 3
    assert p.backoff == "fixed"


def test_explicit_max_attempts():
    p = RetryPolicy(max_attempts=5)
    assert p.max_attempts == 5


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


def test_delay_field_removed():
    """防御性测试:delay 是误导性 dead parameter(实际 TTL 由 retry queue 的
    x-message-ttl 控制),确保不会被无意加回。"""
    sig = inspect.signature(RetryPolicy)
    assert "delay" not in sig.parameters


def test_retry_policy_defaults_no_overload_exemptions():
    policy = RetryPolicy()
    assert policy.overload_exceptions == ()
    assert policy.overload_retry_limit == 100


def test_retry_policy_rejects_overload_limit_below_one():
    with pytest.raises(ValueError, match="overload_retry_limit"):
        RetryPolicy(overload_retry_limit=0)
