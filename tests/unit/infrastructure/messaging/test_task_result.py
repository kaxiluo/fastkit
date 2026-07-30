from dataclasses import FrozenInstanceError

import pytest

from app.infrastructure.messaging.retry_policy import RetryPolicy
from app.infrastructure.messaging.task_result import TaskResult


def test_finished_has_no_reason():
    r = TaskResult.FINISHED()
    assert r.kind == "FINISHED"
    assert r.reason is None


def test_abort_carries_reason():
    r = TaskResult.ABORT("duplicate_message")
    assert r.kind == "ABORT"
    assert r.reason == "duplicate_message"


def test_task_result_is_frozen():
    r = TaskResult.FINISHED()
    with pytest.raises(FrozenInstanceError):
        r.kind = "ABORT"  # type: ignore


def test_retry_policy_default_ok():
    p = RetryPolicy()
    assert p.max_attempts == 3


def test_retry_policy_supports_multi_attempts():
    p = RetryPolicy(max_attempts=5)
    assert p.max_attempts == 5
