"""wait_timeout 三级解析:显式值 / 跟随 timeout / timeout=None 回落全局默认。"""

from __future__ import annotations

import pytest

from app.infrastructure.messaging.task_consumer import (
    _UNSET,
    ConcurrencyWaitTimeout,
    _resolve_wait_timeout,
)


def test_unset_and_timeout_unset_uses_global_default():
    assert _resolve_wait_timeout(_UNSET, _UNSET, 180.0) == 180.0


def test_unset_follows_explicit_timeout():
    assert _resolve_wait_timeout(_UNSET, 30.0, 180.0) == 30.0


def test_unset_with_timeout_none_falls_back_to_default():
    """timeout=None(执行不限时)时等待仍必须有限:回落全局默认。"""
    assert _resolve_wait_timeout(_UNSET, None, 180.0) == 180.0


def test_explicit_wait_timeout_beats_everything():
    assert _resolve_wait_timeout(5.0, 999.0, 180.0) == 5.0


def test_explicit_wait_timeout_none_means_unlimited():
    """逃生舱:显式 None = 无限等(文档须警示 slot 泄漏时消息卡本地)。"""
    assert _resolve_wait_timeout(None, 999.0, 180.0) is None


def test_concurrency_wait_timeout_is_timeout_error_subclass():
    assert issubclass(ConcurrencyWaitTimeout, TimeoutError)


def test_task_consumer_accepts_wait_timeout_param():
    from app.infrastructure.messaging.task_consumer import (
        clear_pending_consumers,
        get_pending_consumers,
        task_consumer,
    )

    @task_consumer("t.wait.param", wait_timeout=7.5, inbox=False)
    async def handler(payload: dict) -> None:
        return None

    spec = get_pending_consumers()[-1]
    assert spec.wait_timeout_override == 7.5
    clear_pending_consumers()


def test_task_consumer_wait_timeout_defaults_to_unset():
    from app.infrastructure.messaging.task_consumer import (
        clear_pending_consumers,
        get_pending_consumers,
        task_consumer,
    )

    @task_consumer("t.wait.default", inbox=False)
    async def handler(payload: dict) -> None:
        return None

    spec = get_pending_consumers()[-1]
    assert spec.wait_timeout_override is _UNSET
    clear_pending_consumers()


def test_task_consumer_rejects_negative_wait_timeout():
    from app.infrastructure.messaging.task_consumer import task_consumer

    with pytest.raises(ValueError, match="wait_timeout"):

        @task_consumer("t.wait.neg", wait_timeout=-1.0, inbox=False)
        async def handler(payload: dict) -> None:
            return None
