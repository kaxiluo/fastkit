from __future__ import annotations

import pytest

from app.infrastructure.messaging.task_consumer import (
    _UNSET,
    TaskTimeout,
    _resolve_timeout,
    clear_pending_consumers,
    get_pending_consumers,
    task_consumer,
)


@pytest.fixture(autouse=True)
def _clear():
    clear_pending_consumers()
    yield
    clear_pending_consumers()


def test_task_timeout_is_timeout_error_subclass():
    assert issubclass(TaskTimeout, TimeoutError)


def test_timeout_default_is_unset_sentinel():
    @task_consumer("t.to.default")
    async def h(payload: dict) -> None: ...

    assert get_pending_consumers()[-1].timeout_override is _UNSET


def test_timeout_explicit_value_stored():
    @task_consumer("t.to.explicit", timeout=12.5)
    async def h(payload: dict) -> None: ...

    assert get_pending_consumers()[-1].timeout_override == 12.5


def test_timeout_none_stored_as_none():
    @task_consumer("t.to.none", timeout=None)
    async def h(payload: dict) -> None: ...

    assert get_pending_consumers()[-1].timeout_override is None


def test_resolve_timeout_unset_uses_default():
    assert _resolve_timeout(_UNSET, 180.0) == 180.0


def test_resolve_timeout_explicit_overrides_default():
    assert _resolve_timeout(30.0, 180.0) == 30.0


def test_resolve_timeout_none_disables():
    assert _resolve_timeout(None, 180.0) is None
