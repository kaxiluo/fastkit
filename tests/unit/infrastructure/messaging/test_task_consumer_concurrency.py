from __future__ import annotations

import pytest

from app.infrastructure.messaging.task_consumer import (
    clear_pending_consumers,
    get_pending_consumers,
    task_consumer,
)


def test_spec_has_no_concurrency_lease_field():
    clear_pending_consumers()

    @task_consumer("t.conc.a", concurrency=3)
    async def h(payload: dict) -> None: ...

    spec = get_pending_consumers()[0]
    assert spec.concurrency == 3
    assert not hasattr(spec, "concurrency_lease")
    clear_pending_consumers()


def test_invalid_concurrency_rejected():
    clear_pending_consumers()
    with pytest.raises(ValueError):

        @task_consumer("t.conc.c", concurrency=0)
        async def h(payload: dict) -> None: ...

    clear_pending_consumers()


def test_concurrency_defaults_to_one():
    """不显式指定时默认 1(串行兜底);不再有 None=不限的逃生口。"""
    clear_pending_consumers()

    @task_consumer("t.conc.default")
    async def h(payload: dict) -> None: ...

    spec = get_pending_consumers()[0]
    assert spec.concurrency == 1
    clear_pending_consumers()
