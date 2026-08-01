"""bootstrap lifespan 单元测试。

直接 patch 掉 app_context(其在 test_app_context_broker.py 等已单独覆盖),
聚焦 worker_lifespan / scheduler_lifespan 自身的启停顺序与分支。
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _fake_app_context(ctx: SimpleNamespace):
    """返回一个无视入参、yield 固定 ctx 的 async context manager 工厂。"""

    @asynccontextmanager
    async def _cm(*_args: object, **_kwargs: object) -> AsyncGenerator[SimpleNamespace]:
        yield ctx

    return _cm


@pytest.mark.asyncio
async def test_worker_lifespan_starts_consumers_then_stops_messaging() -> None:
    from app.bootstrap.worker import worker_lifespan

    messaging = SimpleNamespace(
        start_consumers=AsyncMock(),
        stop=AsyncMock(),
    )
    ctx = SimpleNamespace(
        settings=SimpleNamespace(app_name="test-app"),
        messaging=messaging,
    )
    broker = MagicMock(name="broker")
    integrations = MagicMock(name="integrations")

    with (
        patch("app.bootstrap.worker.app_context", _fake_app_context(ctx)),
        patch("app.bootstrap.worker.integrations_lifecycle", _fake_app_context(integrations)),
    ):
        async with worker_lifespan(broker=broker) as yielded:
            assert yielded is ctx

    messaging.start_consumers.assert_awaited_once_with(integrations=integrations)
    messaging.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_scheduler_lifespan_registers_jobs_with_and_without_session_factory() -> None:
    from app.bootstrap.scheduler import scheduler_lifespan

    session_factory = MagicMock(name="session_factory")
    integrations = MagicMock(name="integrations")
    ctx = SimpleNamespace(
        settings=SimpleNamespace(app_name="test-app"),
        session_factory=session_factory,
    )

    spec_with_sf = SimpleNamespace(
        func=lambda **_kw: None,
        trigger=MagicMock(name="trigger1"),
        job_id="with-sf",
        misfire_grace_time=10,
        max_instances=1,
        accepts_session_factory=True,
        accepts_integrations=False,
    )
    spec_without_sf = SimpleNamespace(
        func=lambda: None,
        trigger=MagicMock(name="trigger2"),
        job_id="without-sf",
        misfire_grace_time=20,
        max_instances=2,
        accepts_session_factory=False,
        accepts_integrations=False,
    )
    spec_with_integ = SimpleNamespace(
        func=lambda **_kw: None,
        trigger=MagicMock(name="trigger3"),
        job_id="with-integ",
        misfire_grace_time=30,
        max_instances=1,
        accepts_session_factory=False,
        accepts_integrations=True,
    )

    with (
        patch("app.bootstrap.scheduler.app_context", _fake_app_context(ctx)),
        patch("app.bootstrap.scheduler.integrations_lifecycle", _fake_app_context(integrations)),
        patch(
            "app.bootstrap.scheduler.get_registered_cron_jobs",
            return_value=[spec_with_sf, spec_without_sf, spec_with_integ],
        ),
        patch("app.bootstrap.scheduler.AsyncIOScheduler") as mock_sched_cls,
    ):
        mock_sched = MagicMock(name="scheduler")
        mock_sched_cls.return_value = mock_sched

        async with scheduler_lifespan() as yielded:
            assert yielded is ctx

    add_job_calls = mock_sched.add_job.call_args_list
    assert len(add_job_calls) == 3

    first = add_job_calls[0]
    assert first.args[0] is spec_with_sf.func
    assert first.args[1] is spec_with_sf.trigger
    assert first.kwargs == {
        "kwargs": {"session_factory": session_factory},
        "id": "with-sf",
        "misfire_grace_time": 10,
        "max_instances": 1,
    }

    second = add_job_calls[1]
    assert second.args[0] is spec_without_sf.func
    assert second.args[1] is spec_without_sf.trigger
    assert second.kwargs == {
        "kwargs": {},
        "id": "without-sf",
        "misfire_grace_time": 20,
        "max_instances": 2,
    }

    third = add_job_calls[2]
    assert third.args[0] is spec_with_integ.func
    assert third.args[1] is spec_with_integ.trigger
    assert third.kwargs == {
        "kwargs": {"integrations": integrations},
        "id": "with-integ",
        "misfire_grace_time": 30,
        "max_instances": 1,
    }

    mock_sched.start.assert_called_once_with()
    mock_sched.shutdown.assert_called_once_with(wait=False)


@pytest.mark.asyncio
async def test_scheduler_lifespan_with_no_cron_jobs_skips_loop_body() -> None:
    """空 cron job 列表覆盖 for 循环不进入的分支。"""

    from app.bootstrap.scheduler import scheduler_lifespan

    ctx = SimpleNamespace(
        settings=SimpleNamespace(app_name="test-app"),
        session_factory=MagicMock(),
    )

    with (
        patch("app.bootstrap.scheduler.app_context", _fake_app_context(ctx)),
        patch("app.bootstrap.scheduler.integrations_lifecycle", _fake_app_context(MagicMock())),
        patch("app.bootstrap.scheduler.get_registered_cron_jobs", return_value=[]),
        patch("app.bootstrap.scheduler.AsyncIOScheduler") as mock_sched_cls,
    ):
        mock_sched = MagicMock(name="scheduler")
        mock_sched_cls.return_value = mock_sched

        async with scheduler_lifespan():
            pass

    mock_sched.add_job.assert_not_called()
    mock_sched.start.assert_called_once_with()
    mock_sched.shutdown.assert_called_once_with(wait=False)
