from __future__ import annotations

import pytest
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.scheduler.registry import (
    _CronJobSpec,
    clear_registered_cron_jobs,
    cron_job,
    get_registered_cron_jobs,
)


@pytest.fixture(autouse=True)
def _clear():
    clear_registered_cron_jobs()
    yield
    clear_registered_cron_jobs()


def test_decorator_registers_spec():
    @cron_job(CronTrigger(hour=2), job_id="my_job")
    async def my_task(session_factory: async_sessionmaker) -> None:
        pass

    specs = get_registered_cron_jobs()
    assert len(specs) == 1
    assert specs[0].job_id == "my_job"
    assert specs[0].func is my_task


def test_spec_detects_session_factory_param():
    @cron_job(CronTrigger(hour=3), job_id="with_sf")
    async def with_sf(session_factory: async_sessionmaker) -> None:
        pass

    assert get_registered_cron_jobs()[0].accepts_session_factory is True


def test_spec_no_session_factory_param():
    @cron_job(CronTrigger(hour=4), job_id="no_sf")
    async def no_sf() -> None:
        pass

    assert get_registered_cron_jobs()[0].accepts_session_factory is False


def test_defaults_stored_on_spec():
    @cron_job(CronTrigger(hour=5), job_id="defaults_job")
    async def my_task() -> None:
        pass

    spec = get_registered_cron_jobs()[0]
    assert spec.misfire_grace_time == 3600
    assert spec.max_instances == 1


def test_explicit_options_stored_on_spec():
    @cron_job(
        CronTrigger(hour=6),
        job_id="custom_job",
        misfire_grace_time=7200,
        max_instances=2,
    )
    async def custom_task() -> None:
        pass

    spec = get_registered_cron_jobs()[0]
    assert spec.misfire_grace_time == 7200
    assert spec.max_instances == 2


def test_decorator_preserves_function_identity():
    @cron_job(CronTrigger(hour=7), job_id="id_check")
    async def my_task() -> None:
        pass

    assert my_task.__name__ == "my_task"
    assert callable(my_task)


def test_get_registered_returns_copy():
    @cron_job(CronTrigger(hour=8), job_id="copy_check")
    async def my_task() -> None:
        pass

    a = get_registered_cron_jobs()
    b = get_registered_cron_jobs()
    assert a is not b
    assert a == b


def test_clear_empties_registry():
    @cron_job(CronTrigger(hour=9), job_id="clear_check")
    async def my_task() -> None:
        pass

    assert len(get_registered_cron_jobs()) == 1
    clear_registered_cron_jobs()
    assert get_registered_cron_jobs() == []


def test_spec_detects_integrations_param():
    @cron_job(CronTrigger(hour=10), job_id="with_integ")
    async def with_integ(integrations) -> None:
        pass

    assert get_registered_cron_jobs()[0].accepts_integrations is True


def test_spec_no_integrations_param():
    @cron_job(CronTrigger(hour=11), job_id="no_integ")
    async def no_integ() -> None:
        pass

    assert get_registered_cron_jobs()[0].accepts_integrations is False
