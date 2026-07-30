"""Scheduler 进程 lifespan 组装:app_context() + APScheduler 启停。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.infrastructure.messaging.cron import inbox_retention as _inbox_retention  # noqa: F401
from app.infrastructure.messaging.cron import outbox_retention as _outbox_retention  # noqa: F401
from app.infrastructure.scheduler.registry import get_registered_cron_jobs

from .lifecycle import AppContext, app_context


@asynccontextmanager
async def scheduler_lifespan() -> AsyncGenerator[AppContext]:
    log = structlog.get_logger()
    async with app_context() as ctx:
        scheduler = AsyncIOScheduler()
        for spec in get_registered_cron_jobs():
            kwargs = {}
            if spec.accepts_session_factory:
                kwargs["session_factory"] = ctx.session_factory
            scheduler.add_job(
                spec.func,
                spec.trigger,
                kwargs=kwargs,
                id=spec.job_id,
                misfire_grace_time=spec.misfire_grace_time,
                max_instances=spec.max_instances,
            )
        scheduler.start()
        log.info("scheduler.started", app_name=ctx.settings.app_name)
        try:
            yield ctx
        finally:
            scheduler.shutdown(wait=False)
            log.info("scheduler.stopped")
