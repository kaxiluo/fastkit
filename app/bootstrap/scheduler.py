"""Scheduler 进程 lifespan 组装:app_context() + APScheduler 启停。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.infrastructure.messaging.cron import inbox_retention as _inbox_retention  # noqa: F401
from app.infrastructure.messaging.cron import outbox_retention as _outbox_retention  # noqa: F401
from app.infrastructure.scheduler.registry import get_registered_cron_jobs

from .container import (
    SCHEDULER_CLIENTS,
    SCHEDULER_DATABASES,
    databases_lifecycle,
    integrations_lifecycle,
)
from .lifecycle import AppContext, app_context


@asynccontextmanager
async def scheduler_lifespan() -> AsyncGenerator[AppContext]:
    log = structlog.get_logger()
    # Scheduler 默认零装配 integration client / 业务库 —— SCHEDULER_* 显式空,零配置可启动,
    # 不被无关 client/settings 缺失阻塞。业务侧需要时在 container.py 对应清单加 ctx,不要默认开启。
    async with (
        app_context() as ctx,
        integrations_lifecycle(*SCHEDULER_CLIENTS) as integrations,
        databases_lifecycle(*SCHEDULER_DATABASES) as databases,
    ):
        scheduler = AsyncIOScheduler()
        for spec in get_registered_cron_jobs():
            kwargs = {}
            if spec.accepts_session_factory:
                kwargs["session_factory"] = ctx.session_factory
            if spec.accepts_integrations:
                kwargs["integrations"] = integrations
            if spec.accepts_databases:
                kwargs["databases"] = databases
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
