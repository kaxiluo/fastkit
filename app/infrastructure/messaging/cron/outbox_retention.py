"""Outbox 历史记录清理 job：删除超保留期的已终态行(published/dead)。"""

from __future__ import annotations

import structlog
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.scheduler.registry import cron_job

log = structlog.get_logger(__name__)

OUTBOX_RETENTION_DAYS = 30


@cron_job(CronTrigger(hour=2), job_id="outbox_retention", misfire_grace_time=3600)
async def run_outbox_retention(session_factory: async_sessionmaker) -> None:
    """清理超 OUTBOX_RETENTION_DAYS 天的已终态行。

    status='published' 行按 published_at 清理;status='dead' 行按 dead_at 清理。
    pending 行不受影响。
    """
    async with session_factory() as session, session.begin():
        result = await session.execute(
            text("""
                DELETE FROM fastkit_outbox
                WHERE (status = 'published' AND published_at < NOW() - make_interval(days => :days))
                   OR (status = 'dead'      AND dead_at        < NOW() - make_interval(days => :days))
            """),
            {"days": OUTBOX_RETENTION_DAYS},
        )
    log.info("outbox.retention_done", deleted=result.rowcount)
