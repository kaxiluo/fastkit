"""Outbox 历史记录清理 job：删除超保留期的已发布行。"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.scheduler.registry import cron_job

log = logging.getLogger(__name__)

OUTBOX_RETENTION_DAYS = 30


@cron_job(CronTrigger(hour=2), job_id="outbox_retention", misfire_grace_time=3600)
async def run_outbox_retention(session_factory: async_sessionmaker) -> None:
    """删除 published_at 超 OUTBOX_RETENTION_DAYS 天的行（active 已发布行 + dead 行）。

    published_at IS NULL 的 pending 行不受影响。
    """
    async with session_factory() as session, session.begin():
        result = await session.execute(
            text("""
                DELETE FROM fastkit_outbox
                WHERE published_at IS NOT NULL
                  AND published_at < NOW() - make_interval(days => :days)
            """),
            {"days": OUTBOX_RETENTION_DAYS},
        )
    log.info("outbox.retention_done", extra={"deleted": result.rowcount})
