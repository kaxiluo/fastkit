"""Inbox 幂等记录清理 job：删除超保留期的 inbox 去重记录。"""

from __future__ import annotations

import structlog
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.scheduler.registry import cron_job

log = structlog.get_logger(__name__)

INBOX_RETENTION_DAYS = 30


@cron_job(CronTrigger(hour=2), job_id="inbox_retention", misfire_grace_time=3600)
async def run_inbox_retention(session_factory: async_sessionmaker) -> None:
    """删除 processed_at 超 INBOX_RETENTION_DAYS 天的 inbox 幂等记录。"""
    async with session_factory() as session, session.begin():
        result = await session.execute(
            text("""
                DELETE FROM fastkit_inbox
                WHERE processed_at < NOW() - make_interval(days => :days)
            """),
            {"days": INBOX_RETENTION_DAYS},
        )
    log.info("inbox.retention_done", deleted=result.rowcount)
