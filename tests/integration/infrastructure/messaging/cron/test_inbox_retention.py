"""inbox retention 集成测试：验证超期幂等记录被删除，未超期记录保留。"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.messaging.cron.inbox_retention import run_inbox_retention

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate(db_engine):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE fastkit_inbox"))
    yield


async def _insert(
    session_factory: async_sessionmaker, *, consumer: str, message_id: str, processed_at_expr: str
) -> None:
    """插入一条 inbox 幂等记录。processed_at_expr 是 SQL 表达式。"""
    async with session_factory() as s, s.begin():
        await s.execute(
            text(
                f"""
                INSERT INTO fastkit_inbox (consumer, message_id, processed_at)
                VALUES (:consumer, :message_id, {processed_at_expr})
                """
            ),
            {"consumer": consumer, "message_id": message_id},
        )


async def _count(session_factory: async_sessionmaker) -> int:
    async with session_factory() as s:
        return (await s.execute(text("SELECT COUNT(*) FROM fastkit_inbox"))).scalar_one()


@pytest.mark.integration
async def test_deletes_old_records(session_factory: async_sessionmaker):
    """超 30 天的 inbox 记录应被删除。"""
    await _insert(
        session_factory,
        consumer="svc.a",
        message_id="msg-1",
        processed_at_expr="NOW() - INTERVAL '31 days'",
    )
    await run_inbox_retention(session_factory)
    assert await _count(session_factory) == 0


@pytest.mark.integration
async def test_keeps_recent_records(session_factory: async_sessionmaker):
    """未超 30 天的 inbox 记录不应被删除。"""
    await _insert(
        session_factory,
        consumer="svc.a",
        message_id="msg-2",
        processed_at_expr="NOW() - INTERVAL '29 days'",
    )
    await run_inbox_retention(session_factory)
    assert await _count(session_factory) == 1


@pytest.mark.integration
async def test_mixed_records(session_factory: async_sessionmaker):
    """超期 2 条、未超期 1 条：删 2 留 1。"""
    await _insert(
        session_factory,
        consumer="svc.a",
        message_id="msg-3",
        processed_at_expr="NOW() - INTERVAL '31 days'",
    )
    await _insert(
        session_factory,
        consumer="svc.a",
        message_id="msg-4",
        processed_at_expr="NOW() - INTERVAL '45 days'",
    )
    await _insert(
        session_factory,
        consumer="svc.b",
        message_id="msg-5",
        processed_at_expr="NOW() - INTERVAL '10 days'",
    )

    await run_inbox_retention(session_factory)

    assert await _count(session_factory) == 1
