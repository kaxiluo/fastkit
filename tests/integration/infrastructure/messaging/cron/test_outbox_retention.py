"""outbox retention 集成测试：验证超期行被删除，pending/未超期行保留。"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.messaging.cron.outbox_retention import run_outbox_retention

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate(db_engine):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE fastkit_outbox"))
    yield


async def _insert(
    session_factory: async_sessionmaker, *, published_at_expr: str, status: str = "active"
) -> int:
    """插入一行 outbox，返回 id。published_at_expr 是 SQL 表达式，如 'NOW() - INTERVAL ''31 days'''。"""
    async with session_factory() as s, s.begin():
        row = (
            await s.execute(
                text(
                    f"""
                    INSERT INTO fastkit_outbox (aggregate, routing_key, payload, headers, published_at, status)
                    VALUES ('t', 'test.evt', '{{}}'::jsonb, '{{}}'::jsonb,
                            {published_at_expr}, :status)
                    RETURNING id
                    """
                ),
                {"status": status},
            )
        ).one()
    return row.id


async def _insert_pending(session_factory: async_sessionmaker) -> int:
    """插入 published_at IS NULL 的 pending 行。"""
    async with session_factory() as s, s.begin():
        row = (
            await s.execute(
                text(
                    """
                    INSERT INTO fastkit_outbox (aggregate, routing_key, payload, headers)
                    VALUES ('t', 'test.evt', '{}'::jsonb, '{}'::jsonb)
                    RETURNING id
                    """
                )
            )
        ).one()
    return row.id


async def _count(session_factory: async_sessionmaker) -> int:
    async with session_factory() as s:
        return (await s.execute(text("SELECT COUNT(*) FROM fastkit_outbox"))).scalar_one()


@pytest.mark.integration
async def test_deletes_old_published_active_rows(session_factory: async_sessionmaker):
    """超 30 天的 active published 行应被删除。"""
    await _insert(session_factory, published_at_expr="NOW() - INTERVAL '31 days'", status="active")
    await run_outbox_retention(session_factory)
    assert await _count(session_factory) == 0


@pytest.mark.integration
async def test_deletes_old_dead_rows(session_factory: async_sessionmaker):
    """超 30 天的 dead 行（published_at 有值）应被删除。"""
    await _insert(session_factory, published_at_expr="NOW() - INTERVAL '31 days'", status="dead")
    await run_outbox_retention(session_factory)
    assert await _count(session_factory) == 0


@pytest.mark.integration
async def test_keeps_recent_rows(session_factory: async_sessionmaker):
    """未超 30 天的 published 行不应被删除。"""
    await _insert(session_factory, published_at_expr="NOW() - INTERVAL '29 days'", status="active")
    await run_outbox_retention(session_factory)
    assert await _count(session_factory) == 1


@pytest.mark.integration
async def test_keeps_pending_rows(session_factory: async_sessionmaker):
    """published_at IS NULL 的 pending 行不应被删除。"""
    await _insert_pending(session_factory)
    await run_outbox_retention(session_factory)
    assert await _count(session_factory) == 1


@pytest.mark.integration
async def test_mixed_rows(session_factory: async_sessionmaker):
    """混合场景：4 行，删 2 行（超期 active + 超期 dead），留 2 行（未超期 + pending）。"""
    await _insert(session_factory, published_at_expr="NOW() - INTERVAL '31 days'", status="active")
    await _insert(session_factory, published_at_expr="NOW() - INTERVAL '31 days'", status="dead")
    await _insert(session_factory, published_at_expr="NOW() - INTERVAL '29 days'", status="active")
    await _insert_pending(session_factory)

    await run_outbox_retention(session_factory)

    assert await _count(session_factory) == 2
