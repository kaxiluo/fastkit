"""验证 outbox 表结构升级后含 status + dead_reason 列。"""

from __future__ import annotations

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.integration


async def test_outbox_has_status_and_dead_reason_columns(session_factory):
    async with session_factory() as session:
        result = await session.execute(
            text("""
                SELECT column_name, data_type, column_default
                FROM information_schema.columns
                WHERE table_name = 'fastkit_outbox' AND column_name IN ('status', 'dead_reason')
                ORDER BY column_name
            """)
        )
        rows = result.all()
    columns = {r.column_name: r for r in rows}
    assert "status" in columns
    assert "dead_reason" in columns
    assert "pending" in columns["status"].column_default


async def test_idx_outbox_pending_filters_status(session_factory):
    async with session_factory() as session:
        result = await session.execute(
            text("""
                SELECT indexdef FROM pg_indexes
                WHERE indexname = 'idx_fastkit_outbox_pending'
            """)
        )
        row = result.first()
    assert row is not None
    assert "status" in row.indexdef.lower()


async def test_outbox_has_dead_at_column(session_factory):
    async with session_factory() as session:
        result = await session.execute(
            text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'fastkit_outbox' AND column_name = 'dead_at'
            """)
        )
        row = result.first()
    assert row is not None
    assert row.column_name == "dead_at"
    assert "timestamp" in row.data_type.lower()


async def test_idx_outbox_published_predicate_uses_status(session_factory):
    """idx_fastkit_outbox_published 谓词应为 status='published'，不再是 published_at IS NOT NULL。"""
    async with session_factory() as session:
        result = await session.execute(
            text("""
                SELECT indexdef FROM pg_indexes
                WHERE indexname = 'idx_fastkit_outbox_published'
            """)
        )
        row = result.first()
    assert row is not None
    assert "status" in row.indexdef.lower()
    assert "published_at is not null" not in row.indexdef.lower()


async def test_idx_outbox_dead_exists(session_factory):
    """idx_fastkit_outbox_dead 应存在，覆盖 dead_at 列，谓词为 status='dead'。"""
    async with session_factory() as session:
        result = await session.execute(
            text("""
                SELECT indexdef FROM pg_indexes
                WHERE indexname = 'idx_fastkit_outbox_dead'
            """)
        )
        row = result.first()
    assert row is not None
    assert "dead_at" in row.indexdef.lower()
    assert "status" in row.indexdef.lower()
