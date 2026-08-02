"""outbox relay 持续失败达上限 → 标 status='dead'，不转投 DLX。"""

from __future__ import annotations

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_outbox(db_engine):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE fastkit_outbox"))
    yield


async def test_outbox_row_transitions_to_dead_after_max_attempts(
    session_factory, broker, test_messaging_settings, monkeypatch
):
    from app.infrastructure.messaging.outbox.relay import _drain_once

    monkeypatch.setattr(test_messaging_settings, "outbox_max_attempts", 2)
    # backoff=0：第一次失败后 next_attempt_at=NOW()，第二次 drain 立即选中
    # 避免依赖 wall clock 的 sleep/polling（在 WSL2 + PG 事务时间下偶发 flake）
    monkeypatch.setattr(test_messaging_settings, "outbox_backoff_max_seconds", 0)

    # broker.publish 恒抛，不区分 exchange（不再有 DLX 分支）
    async def failing_publish(*args, **kwargs):
        raise ConnectionError("broker down")

    monkeypatch.setattr(broker, "publish", failing_publish)

    async with session_factory() as session, session.begin():
        await session.execute(
            text(
                """
                INSERT INTO fastkit_outbox (aggregate, routing_key, payload, headers)
                VALUES (:agg, :rk, (:payload)::jsonb, (:headers)::jsonb)
                """
            ),
            {"agg": "test", "rk": "test.dead", "payload": '{"a":1}', "headers": "{}"},
        )

    # 第一次 drain: attempts 0→1，backoff=min(2^1, 0)=0，next_attempt_at=NOW()
    await _drain_once(session_factory, broker, test_messaging_settings)

    # 第二次 drain: attempts 1→2，达上限，标 dead
    await _drain_once(session_factory, broker, test_messaging_settings)

    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT status, dead_reason, published_at, dead_at, attempts FROM fastkit_outbox "
                "WHERE routing_key='test.dead' ORDER BY id DESC LIMIT 1"
            )
        )
        row = result.first()

    assert row.status == "dead"
    assert row.published_at is None
    assert row.dead_at is not None
    assert row.attempts >= 2
    assert row.dead_reason and "ConnectionError" in row.dead_reason
