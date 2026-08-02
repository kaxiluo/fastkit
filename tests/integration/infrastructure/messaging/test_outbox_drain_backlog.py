"""outbox relay 积压消化:_drain_once 返回抓回行数 + _drain_until_empty 连续消化。"""

from __future__ import annotations

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_outbox(db_engine):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE fastkit_outbox"))
    yield


async def _insert_rows(session_factory, n: int) -> None:
    async with session_factory() as session, session.begin():
        for i in range(n):
            await session.execute(
                text(
                    """
                    INSERT INTO fastkit_outbox (aggregate, routing_key, payload, headers)
                    VALUES (:agg, :rk, (:payload)::jsonb, (:headers)::jsonb)
                    """
                ),
                {"agg": "test", "rk": f"test.backlog.{i}", "payload": "{}", "headers": "{}"},
            )


async def _pending_count(session_factory) -> int:
    async with session_factory() as session:
        result = await session.execute(
            text("SELECT COUNT(*) FROM fastkit_outbox WHERE status = 'pending'")
        )
        return result.scalar_one()


async def test_drain_once_returns_fetched_row_count(
    session_factory, broker, test_messaging_settings, monkeypatch
):
    """返回值语义 = 本批抓回行数(用于判定是否抓满),即使 publish 全失败也非 0。"""
    from app.infrastructure.messaging.outbox.relay import _drain_once

    async def failing_publish(*a, **kw):
        raise ConnectionError("broker down")

    monkeypatch.setattr(broker, "publish", failing_publish)

    await _insert_rows(session_factory, 3)

    n = await _drain_once(session_factory, broker, test_messaging_settings)

    # 3 行都被抓回处理(失败推退避),抓回行数=3;旧实现返回成功数 0
    assert n == 3


async def test_drain_until_empty_converges_when_broker_is_down(
    session_factory, broker, test_messaging_settings, monkeypatch
):
    """broker 全挂时,_drain_until_empty 不忙等:失败行退避到未来,下批 SELECT 为空自然退出。"""
    from app.infrastructure.messaging.outbox.relay import _drain_until_empty

    monkeypatch.setattr(test_messaging_settings, "outbox_batch_size", 2)

    async def failing_publish(*a, **kw):
        raise ConnectionError("broker down")

    monkeypatch.setattr(broker, "publish", failing_publish)

    await _insert_rows(session_factory, 3)

    # 如果有忙等 bug 此调用会挂住;正常应有限次 drain 后退出
    await _drain_until_empty(session_factory, broker, test_messaging_settings)

    # 所有行仍未发布,attempts 已递增,next_attempt_at 被推到未来
    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT COUNT(*) FROM fastkit_outbox "
                "WHERE status = 'pending' AND attempts = 1 AND next_attempt_at > NOW()"
            )
        )
        assert result.scalar_one() == 3


async def test_drain_until_empty_processes_backlog_beyond_batch_size(
    session_factory, broker, test_messaging_settings, monkeypatch
):
    """积压 > batch_size 时,一次调用连续消化直至清空,不受单批 LIMIT 卡住。"""
    from app.infrastructure.messaging.outbox.relay import _drain_until_empty

    monkeypatch.setattr(test_messaging_settings, "outbox_batch_size", 2)

    await _insert_rows(session_factory, 5)

    await _drain_until_empty(session_factory, broker, test_messaging_settings)

    assert await _pending_count(session_factory) == 0
