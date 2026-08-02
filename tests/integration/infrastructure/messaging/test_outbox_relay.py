import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from app.infrastructure.messaging.outbox.relay import _backoff, _drain_once, relay_loop
from app.infrastructure.messaging.settings import MessagingSettings

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_outbox(db_engine):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE fastkit_outbox"))
    yield


def test_backoff_is_exponential_and_capped():
    assert _backoff(0, cap=600) == 1
    assert _backoff(1, cap=600) == 2
    assert _backoff(5, cap=600) == 32
    assert _backoff(15, cap=600) == 600  # 2^15=32768 → cap


async def _seed(session, routing_key: str = "t.evt", payload: dict | None = None):
    await session.execute(
        text(
            """
            INSERT INTO fastkit_outbox (aggregate, routing_key, payload, headers)
            VALUES ('t', :rk, CAST(:p AS JSONB), '{}'::jsonb)
            """
        ),
        {"rk": routing_key, "p": '{"v":1}' if payload is None else str(payload)},
    )


async def test_drain_once_publishes_and_marks_row(
    session_factory, test_messaging_settings: MessagingSettings
):
    async with session_factory() as s, s.begin():
        await _seed(s)

    broker = AsyncMock()
    broker.publish = AsyncMock(return_value=None)

    fetched = await _drain_once(session_factory, broker, test_messaging_settings)
    assert fetched == 1  # 1 行被抓回且成功发布
    broker.publish.assert_awaited_once()

    async with session_factory() as s:
        row = (
            (await s.execute(text("SELECT published_at, attempts, status FROM fastkit_outbox")))
            .mappings()
            .one()
        )
    assert row["published_at"] is not None
    assert row["attempts"] == 0
    assert row["status"] == "published"


async def test_drain_once_increments_attempts_on_broker_failure(
    session_factory, test_messaging_settings: MessagingSettings
):
    async with session_factory() as s, s.begin():
        await _seed(s)

    broker = AsyncMock()
    broker.publish = AsyncMock(side_effect=RuntimeError("broker down"))

    fetched = await _drain_once(session_factory, broker, test_messaging_settings)
    assert fetched == 1  # 1 行被抓回处理(publish 失败,退避)

    async with session_factory() as s:
        row = (
            (await s.execute(text("SELECT published_at, attempts, last_error FROM fastkit_outbox")))
            .mappings()
            .one()
        )
    assert row["published_at"] is None
    assert row["attempts"] == 1
    assert "broker down" in (row["last_error"] or "")


async def test_relay_loop_reacts_to_listen_notify(
    session_factory, test_messaging_settings: MessagingSettings
):
    """开 relay,插一行,等 < 2s 内被处理。"""
    broker = AsyncMock()
    broker.publish = AsyncMock(return_value=None)

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        relay_loop(session_factory, broker, test_messaging_settings, shutdown)
    )

    try:
        # 给 LISTEN 建立时间
        await asyncio.sleep(0.1)
        async with session_factory() as s, s.begin():
            await _seed(s, routing_key="test.outbox.relay")

        # 等 broker.publish 被调用
        for _ in range(20):
            await asyncio.sleep(0.1)
            if broker.publish.await_count >= 1:
                break
        assert broker.publish.await_count == 1
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=3.0)
