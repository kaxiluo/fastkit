"""outbox relay 持续失败达上限 → 转投 DLX + 标 status='dead'。"""

from __future__ import annotations

import asyncio

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

    # 缩小 max_attempts 加速测试
    monkeypatch.setattr(test_messaging_settings, "outbox_max_attempts", 2)
    # 缩小 backoff 上限,让第一次失败后 delay 短到 1 秒内可等
    monkeypatch.setattr(test_messaging_settings, "outbox_backoff_max_seconds", 1)

    # 让 broker.publish 恒抛异常(仅对业务 routing_key;DLX 走真实以验证 DLQ 收到)
    original_publish = broker.publish
    dlx_calls: list[dict] = []

    async def flaky_publish(payload, *, routing_key=None, exchange=None, headers=None, **kw):
        if exchange == test_messaging_settings.dlq_exchange:
            dlx_calls.append({"payload": payload, "headers": headers})
            return await original_publish(
                payload,
                routing_key="",
                exchange=exchange,
                headers=headers,
                **kw,
            )
        raise ConnectionError("broker down")

    monkeypatch.setattr(broker, "publish", flaky_publish)

    # 插一条 attempts=0 的 outbox 行
    async with session_factory() as session, session.begin():
        await session.execute(
            text(
                """
                INSERT INTO fastkit_outbox (aggregate, routing_key, payload, headers)
                VALUES (:agg, :rk, (:payload)::jsonb, (:headers)::jsonb)
                """
            ),
            {
                "agg": "test",
                "rk": "test.dead",
                "payload": '{"a":1}',
                "headers": "{}",
            },
        )

    # 第一次 drain:attempts 0→1,走 backoff(1 秒后重试)
    await _drain_once(session_factory, broker, test_messaging_settings)

    # 等 backoff 过期(1 秒)+ 余量
    await asyncio.sleep(1.2)

    # 第二次 drain:attempts 1→2,达上限,走 dead + DLX 转投
    await _drain_once(session_factory, broker, test_messaging_settings)

    async with session_factory() as session:
        result = await session.execute(
            text(
                "SELECT status, dead_reason, published_at, attempts FROM fastkit_outbox "
                "WHERE routing_key='test.dead' ORDER BY id DESC LIMIT 1"
            )
        )
        row = result.first()

    assert row.status == "dead"
    assert row.published_at is not None
    assert row.attempts >= 2
    assert row.dead_reason and "ConnectionError" in row.dead_reason
    assert len(dlx_calls) == 1
    failure = dlx_calls[0]["headers"].get("failure", {})
    assert failure.get("type", "").endswith("ConnectionError")
