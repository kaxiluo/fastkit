"""端到端:consumer 抛异常 → retry.500ms → 回原队列 → 成功。"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate(db_engine):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE fastkit_outbox"))
        await conn.execute(text("TRUNCATE fastkit_inbox"))
    yield


async def test_handler_retries_and_succeeds(broker, session_factory, fast_retry_settings):
    from app.infrastructure.messaging.engine import Messaging
    from app.infrastructure.messaging.retry_policy import RetryPolicy
    from app.infrastructure.messaging.task_consumer import (
        clear_pending_consumers,
        task_consumer,
    )

    clear_pending_consumers()

    counter = {"calls": 0}

    @task_consumer(
        "test.retry_flow.success",
        retry=RetryPolicy(max_attempts=3),
        inbox=False,
    )
    async def handler(payload: dict) -> None:
        counter["calls"] += 1
        if counter["calls"] < 3:
            raise ValueError(f"transient failure #{counter['calls']}")
        # 第 3 次成功

    msg = Messaging(
        broker=broker,
        session_factory=session_factory,
        settings=fast_retry_settings,
    )
    try:
        await msg.start_consumers()

        # 直接 publish 到业务 queue(绕开 outbox,专注 consumer 重试路径)
        await broker.publish(
            {"n": 1},
            routing_key="test.retry_flow.success",
            headers={
                "message_id": "retry-1",
                "attempts": 1,
                "routing_key": "test.retry_flow.success",
            },
        )

        # 等 2 * 500ms(TTL) + 处理时间;给足余量
        for _ in range(40):
            if counter["calls"] >= 3:
                break
            await asyncio.sleep(0.2)

        assert counter["calls"] == 3, f"expected 3 attempts, got {counter['calls']}"
    finally:
        await msg.stop()
        clear_pending_consumers()
