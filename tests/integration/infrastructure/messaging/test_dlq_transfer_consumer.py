"""端到端:consumer 达 max_attempts 后进 DLQ,携带 failure.*。"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate(db_engine, broker, test_messaging_settings):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE fastkit_outbox"))
        await conn.execute(text("TRUNCATE fastkit_inbox"))
    # 清空 DLQ 残留消息(其他测试可能也往 DLX 投递)
    dlq = await broker._channel.declare_queue(
        test_messaging_settings.dlq_queue,
        durable=True,
        passive=False,
    )
    await dlq.purge()
    yield


async def test_handler_exhausts_retries_and_dead_letters(
    broker, session_factory, fast_retry_settings
):
    from app.infrastructure.messaging.engine import Messaging
    from app.infrastructure.messaging.retry_policy import RetryPolicy
    from app.infrastructure.messaging.task_consumer import (
        clear_pending_consumers,
        task_consumer,
    )

    clear_pending_consumers()

    call_count = {"n": 0}

    @task_consumer(
        "test.dlq_flow.consumer",
        retry=RetryPolicy(max_attempts=2, delay=0),
        inbox=False,
    )
    async def handler(payload: dict) -> None:
        call_count["n"] += 1
        raise RuntimeError("always fails")

    msg = Messaging(
        broker=broker,
        session_factory=session_factory,
        settings=fast_retry_settings,
    )
    try:
        await msg.start_consumers()

        await broker.publish(
            {"n": 1},
            routing_key="test.dlq_flow.consumer",
            headers={
                "message_id": "dlq-1",
                "attempts": 1,
                "routing_key": "test.dlq_flow.consumer",
            },
        )

        # 等 handler 被调 2 次(首发 + retry 500ms 后)
        for _ in range(40):
            if call_count["n"] >= 2:
                break
            await asyncio.sleep(0.2)

        # 再等 DLQ 转投完成
        await asyncio.sleep(0.5)

        assert call_count["n"] == 2, f"expected 2 attempts, got {call_count['n']}"

        # 用 aio-pika channel 直接从 DLQ 拉一条消息断言
        dlq = await broker._channel.declare_queue(
            fast_retry_settings.dlq_queue,
            durable=True,
            passive=True,
        )
        incoming = None
        for _ in range(20):
            incoming = await dlq.get(no_ack=True, fail=False)
            if incoming is not None:
                break
            await asyncio.sleep(0.2)

        assert incoming is not None, "DLQ should have received the dead-lettered message"
        headers = incoming.headers
        assert headers["attempts"] == 2
        assert headers["failure"]["type"].endswith("RuntimeError")
        assert "always fails" in headers["failure"]["message"]
        assert headers["failure"]["at"]  # 非空
    finally:
        await msg.stop()
        clear_pending_consumers()
