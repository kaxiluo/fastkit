"""端到端:concurrency=2 时在途峰值不超过 2(由 prefetch_count 保证)。"""

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


async def test_concurrency_caps_inflight_at_two(
    broker, session_factory, test_messaging_settings, redis_client
):
    from app.infrastructure.messaging.engine import Messaging
    from app.infrastructure.messaging.task_consumer import clear_pending_consumers, task_consumer

    clear_pending_consumers()

    state = {"inflight": 0, "peak": 0, "done": 0}

    @task_consumer("test.conc.slot", concurrency=2, inbox=False)
    async def handler(payload: dict) -> None:
        state["inflight"] += 1
        state["peak"] = max(state["peak"], state["inflight"])
        await asyncio.sleep(0.3)
        state["inflight"] -= 1
        state["done"] += 1

    msg = Messaging(
        broker=broker,
        session_factory=session_factory,
        settings=test_messaging_settings,
    )
    try:
        await msg.start_consumers(redis=redis_client)
        for n in range(5):
            await broker.publish(
                {"n": n},
                routing_key="test.conc.slot",
                headers={"message_id": f"c-{n}", "routing_key": "test.conc.slot"},
            )
        for _ in range(60):
            if state["done"] >= 5:
                break
            await asyncio.sleep(0.2)
        assert state["done"] == 5, state
        assert state["peak"] == 2, f"expected peak 2, got {state['peak']}"
    finally:
        await msg.stop()
        clear_pending_consumers()
