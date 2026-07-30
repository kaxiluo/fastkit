"""集成:fail_until_attempt=99 → 达 max_attempts=3 → DLQ + status=failed。"""

from __future__ import annotations

import asyncio
import json

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from app.config.settings import get_app_settings
from app.infrastructure.database.settings import get_database_settings
from app.infrastructure.messaging import Messaging
from app.infrastructure.messaging.settings import get_messaging_settings
from app.infrastructure.redis.settings import get_redis_settings

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate(db_engine, broker, test_messaging_settings):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE example_widgets RESTART IDENTITY"))
        await conn.execute(text("TRUNCATE fastkit_outbox"))
        await conn.execute(text("TRUNCATE fastkit_inbox"))
    dlq = await broker._channel.declare_queue(
        test_messaging_settings.dlq_queue, durable=True, passive=False
    )
    await dlq.purge()
    yield


@pytest.fixture(autouse=True)
def _patch_settings(
    test_settings,
    test_database_settings,
    test_redis_settings,
    test_messaging_settings,
    monkeypatch,
):
    monkeypatch.setattr(get_app_settings, "cache_clear", lambda: None)
    monkeypatch.setattr(get_database_settings, "cache_clear", lambda: None)
    monkeypatch.setattr(get_redis_settings, "cache_clear", lambda: None)
    monkeypatch.setattr(get_messaging_settings, "cache_clear", lambda: None)
    monkeypatch.setattr("app.config.settings.get_app_settings", lambda: test_settings)
    monkeypatch.setattr(
        "app.bootstrap.lifecycle.get_database_settings", lambda: test_database_settings
    )
    monkeypatch.setattr(
        "app.bootstrap.lifecycle.get_redis_settings", lambda: test_redis_settings
    )
    monkeypatch.setattr(
        "app.bootstrap.lifecycle.get_messaging_settings", lambda: test_messaging_settings
    )
    yield


async def test_fail_until_attempt_99_goes_to_dlq_after_3_tries(
    session_factory,
    broker,
    fast_retry_settings,
):
    from app.entrypoints.http.app import app as api_app
    from app.modules.example import (  # noqa: F401
        consumers,
        events,
    )

    messaging = Messaging(
        broker=broker,
        session_factory=session_factory,
        settings=fast_retry_settings,
    )
    await messaging.start_consumers()

    try:
        async with LifespanManager(api_app):
            transport = ASGITransport(app=api_app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/v1/example/widgets",
                    json={"payload": {"fail_until_attempt": 99}},
                )
            assert resp.status_code == 201, resp.text
            widget_id = resp.json()["id"]

            row = None
            for _ in range(75):
                await asyncio.sleep(0.2)
                async with session_factory() as s:
                    row = (
                        (
                            await s.execute(
                                text(
                                    "SELECT status, attempts, last_error FROM example_widgets "
                                    "WHERE id = :id"
                                ),
                                {"id": widget_id},
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                if row and row["attempts"] >= 3:
                    break

            assert row is not None
            assert row["attempts"] == 3, f"expected attempts=3, got {row['attempts']}"
            assert row["status"] == "failed"
            assert row["last_error"] is not None
            assert "fail_until_attempt" in row["last_error"]

            dlq = await broker._channel.declare_queue(
                fast_retry_settings.dlq_queue, durable=True, passive=True
            )
            incoming = None
            for _ in range(20):
                incoming = await dlq.get(no_ack=True, fail=False)
                if incoming is not None:
                    break
                await asyncio.sleep(0.2)

            assert incoming is not None, "DLQ 未收到消息"
            headers = dict(incoming.headers or {})
            assert headers.get("routing_key") == "example.widget.requested"
            failure = headers.get("failure")
            if isinstance(failure, str):
                failure = json.loads(failure)
            assert failure is not None, "envelope.failure 缺失"
            assert failure["type"].endswith("ExampleWidgetFailedError")
            assert "fail_until_attempt" in failure["message"]
            assert "at" in failure
    finally:
        await messaging.stop()
