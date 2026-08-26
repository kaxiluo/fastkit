"""集成:HTTP 创建 widget → outbox → consumer → DB status=finished。"""

from __future__ import annotations

import asyncio

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
async def _truncate(db_engine):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE example_widgets RESTART IDENTITY"))
        await conn.execute(text("TRUNCATE fastkit_outbox"))
        await conn.execute(text("TRUNCATE fastkit_inbox"))
    yield


@pytest.fixture(autouse=True)
def _patch_settings(
    test_settings,
    test_database_settings,
    test_redis_settings,
    test_messaging_settings,
    monkeypatch,
):
    """让 app.entrypoints.http.app 容器与测试 Messaging 共用同一 DB / broker。

    http/app.py 在模块加载时调 get_app_settings(),lifecycle.py 调各组件 getter;
    把每个 getter 都 patch 成返回测试 fixture,绕过 lru_cache 的真实 env 读取。
    """
    monkeypatch.setattr(get_app_settings, "cache_clear", lambda: None)
    monkeypatch.setattr(get_database_settings, "cache_clear", lambda: None)
    monkeypatch.setattr(get_redis_settings, "cache_clear", lambda: None)
    monkeypatch.setattr(get_messaging_settings, "cache_clear", lambda: None)
    monkeypatch.setattr("app.config.settings.get_app_settings", lambda: test_settings)
    monkeypatch.setattr(
        "app.bootstrap.lifecycle.get_database_settings", lambda: test_database_settings
    )
    monkeypatch.setattr("app.bootstrap.lifecycle.get_redis_settings", lambda: test_redis_settings)
    monkeypatch.setattr(
        "app.bootstrap.lifecycle.get_messaging_settings", lambda: test_messaging_settings
    )
    yield


async def test_http_create_widget_flows_through_consumer_and_finishes(
    session_factory,
    broker,
    test_messaging_settings,
    redis_client,
):
    from app.entrypoints.http.app import app as api_app
    from app.infrastructure.messaging.task_consumer import get_pending_consumers
    from app.modules.example import (  # noqa: F401
        consumers,
        events,
    )

    assert any(s.routing_key == "example.widget.requested" for s in get_pending_consumers()), (
        "example.widget.requested consumer 未注册"
    )

    messaging = Messaging(
        broker=broker,
        session_factory=session_factory,
        settings=test_messaging_settings,
    )
    await messaging.start_consumers(redis=redis_client)

    try:
        async with LifespanManager(api_app):
            transport = ASGITransport(app=api_app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/v1/example/widgets",
                    json={"payload": {}},
                )
            assert resp.status_code == 201, resp.text
            widget_id = resp.json()["id"]

            row = None
            for _ in range(50):
                await asyncio.sleep(0.1)
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
                if row and row["status"] == "finished":
                    break
            assert row is not None, f"example_widgets row {widget_id} not found"
            assert row["status"] == "finished", f"expected status=finished, got {row['status']}"
            assert row["attempts"] == 1
            assert row["last_error"] is None

            async with session_factory() as s:
                outbox_row = (
                    (
                        await s.execute(
                            text(
                                "SELECT published_at FROM fastkit_outbox "
                                "WHERE routing_key='example.widget.requested' "
                                "ORDER BY id DESC LIMIT 1"
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
            assert outbox_row["published_at"] is not None

            async with session_factory() as s:
                inbox_cnt = (
                    await s.execute(
                        text(
                            "SELECT COUNT(*) FROM fastkit_inbox "
                            "WHERE consumer='on_example_widget_requested'"
                        )
                    )
                ).scalar_one()
            assert inbox_cnt == 1
    finally:
        await messaging.stop()
