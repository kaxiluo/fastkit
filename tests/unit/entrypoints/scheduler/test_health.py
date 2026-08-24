"""Scheduler 进程 /health 探针单元测试。

ASGITransport 直连 app(不触发 lifespan,不连外部资源),
覆盖部署健康检查依赖的 GET /health 路由与响应体。
"""

from __future__ import annotations

import httpx
import pytest

from app.entrypoints.scheduler import app


@pytest.mark.asyncio
async def test_health_returns_ok() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
