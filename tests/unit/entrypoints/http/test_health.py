"""HTTP /health 与 /ready 探针单元测试。

直接 await 路由函数,绕开 dishka 注入;通过 fake engine/redis/broker
覆盖 ok 与 degraded 两条主路径,以及 broker.ping 返回 False 的显式失败分支。
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.entrypoints.http.health import health as health_endpoint
from app.entrypoints.http.health import ready as ready_endpoint

_TIMEOUT = 0.1
_SETTINGS = SimpleNamespace(ready_broker_ping_timeout=_TIMEOUT)


class _OkEngine:
    """engine.connect() async cm + conn.execute 成功。"""

    def connect(self) -> _ConnectCM:
        return _ConnectCM()


class _ConnectCM:
    async def __aenter__(self) -> AsyncMock:
        return AsyncMock()

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FailingConnectCM:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def __aenter__(self) -> AsyncMock:
        raise self._exc

    async def __aexit__(self, *exc: object) -> None:
        return None


def _failing_engine(exc: Exception) -> MagicMock:
    engine = MagicMock()
    engine.connect.return_value = _FailingConnectCM(exc)
    return engine


def _ok_redis() -> AsyncMock:
    redis = AsyncMock()
    redis.ping.return_value = None
    return redis


def _ok_broker() -> AsyncMock:
    broker = AsyncMock()
    broker.ping.return_value = True
    return broker


def _false_broker() -> AsyncMock:
    broker = AsyncMock()
    broker.ping.return_value = False
    return broker


@pytest.mark.asyncio
async def test_health_returns_ok() -> None:
    assert await health_endpoint() == {"status": "ok"}


@pytest.mark.asyncio
async def test_ready_returns_ok_when_all_probes_pass() -> None:
    resp = await ready_endpoint(
        settings=_SETTINGS,
        engine=_OkEngine(),
        redis=_ok_redis(),
        broker=_ok_broker(),
    )

    assert resp.status_code == 200
    assert json.loads(resp.body) == {
        "status": "ok",
        "checks": {"db": "ok", "redis": "ok", "broker": "ok"},
    }


@pytest.mark.asyncio
async def test_ready_returns_degraded_when_db_probe_fails() -> None:
    resp = await ready_endpoint(
        settings=_SETTINGS,
        engine=_failing_engine(OSError("connection refused")),
        redis=_ok_redis(),
        broker=_ok_broker(),
    )

    assert resp.status_code == 503
    body = json.loads(resp.body)
    assert body["status"] == "degraded"
    assert body["checks"]["db"].startswith("error: OSError:")
    assert body["checks"]["redis"] == "ok"
    assert body["checks"]["broker"] == "ok"


@pytest.mark.asyncio
async def test_ready_returns_degraded_when_broker_ping_returns_false() -> None:
    resp = await ready_endpoint(
        settings=_SETTINGS,
        engine=_OkEngine(),
        redis=_ok_redis(),
        broker=_false_broker(),
    )

    assert resp.status_code == 503
    body = json.loads(resp.body)
    assert body["status"] == "degraded"
    assert body["checks"]["broker"].startswith("error: RuntimeError:")
    assert body["checks"]["db"] == "ok"
    assert body["checks"]["redis"] == "ok"
