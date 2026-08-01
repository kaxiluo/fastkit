"""bootstrap api.py / container.py 单元测试。

patch 掉 app_context、dummyjson_client_ctx、make_async_container,
聚焦 _ContextProvider 的 provide、setup_api 的中间件注册、
api_lifespan 的启停顺序、dummyjson_client_ctx 的资源构造。
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI

from app.bootstrap.api import _ContextProvider, api_lifespan, setup_api
from app.bootstrap.container import dummyjson_client_ctx
from app.integrations.dummyjson.client import DummyJsonClient


def test_context_provider_exposes_all_app_context_dependencies() -> None:
    ctx = SimpleNamespace(
        settings=MagicMock(name="settings"),
        engine=MagicMock(name="engine"),
        session_factory=MagicMock(name="session_factory"),
        redis=MagicMock(name="redis"),
        broker=MagicMock(name="broker"),
        messaging=SimpleNamespace(registry=MagicMock(name="registry")),
    )
    dummyjson = MagicMock(name="dummyjson")

    provider = _ContextProvider(ctx, dummyjson)

    assert provider.settings() is ctx.settings
    assert provider.engine() is ctx.engine
    assert provider.redis() is ctx.redis
    assert provider.broker() is ctx.broker
    assert provider.session_factory() is ctx.session_factory
    assert provider.messaging() is ctx.messaging
    assert provider.dummyjson_client() is dummyjson
    assert provider.events(ctx.messaging) is ctx.messaging.registry


def test_setup_api_registers_container_middleware_and_resets_state() -> None:
    app = FastAPI()

    setup_api(app)

    middleware_reprs = [str(m.cls) for m in app.user_middleware]
    assert any("ContainerMiddleware" in repr_str for repr_str in middleware_reprs)
    assert app.state.dishka_container is None


@pytest.mark.asyncio
async def test_api_lifespan_injects_container_and_cleans_up_on_exit() -> None:
    messaging = SimpleNamespace(
        start_publishing_only=AsyncMock(),
        stop=AsyncMock(),
    )
    ctx = SimpleNamespace(
        settings=SimpleNamespace(app_name="test-app"),
        messaging=messaging,
    )
    dummyjson = MagicMock(name="dummyjson")
    fake_container = MagicMock(name="container")
    fake_container.close = AsyncMock()

    @asynccontextmanager
    async def fake_app_context() -> AsyncGenerator[SimpleNamespace]:
        yield ctx

    @asynccontextmanager
    async def fake_dummyjson_ctx() -> AsyncGenerator[MagicMock]:
        yield dummyjson

    app = FastAPI()

    with (
        patch("app.bootstrap.api.app_context", fake_app_context),
        patch("app.bootstrap.api.dummyjson_client_ctx", fake_dummyjson_ctx),
        patch("app.bootstrap.api.make_async_container", return_value=fake_container),
    ):
        async with api_lifespan(app):
            assert app.state.dishka_container is fake_container

    messaging.start_publishing_only.assert_awaited_once_with()
    messaging.stop.assert_awaited_once_with()
    fake_container.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_dummyjson_client_ctx_yields_dummyjson_client() -> None:
    async with dummyjson_client_ctx() as client:
        assert isinstance(client, DummyJsonClient)


@pytest.mark.asyncio
async def test_integrations_lifecycle_yields_bundle_with_dummyjson() -> None:
    from app.bootstrap.container import integrations_lifecycle
    from app.integrations.bundle import Integrations

    async with integrations_lifecycle() as integrations:
        assert isinstance(integrations, Integrations)
        assert isinstance(integrations.dummyjson, DummyJsonClient)
