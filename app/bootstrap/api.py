"""API 进程 lifespan 组装:app_context() + dummyjson_client_ctx() + dishka。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from dishka import Provider, Scope, make_async_container, provide
from fastapi import FastAPI
from faststream.rabbit import RabbitBroker
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.config.settings import AppSettings
from app.infrastructure.messaging import EventRegistry, Messaging
from app.integrations.dummyjson.client import DummyJsonClient
from app.modules.example import events as _example_events  # noqa: F401  触发 @event 注册

from .container import dummyjson_client_ctx
from .lifecycle import AppContext, app_context


class _ContextProvider(Provider):
    scope = Scope.APP

    def __init__(self, ctx: AppContext, dummyjson: DummyJsonClient) -> None:
        super().__init__()
        self._ctx = ctx
        self._dummyjson = dummyjson

    @provide
    def settings(self) -> AppSettings:
        return self._ctx.settings

    @provide
    def engine(self) -> AsyncEngine:
        return self._ctx.engine

    @provide
    def redis(self) -> Redis:
        return self._ctx.redis

    @provide
    def broker(self) -> RabbitBroker:
        return self._ctx.broker

    @provide
    def session_factory(self) -> async_sessionmaker:
        return self._ctx.session_factory

    @provide
    def messaging(self) -> Messaging:
        return self._ctx.messaging

    @provide
    def events(self, messaging: Messaging) -> EventRegistry:
        return messaging.registry

    @provide
    def dummyjson_client(self) -> DummyJsonClient:
        return self._dummyjson


def setup_api(app: FastAPI) -> None:
    """在应用构建期调用:注册 dishka ContainerMiddleware。

    ContainerMiddleware 在请求时读 ``app.state.dishka_container``,所以真实 container
    在 lifespan 启动时再注入(见 ``api_lifespan``)。
    add_middleware 必须先于任何请求/中间件栈构建,故只能在模块级调用。
    """
    from dishka.integrations.fastapi import ContainerMiddleware

    app.add_middleware(ContainerMiddleware)
    app.state.dishka_container = None


@asynccontextmanager
async def api_lifespan(app: FastAPI) -> AsyncGenerator[None]:
    log = structlog.get_logger()
    async with app_context() as ctx, dummyjson_client_ctx() as dummyjson:
        await ctx.messaging.start_publishing_only()
        log.info("api.started", app_name=ctx.settings.app_name)
        container = make_async_container(_ContextProvider(ctx, dummyjson))
        # 注入真实 container,ContainerMiddleware 在请求时按此键读取
        app.state.dishka_container = container
        try:
            yield
        finally:
            log.info("api.stopping")
            await ctx.messaging.stop()
            await container.close()
