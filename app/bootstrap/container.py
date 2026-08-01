"""Bootstrap 资源 builder / lifecycle 集合点。

聚合各 infrastructure 模块的 builder 与 lifecycle context manager,
新增外部资源(httpx client、外部集成等)的 lifecycle 也收拢到这里。
不持有状态,不依赖 DI 框架。各进程 bootstrap 文件(api/worker/scheduler)
按需从此处挑选资源组装自己的 lifespan。
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager

import httpx

from app.infrastructure.database.engine import build_engine, engine_lifecycle
from app.infrastructure.database.session import build_session_factory
from app.infrastructure.messaging.broker import build_broker
from app.infrastructure.redis.client import redis_lifecycle
from app.integrations.bundle import Integrations
from app.integrations.dummyjson.client import DummyJsonClient
from app.integrations.dummyjson.settings import get_dummyjson_settings

__all__ = [
    "build_broker",
    "build_engine",
    "build_session_factory",
    "dummyjson_client_ctx",
    "engine_lifecycle",
    "integrations_lifecycle",
    "redis_lifecycle",
]


@asynccontextmanager
async def dummyjson_client_ctx() -> AsyncGenerator[DummyJsonClient]:
    """DummyJsonClient 生命周期:进程级单例,连接池复用。"""
    cfg = get_dummyjson_settings()
    async with httpx.AsyncClient(
        base_url=cfg.base_url,
        timeout=cfg.timeout,
    ) as http:
        yield DummyJsonClient(http)


@asynccontextmanager
async def integrations_lifecycle() -> AsyncGenerator[Integrations]:
    """进程级 integration 客户端聚合生命周期。

    新增业务 client:多 enter 一层 <name>_client_ctx() + Integrations 加一个字段。
    """
    async with AsyncExitStack() as stack:
        dummyjson = await stack.enter_async_context(dummyjson_client_ctx())
        yield Integrations(dummyjson=dummyjson)
