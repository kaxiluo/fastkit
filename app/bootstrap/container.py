"""Bootstrap 资源 builder / lifecycle 集合点。

聚合各 infrastructure 模块的 builder 与 lifecycle context manager,
新增外部资源(httpx client、外部集成等)的 lifecycle 也收拢到这里。
不持有状态,不依赖 DI 框架。各进程 bootstrap 文件(api/worker/scheduler)
按需从此处挑选资源组装自己的 lifespan。
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
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
    "API_CLIENTS",
    "WORKER_CLIENTS",
    "SCHEDULER_CLIENTS",
    "build_broker",
    "build_engine",
    "build_session_factory",
    "dummyjson_client_ctx",  # demo:dummyjson
    "engine_lifecycle",
    "integrations_lifecycle",
    "redis_lifecycle",
]


# demo:dummyjson —— 删除示例时连带删本函数
@asynccontextmanager
async def dummyjson_client_ctx() -> AsyncGenerator[DummyJsonClient]:
    """DummyJsonClient 生命周期:进程级单例,连接池复用。"""
    cfg = get_dummyjson_settings()
    async with httpx.AsyncClient(
        base_url=cfg.base_url,
        timeout=cfg.timeout,
    ) as http:
        yield DummyJsonClient(http)


ClientCtx = Callable[[], AsyncGenerator]

API_CLIENTS: tuple[ClientCtx, ...] = (dummyjson_client_ctx,)
WORKER_CLIENTS: tuple[ClientCtx, ...] = (dummyjson_client_ctx,)
SCHEDULER_CLIENTS: tuple[ClientCtx, ...] = ()  # 显式空,固化零装配


@asynccontextmanager
async def integrations_lifecycle(
    *ctx_providers: Callable[[], AsyncGenerator],
) -> AsyncGenerator[Integrations]:
    """进程级 integration 客户端聚合生命周期(Composition Root 显式装配)。

    只 enter 列表里的 *_client_ctx()。三进程各自声明,互不强耦合:
    未列入的 client 不 enter → 其 settings 不被读 → 该进程零配置可启动。
    被列入的 client 的 settings 缺失仍会启动期 fail-fast(ValidationError),
    保留 fail-fast 原则。

    新增业务 client:写一个 <name>_client_ctx(),加进用到的进程对应的
    *_CLIENTS 清单(API_CLIENTS / WORKER_CLIENTS / SCHEDULER_CLIENTS)。
    lifespan 文件不改 —— body 永远是 integrations_lifecycle(*<NAME>_CLIENTS)。
    """
    async with AsyncExitStack() as stack:
        integrations = Integrations()
        for ctx_provider in ctx_providers:
            client = await stack.enter_async_context(ctx_provider())
            integrations.register(client)
        yield integrations
