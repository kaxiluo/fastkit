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
from redis.asyncio import ConnectionPool

from app.config.settings import get_app_settings
from app.infrastructure.database.business.handle import BusinessDb, Databases
from app.infrastructure.database.engine import build_engine, engine_lifecycle
from app.infrastructure.database.session import build_session_factory
from app.infrastructure.messaging.broker import build_broker
from app.infrastructure.ratelimit import RateLimiter, build_rate_limiter
from app.infrastructure.ratelimit.settings import get_ratelimit_settings
from app.infrastructure.redis.client import redis_lifecycle
from app.infrastructure.redis.settings import get_redis_settings
from app.integrations.bundle import Integrations
from app.integrations.dummyjson.client import DummyJsonClient
from app.integrations.dummyjson.settings import get_dummyjson_settings

__all__ = [
    "API_CLIENTS",
    "API_DATABASES",
    "WORKER_CLIENTS",
    "WORKER_DATABASES",
    "SCHEDULER_CLIENTS",
    "SCHEDULER_DATABASES",
    "build_broker",
    "build_engine",
    "build_session_factory",
    "databases_lifecycle",
    "dummyjson_client_ctx",  # demo:dummyjson
    "engine_lifecycle",
    "integrations_lifecycle",
    "rate_limiter_ctx",
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


@asynccontextmanager
async def rate_limiter_ctx() -> AsyncGenerator[RateLimiter]:
    """全局限流器生命周期:自建 redis 连接池传给 limits storage,池归本 ctx 所有。

    infrastructure 组件借道 integrations 通道的原因:装配粒度是"按进程组合"
    (哪些进程列哪些 *_CLIENTS),与 AppContext"所有进程共享"的定位不符;
    RateLimiter 与外部 client 同为进程级单例、按类型取用,机制同构。
    骨架默认不装配;需要限流的进程把它列入自己的 *_CLIENTS。
    """
    pool = ConnectionPool.from_url(get_redis_settings().url.get_secret_value())
    try:
        yield build_rate_limiter(
            get_redis_settings(),
            get_app_settings(),
            get_ratelimit_settings(),
            connection_pool=pool,
        )
    finally:
        await pool.aclose()


ClientCtx = Callable[[], AsyncGenerator]
DbCtx = Callable[[], AsyncGenerator[BusinessDb]]

API_CLIENTS: tuple[ClientCtx, ...] = (dummyjson_client_ctx,)
WORKER_CLIENTS: tuple[ClientCtx, ...] = (dummyjson_client_ctx,)
SCHEDULER_CLIENTS: tuple[ClientCtx, ...] = ()  # 显式空,固化零装配

API_DATABASES: tuple[DbCtx, ...] = ()  # 业务库按需装配,见 docs/development-guide.md
WORKER_DATABASES: tuple[DbCtx, ...] = ()
SCHEDULER_DATABASES: tuple[DbCtx, ...] = ()  # 显式空,固化零装配


@asynccontextmanager
async def integrations_lifecycle(
    *ctx_providers: Callable[[], AsyncGenerator],
) -> AsyncGenerator[Integrations]:
    """进程级 integration 客户端聚合生命周期(Composition Root 显式装配)。

    只 enter 传入的 *_client_ctx(),按 type 注册进 Integrations registry。
    registry 除外部集成 client 外,还收借道装配的 infrastructure 组件
    (如全局限流器 rate_limiter_ctx)。未列入的组件不 enter → 其 settings
    不被读 → 该进程零配置可启动;被列入的组件 settings 缺失仍会启动期
    fail-fast(ValidationError)。
    """
    async with AsyncExitStack() as stack:
        integrations = Integrations()
        for ctx_provider in ctx_providers:
            client = await stack.enter_async_context(ctx_provider())
            integrations.register(client)
        yield integrations


@asynccontextmanager
async def databases_lifecycle(
    *ctxs: DbCtx,
) -> AsyncGenerator[Databases]:
    """进程级业务库句柄聚合生命周期(Composition Root 显式装配)。

    只 enter 传入的 ctx;退出时逆序 dispose 各业务库 engine。
    未列入的库其 settings 不被读 → 该进程零配置可启。
    """
    async with AsyncExitStack() as stack:
        databases = Databases()
        for c in ctxs:
            handle = await stack.enter_async_context(c())
            databases.register(handle)
        yield databases
