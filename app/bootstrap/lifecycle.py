"""框架无关的进程级资源编排。

app_context() 按序创建所有进程共享的基础资源(DB -> Redis -> broker + Messaging),
yield AppContext,退出时逆序关闭。API 进程特有资源(如 DummyJsonClient)不在这里,
由各进程 bootstrap 文件单独管。

broker 参数：外部传入时直接复用（worker 进程把同一 broker 传给 AsgiFastStream），
            不传时自建（api、scheduler 进程各自管理连接）。
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import structlog
from faststream.rabbit import RabbitBroker
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.config.settings import AppSettings, get_app_settings
from app.infrastructure.database.engine import engine_lifecycle
from app.infrastructure.database.session import build_session_factory
from app.infrastructure.database.settings import get_database_settings
from app.infrastructure.messaging import Messaging
from app.infrastructure.messaging.broker import build_broker
from app.infrastructure.messaging.settings import get_messaging_settings
from app.infrastructure.observability.logging import configure_logging
from app.infrastructure.redis.client import redis_lifecycle
from app.infrastructure.redis.settings import get_redis_settings


@dataclass
class AppContext:
    settings: AppSettings
    engine: AsyncEngine
    session_factory: async_sessionmaker
    redis: Redis
    broker: RabbitBroker
    messaging: Messaging


@asynccontextmanager
async def app_context(
    settings: AppSettings | None = None,
    broker: RabbitBroker | None = None,
) -> AsyncGenerator[AppContext]:
    settings = settings or get_app_settings()
    db_settings = get_database_settings()
    redis_settings = get_redis_settings()
    messaging_settings = get_messaging_settings()

    configure_logging(settings)
    log = structlog.get_logger()
    log.info("app.starting", app_name=settings.app_name, env=settings.app_env)

    async for engine in engine_lifecycle(db_settings):
        async for redis in redis_lifecycle(redis_settings):
            session_factory = build_session_factory(engine)
            _broker = broker if broker is not None else build_broker(messaging_settings)
            messaging = Messaging(
                broker=_broker,
                session_factory=session_factory,
                settings=messaging_settings,
            )
            yield AppContext(
                settings=settings,
                engine=engine,
                session_factory=session_factory,
                redis=redis,
                broker=_broker,
                messaging=messaging,
            )

    log.info("app.stopped")
