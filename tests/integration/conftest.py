"""integration 层 fixture:DB / Redis / Broker,env 缺失自动 skip。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from faststream.rabbit import RabbitBroker
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.infrastructure.database.engine import build_engine
from app.infrastructure.database.session import build_session_factory
from app.infrastructure.database.settings import DatabaseSettings
from app.infrastructure.messaging.broker import build_broker
from app.infrastructure.messaging.settings import MessagingSettings
from app.infrastructure.redis.client import redis_lifecycle
from app.infrastructure.redis.settings import RedisSettings

_PROJECT_ROOT = Path(__file__).parent.parent.parent


@pytest.fixture(scope="session", autouse=True)
def run_migrations(test_database_settings: DatabaseSettings) -> None:
    alembic_cfg = Config(str(_PROJECT_ROOT / "alembic.ini"))
    alembic_cfg.set_main_option("sqlalchemy.url", test_database_settings.url.get_secret_value())
    command.upgrade(alembic_cfg, "head")


@pytest_asyncio.fixture
async def db_engine(
    test_database_settings: DatabaseSettings,
) -> AsyncIterator[AsyncEngine]:
    engine = build_engine(test_database_settings)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(db_engine: AsyncEngine) -> async_sessionmaker:
    return build_session_factory(db_engine)


@pytest_asyncio.fixture
async def redis_client(
    test_redis_settings: RedisSettings,
) -> AsyncIterator[Redis]:
    async for client in redis_lifecycle(test_redis_settings):
        yield client


@pytest_asyncio.fixture
async def broker(
    test_messaging_settings: MessagingSettings,
) -> AsyncIterator[RabbitBroker]:
    b = build_broker(test_messaging_settings)
    await b.connect()
    try:
        yield b
    finally:
        await b.stop()


@pytest.fixture
def fast_retry_settings(test_messaging_settings: MessagingSettings, monkeypatch) -> MessagingSettings:
    """集成测试专用:TTL 500ms 加速 retry 回投。"""
    monkeypatch.setattr(test_messaging_settings, "retry_ttl_ms", 500)
    monkeypatch.setattr(
        test_messaging_settings,
        "retry_queue",
        f"{test_messaging_settings.app_name}.retry.500ms",
    )
    return test_messaging_settings
