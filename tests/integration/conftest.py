"""integration 层 fixture:DB / Redis / Broker,env 缺失自动 skip。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx
import pytest
import pytest_asyncio
from alembic.config import Config
from faststream.rabbit import RabbitBroker
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from alembic import command
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


def _purge_test_vhost(broker_url: str) -> None:
    """删光 test vhost 里所有队列(连同其中残留消息)。

    durable 队列跨 pytest 会话持久:上一次被中断的 run 可能把带 attempts 的消息
    经 retry TTL dead-letter 回业务队列静躺,污染下一次 run。整库清空是零维护的兜底
    (不逐个列队列名),仅在 test vhost 已与 dev 隔离时安全。各测试 start_consumers
    会按需重新声明队列。管理端口用 RabbitMQ 默认 15672,host 从 broker_url 取。
    """
    u = urlparse(broker_url)
    vhost = quote(u.path.lstrip("/") or "/", safe="")
    base = f"http://{u.hostname}:15672/api"
    with httpx.Client(auth=(u.username or "", u.password or ""), timeout=5.0) as c:
        resp = c.get(f"{base}/queues/{vhost}")
        resp.raise_for_status()
        for q in resp.json():
            c.delete(f"{base}/queues/{vhost}/{quote(q['name'], safe='')}").raise_for_status()


@pytest.fixture(scope="session", autouse=True)
def clean_test_vhost(test_messaging_settings: MessagingSettings) -> None:
    """session 开跑前清除跨会话/中断残留的化石消息与坏拓扑。见 _purge_test_vhost。"""
    _purge_test_vhost(test_messaging_settings.broker_url.get_secret_value())


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
async def secondary_db_engine(
    test_secondary_database_settings: DatabaseSettings,
) -> AsyncIterator[AsyncEngine]:
    """第二业务库 engine。DATABASE_SECONDARY_URL 不可达时 skip 而非 fail。"""
    from sqlalchemy import text as sa_text
    from sqlalchemy.exc import OperationalError

    engine = build_engine(test_secondary_database_settings)
    try:
        async with engine.connect() as conn:
            await conn.execute(sa_text("SELECT 1"))
    except (OperationalError, OSError):
        # OperationalError:SQLAlchemy 包装的 asyncpg 连接失败
        # OSError:纯网络层不可达
        await engine.dispose()
        pytest.skip("DATABASE_SECONDARY_URL 不可达,跳过 secondary db 集成测试")
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def secondary_db(secondary_db_engine: AsyncEngine):
    from app.infrastructure.database.business.secondary import SecondaryDb
    from app.infrastructure.database.session import build_session_factory

    return SecondaryDb(
        engine=secondary_db_engine,
        session_factory=build_session_factory(secondary_db_engine),
    )


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
def fast_retry_settings(
    test_messaging_settings: MessagingSettings, monkeypatch
) -> MessagingSettings:
    """集成测试专用:TTL 500ms 加速 retry 回投。"""
    monkeypatch.setattr(test_messaging_settings, "retry_ttl_ms", 500)
    monkeypatch.setattr(
        test_messaging_settings,
        "retry_queue",
        f"{test_messaging_settings.app_name}.retry.500ms",
    )
    return test_messaging_settings
