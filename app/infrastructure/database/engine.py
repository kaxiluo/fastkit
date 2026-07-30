from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.infrastructure.database.settings import DatabaseSettings


def build_engine(settings: DatabaseSettings) -> AsyncEngine:
    return create_async_engine(
        settings.url.get_secret_value(),
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        pool_pre_ping=True,
        pool_recycle=settings.pool_recycle,
        pool_timeout=settings.pool_timeout,
    )


async def engine_lifecycle(settings: DatabaseSettings) -> AsyncGenerator[AsyncEngine]:
    engine = build_engine(settings)
    try:
        yield engine
    finally:
        await engine.dispose()
