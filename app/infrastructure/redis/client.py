from collections.abc import AsyncGenerator

from redis.asyncio import Redis

from app.infrastructure.redis.settings import RedisSettings


def build_redis(settings: RedisSettings) -> Redis:
    return Redis.from_url(
        settings.url.get_secret_value(),
        max_connections=settings.max_connections,
        health_check_interval=settings.health_check_interval,
        socket_keepalive=True,
        decode_responses=False,
    )


async def redis_lifecycle(settings: RedisSettings) -> AsyncGenerator[Redis]:
    client = build_redis(settings)
    try:
        yield client
    finally:
        await client.aclose()
