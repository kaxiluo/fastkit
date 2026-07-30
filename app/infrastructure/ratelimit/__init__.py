"""限流组件对外 API:RateLimiter 与进程级 builder。"""

from limits.aio.strategies import MovingWindowRateLimiter
from limits.storage import storage_from_string

from app.config.settings import AppSettings
from app.infrastructure.ratelimit.limiter import RateLimiter
from app.infrastructure.ratelimit.settings import RateLimitSettings
from app.infrastructure.redis.settings import RedisSettings

__all__ = ["RateLimiter", "build_rate_limiter"]


def build_rate_limiter(
    redis_settings: RedisSettings,
    app_settings: AppSettings,
    ratelimit_settings: RateLimitSettings,
) -> RateLimiter:
    url = redis_settings.url.get_secret_value()
    storage = storage_from_string(f"async+{url}", implementation="redispy")
    strategy = MovingWindowRateLimiter(storage)
    return RateLimiter(
        strategy,
        namespace=app_settings.app_name,
        default_poll=ratelimit_settings.poll_interval_seconds,
        max_wait=ratelimit_settings.max_wait_seconds,
    )
