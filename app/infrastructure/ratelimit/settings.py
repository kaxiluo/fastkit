from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class RateLimitSettings(BaseSettings):
    """限流(令牌桶等待)。"""

    model_config = SettingsConfigDict(
        env_prefix="RATELIMIT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    poll_interval_seconds: float = 0.05  # 桶空时令牌补充的轮询间隔秒数
    max_wait_seconds: float = 5.0  # 桶空时调用方最大等待秒数;超时抛限流错误


@lru_cache
def get_ratelimit_settings() -> RateLimitSettings:
    return RateLimitSettings()
