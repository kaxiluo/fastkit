from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    """Redis 连接池。"""

    model_config = SettingsConfigDict(
        env_prefix="REDIS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    url: SecretStr
    max_connections: int = 50
    health_check_interval: int = 30  # 自动健康检查间隔秒数


@lru_cache
def get_redis_settings() -> RedisSettings:
    return RedisSettings()
