from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """DB 连接池(SQLAlchemy AsyncEngine)。"""

    model_config = SettingsConfigDict(
        env_prefix="DATABASE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    url: SecretStr
    pool_size: int = 10  # 池内常驻连接数
    max_overflow: int = 10  # 超出 pool_size 后允许的临时扩张
    pool_recycle: int = 1800  # 单连接最大存活秒数,绕开 DB 侧 idle 超时
    pool_timeout: int = 60  # 池满时排队等待秒数;偏长用于软背压,避免高峰直接抛 TimeoutError 走 retry/DLQ


@lru_cache
def get_database_settings() -> DatabaseSettings:
    return DatabaseSettings()
