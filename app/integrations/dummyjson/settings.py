from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class DummyJsonSettings(BaseSettings):
    """DummyJson 集成(httpx 客户端)。"""

    model_config = SettingsConfigDict(
        env_prefix="DUMMYJSON_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    base_url: str = "https://dummyjson.com"
    timeout: float = 10.0


@lru_cache
def get_dummyjson_settings() -> DummyJsonSettings:
    return DummyJsonSettings()
