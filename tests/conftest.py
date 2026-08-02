"""根 conftest:加载 .env.test,提供全局组件 settings 覆盖。"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic_settings import SettingsConfigDict

from app.config.settings import AppSettings, get_app_settings
from app.infrastructure.database.settings import DatabaseSettings, get_database_settings
from app.infrastructure.messaging.settings import (
    MessagingSettings,
    get_messaging_settings,
)
from app.infrastructure.redis.settings import RedisSettings, get_redis_settings

_TEST_ENV = Path(__file__).parent.parent / ".env.test"


def _env_file() -> str | None:
    return str(_TEST_ENV) if _TEST_ENV.exists() else None


def _make_test_app_settings() -> AppSettings:
    class _TestAppSettings(AppSettings):
        model_config = SettingsConfigDict(
            env_file=_env_file(),
            env_file_encoding="utf-8",
            extra="ignore",
        )

    return _TestAppSettings()


def _make_test_database_settings() -> DatabaseSettings:
    class _TestDatabaseSettings(DatabaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="DATABASE_",
            env_file=_env_file(),
            env_file_encoding="utf-8",
            extra="ignore",
            case_sensitive=False,
        )

    return _TestDatabaseSettings()


def _make_test_secondary_database_settings() -> DatabaseSettings:
    class _TestSecondaryDatabaseSettings(DatabaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="DATABASE_SECONDARY_",
            env_file=_env_file(),
            env_file_encoding="utf-8",
            extra="ignore",
            case_sensitive=False,
        )

    return _TestSecondaryDatabaseSettings()


def _make_test_redis_settings() -> RedisSettings:
    class _TestRedisSettings(RedisSettings):
        model_config = SettingsConfigDict(
            env_prefix="REDIS_",
            env_file=_env_file(),
            env_file_encoding="utf-8",
            extra="ignore",
            case_sensitive=False,
        )

    return _TestRedisSettings()


def _make_test_messaging_settings() -> MessagingSettings:
    class _TestMessagingSettings(MessagingSettings):
        model_config = SettingsConfigDict(
            env_prefix="MESSAGING_",
            env_file=_env_file(),
            env_file_encoding="utf-8",
            extra="ignore",
            case_sensitive=False,
            populate_by_name=True,
        )

    return _TestMessagingSettings()


def _skip_if_no_env_test() -> None:
    if not _TEST_ENV.exists():
        pytest.skip(f".env.test 不存在;复制 .env.test.example 到 {_TEST_ENV} 并按需修改")


@pytest.fixture(scope="session")
def test_settings() -> AppSettings:
    """会话级 AppSettings;绝不复用 app 的 get_app_settings() 缓存。"""
    _skip_if_no_env_test()
    return _make_test_app_settings()


@pytest.fixture(scope="session")
def test_database_settings() -> DatabaseSettings:
    _skip_if_no_env_test()
    return _make_test_database_settings()


@pytest.fixture(scope="session")
def test_secondary_database_settings() -> DatabaseSettings:
    _skip_if_no_env_test()
    return _make_test_secondary_database_settings()


@pytest.fixture(scope="session")
def test_redis_settings() -> RedisSettings:
    _skip_if_no_env_test()
    return _make_test_redis_settings()


@pytest.fixture(scope="session")
def test_messaging_settings() -> MessagingSettings:
    _skip_if_no_env_test()
    return _make_test_messaging_settings()


@pytest.fixture(autouse=True)
def _reset_settings_cache():
    """每个测试后清各 settings lru_cache,避免测试间污染。"""
    yield
    get_app_settings.cache_clear()
    get_database_settings.cache_clear()
    get_redis_settings.cache_clear()
    get_messaging_settings.cache_clear()


def pytest_collection_modifyitems(items):
    """按目录自动打 marker(目录即 marker 约定),显式 marker 优先。

    tests/integration/ → integration;tests/contract/ → contract;
    tests/e2e/ → e2e;其他(含 tests/unit/) → unit。
    """

    root = Path(__file__).parent
    section_to_marker = {"integration", "contract", "e2e"}
    known = section_to_marker | {"unit"}
    for item in items:
        if any(m.name in known for m in item.iter_markers()):
            continue
        try:
            rel = Path(item.fspath).relative_to(root)
            section = rel.parts[0] if rel.parts else ""
        except ValueError:
            section = ""
        name = section if section in section_to_marker else "unit"
        item.add_marker(getattr(pytest.mark, name))
