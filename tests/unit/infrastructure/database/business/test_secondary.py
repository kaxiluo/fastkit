from __future__ import annotations


def test_secondary_settings_reads_secondary_prefix(monkeypatch):
    """DATABASE_SECONDARY_URL 被 secondary settings 读取,DATABASE_URL 不被读取。"""
    monkeypatch.setenv("DATABASE_SECONDARY_URL", "postgresql+asyncpg://u:p@h/secondary")
    monkeypatch.delenv("DATABASE_URL", raising=False)

    from app.infrastructure.database.business.secondary import SecondaryDatabaseSettings

    s = SecondaryDatabaseSettings()
    assert "secondary" in s.url.get_secret_value()


def test_main_settings_does_not_read_secondary_url(monkeypatch):
    """DATABASE_SECONDARY_URL 不被主库 DatabaseSettings 误读。"""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h/main")
    monkeypatch.setenv("DATABASE_SECONDARY_URL", "postgresql+asyncpg://u:p@h/secondary")

    from app.infrastructure.database.settings import DatabaseSettings

    s = DatabaseSettings()
    assert "main" in s.url.get_secret_value()


def test_secondary_db_is_business_db_subclass():
    from app.infrastructure.database.business.handle import BusinessDb
    from app.infrastructure.database.business.secondary import SecondaryDb

    assert issubclass(SecondaryDb, BusinessDb)


def test_secondary_base_is_independent_from_main():
    from app.infrastructure.database.base import Base
    from app.infrastructure.database.business.secondary import SecondaryBase

    assert SecondaryBase.metadata is not Base.metadata
