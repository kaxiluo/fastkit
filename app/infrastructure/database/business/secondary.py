# demo:secondary-db —— 删除示例时连带删本文件
from __future__ import annotations

from pydantic_settings import SettingsConfigDict
from sqlalchemy.orm import DeclarativeBase

from app.infrastructure.database.business.handle import BusinessDb, business_db_ctx
from app.infrastructure.database.settings import DatabaseSettings


class SecondaryDatabaseSettings(DatabaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DATABASE_SECONDARY_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


class SecondaryBase(DeclarativeBase):
    """secondary 库 ORM 模型的独立继承点,metadata 与主库隔离。"""


class SecondaryDb(BusinessDb):
    """secondary 库注入类型。仅为 DI 类型区分,无新增字段。"""


secondary_db_ctx = business_db_ctx(SecondaryDb, SecondaryDatabaseSettings)
