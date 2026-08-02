from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TypeVar

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.infrastructure.database.engine import engine_lifecycle
from app.infrastructure.database.session import build_session_factory
from app.infrastructure.database.settings import DatabaseSettings

BusinessDbT = TypeVar("BusinessDbT", bound="BusinessDb")


@dataclass(frozen=True)
class BusinessDb:
    """业务库句柄基类。子类仅用于 DI 类型区分,无需新增字段。

    子类不需要重声明 @dataclass,直接继承 frozen dataclass 的 __init__。
    """

    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]


class DatabaseNotRegisteredError(RuntimeError):
    """业务侧 .get(SomeDb) 取一个未在本进程装配的业务库。"""

    def __init__(self, typ: type, registered: list[type]) -> None:
        registered_names = "\n".join(f"  - {t.__name__}" for t in registered) or "  (none)"
        super().__init__(
            f"{typ.__name__} is not registered in this process.\n"
            f"Registered databases:\n{registered_names}\n"
            f"Add the corresponding *_db_ctx to the process's *_DATABASES tuple "
            f"in bootstrap/container.py."
        )


class Databases:
    """进程级业务库句柄注册表,供 worker task_consumer / API dishka provider 共用。

    与 Integrations 对称但独立——避免 DB handle 与 HTTP client 混用。
    未注册时 .get() 抛 DatabaseNotRegisteredError(fail-fast,不静默降级)。
    """

    def __init__(self) -> None:
        self._registry: dict[type, BusinessDb] = {}

    def register(self, handle: BusinessDb) -> None:
        self._registry[type(handle)] = handle

    def get(self, typ: type[BusinessDbT]) -> BusinessDbT:
        try:
            return self._registry[typ]  # type: ignore[return-value]
        except KeyError:
            raise DatabaseNotRegisteredError(typ, list(self._registry)) from None

    def all(self) -> list[BusinessDb]:
        """供 build_databases_provider 遍历,API 侧 dishka provide 用。"""
        return list(self._registry.values())


def business_db_ctx(
    handle_type: type[BusinessDbT],
    settings_type: type[DatabaseSettings],
) -> Callable[[], AsyncGenerator[BusinessDbT]]:
    """返回一个 @asynccontextmanager:建引擎 → 包成 handle_type → 退出时 dispose。

    settings_type() 在 ctx 内实例化一次(进程级单例),无需 @lru_cache——
    databases_lifecycle 每进程只 enter 一次,实例化次数 = 1。
    """

    @asynccontextmanager
    async def _ctx() -> AsyncGenerator[BusinessDbT]:
        settings = settings_type()
        async for eng in engine_lifecycle(settings):
            yield handle_type(eng, build_session_factory(eng))

    return _ctx
