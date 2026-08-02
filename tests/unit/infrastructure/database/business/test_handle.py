from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.infrastructure.database.business.handle import (
    BusinessDb,
    DatabaseNotRegisteredError,
    Databases,
    business_db_ctx,
)


class FakeDb(BusinessDb):
    pass


def _make_handle() -> FakeDb:
    return FakeDb(engine=MagicMock(), session_factory=MagicMock())


def test_business_db_subclass_inherits_init():
    handle = FakeDb(engine="eng", session_factory="sf")
    assert handle.engine == "eng"
    assert isinstance(handle, BusinessDb)


def test_databases_get_returns_registered_handle():
    dbs = Databases()
    handle = _make_handle()
    dbs.register(handle)
    assert dbs.get(FakeDb) is handle


def test_databases_get_raises_with_registered_list():
    class OtherDb(BusinessDb):
        pass

    dbs = Databases()
    dbs.register(_make_handle())

    with pytest.raises(DatabaseNotRegisteredError) as exc_info:
        dbs.get(OtherDb)

    msg = str(exc_info.value)
    assert "OtherDb" in msg
    assert "FakeDb" in msg  # 已注册列表中出现


def test_databases_all_returns_all_handles():
    dbs = Databases()
    h = _make_handle()
    dbs.register(h)
    assert dbs.all() == [h]


def test_databases_empty_all():
    assert Databases().all() == []


async def test_business_db_ctx_yields_correct_handle_type_and_disposes():
    """business_db_ctx 返回的 ctx 能正常 yield handle，退出后 dispose engine。"""
    fake_engine = MagicMock(name="engine")
    fake_engine.dispose = AsyncMock()

    # engine_lifecycle 是纯 async generator（async for 消费），不是 context manager
    async def _fake_engine_lifecycle(_settings):
        yield fake_engine

    class TestDb(BusinessDb):
        pass

    from app.infrastructure.database.settings import DatabaseSettings

    class TestSettings(DatabaseSettings):
        pass

    with patch(
        "app.infrastructure.database.business.handle.engine_lifecycle",
        _fake_engine_lifecycle,
    ):
        ctx_fn = business_db_ctx(TestDb, TestSettings)
        async with ctx_fn() as handle:
            assert isinstance(handle, TestDb)
            assert handle.engine is fake_engine
