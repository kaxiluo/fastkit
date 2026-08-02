from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.infrastructure.database.business.handle import (
    BusinessDb,
    Databases,
    DatabaseNotRegisteredError,
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
