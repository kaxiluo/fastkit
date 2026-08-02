from __future__ import annotations

import pytest

from app.infrastructure.database.business.handle import Databases
from app.infrastructure.database.business.secondary import SecondaryDb


@pytest.mark.integration
async def test_secondary_db_connects(secondary_db: SecondaryDb):
    """secondary db 能建连并执行简单查询。"""
    from sqlalchemy import text

    async with secondary_db.session_factory() as session:
        result = await session.execute(text("SELECT 1"))
        assert result.scalar() == 1


@pytest.mark.integration
async def test_secondary_db_injectable_via_databases_bundle(secondary_db: SecondaryDb):
    """Databases bundle .get(SecondaryDb) 返回正确 handle。"""
    dbs = Databases()
    dbs.register(secondary_db)

    retrieved = dbs.get(SecondaryDb)
    assert retrieved is secondary_db
    assert retrieved.engine is secondary_db.engine


@pytest.mark.integration
async def test_build_databases_provider_provides_by_type(secondary_db: SecondaryDb):
    """build_databases_provider 能让 dishka container 按 SecondaryDb 类型注入正确实例。"""
    from dishka import make_container

    from app.bootstrap.api import build_databases_provider

    dbs = Databases()
    dbs.register(secondary_db)

    provider = build_databases_provider(dbs)
    container = make_container(provider)
    with container() as c:
        result = c.get(SecondaryDb)
    assert result is secondary_db
