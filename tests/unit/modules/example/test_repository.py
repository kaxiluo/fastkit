"""ExampleWidgetRepository 单元测试:验证 SQL 构造与调用序列。"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def session():
    s = MagicMock()
    s.execute = AsyncMock()
    s.get = AsyncMock()
    s.add = MagicMock()
    s.flush = AsyncMock()
    return s


async def test_create_returns_widget(session):
    from app.modules.example.models import ExampleWidget
    from app.modules.example.repository import ExampleWidgetRepository

    repo = ExampleWidgetRepository()
    widget = await repo.create(session, payload={"k": 1})

    assert isinstance(widget, ExampleWidget)
    assert widget.payload == {"k": 1}
    assert widget.status == "pending"
    session.add.assert_called_once_with(widget)
    session.flush.assert_awaited_once()


async def test_get_calls_session_get(session):
    from app.modules.example.models import ExampleWidget
    from app.modules.example.repository import ExampleWidgetRepository

    repo = ExampleWidgetRepository()
    await repo.get(session, 42)
    session.get.assert_awaited_once_with(ExampleWidget, 42)


async def test_update_executes_update_stmt(session):
    from app.modules.example.repository import ExampleWidgetRepository

    repo = ExampleWidgetRepository()
    await repo.update(session, 7, status="finished", attempts=3)
    session.execute.assert_awaited_once()
