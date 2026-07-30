"""ExampleWidgetService 单元测试:三分支覆盖。"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


class _CtxSession:
    """异步上下文 + session.begin() 上下文 双兼容 mock。"""

    def __init__(self):
        self.execute = AsyncMock()
        self.get = AsyncMock()
        self.add = MagicMock()
        self.flush = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def begin(self):
        return self


@pytest.fixture
def session_factory():
    session = _CtxSession()

    def factory():
        return session

    factory.session = session
    return factory


@pytest.fixture
def repository():
    r = MagicMock()
    r.create = AsyncMock()
    r.get = AsyncMock()
    r.get_for_update = AsyncMock()
    r.update = AsyncMock()
    return r


@pytest.fixture
def events():
    e = MagicMock()
    e.example_widget_requested = MagicMock()
    e.example_widget_requested.publish = AsyncMock()
    return e


async def test_create_widget_publishes_event_in_same_transaction(
    session_factory, repository, events
):
    from app.modules.example.models import ExampleWidget
    from app.modules.example.service import ExampleWidgetService

    widget = ExampleWidget(id=1, payload={}, status="pending", attempts=0)
    repository.create.return_value = widget

    service = ExampleWidgetService(
        session_factory=session_factory, repository=repository, events=events
    )
    result = await service.create_widget(payload={})

    assert result is widget
    repository.create.assert_awaited_once()
    events.example_widget_requested.publish.assert_awaited_once()


async def test_process_widget_happy_path_marks_finished(session_factory, repository, events):
    from app.modules.example.models import ExampleWidget
    from app.modules.example.service import ExampleWidgetService

    widget = ExampleWidget(id=1, payload={}, status="pending", attempts=0)
    repository.get_for_update.return_value = widget

    service = ExampleWidgetService(
        session_factory=session_factory, repository=repository, events=events
    )
    await service.process_widget(1)

    assert repository.update.await_count == 2
    kw_calls = [c.kwargs for c in repository.update.await_args_list]
    statuses = [c.get("status") for c in kw_calls]
    assert "running" in statuses
    assert "finished" in statuses


async def test_process_widget_fail_until_raises_and_marks_failed(
    session_factory, repository, events
):
    from app.modules.example.models import ExampleWidget
    from app.modules.example.service import (
        ExampleWidgetFailedError,
        ExampleWidgetService,
    )

    widget = ExampleWidget(
        id=2,
        payload={"fail_until_attempt": 2},
        status="pending",
        attempts=0,
    )
    repository.get_for_update.return_value = widget

    service = ExampleWidgetService(
        session_factory=session_factory, repository=repository, events=events
    )
    with pytest.raises(ExampleWidgetFailedError):
        await service.process_widget(2)

    statuses = [c.kwargs.get("status") for c in repository.update.await_args_list]
    assert "running" in statuses
    assert "failed" in statuses


async def test_process_widget_not_found_raises(session_factory, repository, events):
    from app.modules.example.service import (
        ExampleWidgetNotFoundError,
        ExampleWidgetService,
    )

    repository.get_for_update.return_value = None

    service = ExampleWidgetService(
        session_factory=session_factory, repository=repository, events=events
    )
    with pytest.raises(ExampleWidgetNotFoundError):
        await service.process_widget(999)
