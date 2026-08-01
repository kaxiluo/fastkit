"""验证 _make_entry 把 session_factory / integrations 透传给 wrapped。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.infrastructure.messaging.engine import _make_entry


async def test_make_entry_forwards_session_factory_and_integrations():
    wrapped = AsyncMock()
    session_factory = object()
    integrations = object()

    entry = _make_entry(wrapped, session_factory, integrations)
    await entry({"v": 1}, SimpleNamespace(headers={}))

    wrapped.assert_awaited_once()
    _args, kwargs = wrapped.await_args
    assert kwargs["session_factory"] is session_factory
    assert kwargs["integrations"] is integrations
