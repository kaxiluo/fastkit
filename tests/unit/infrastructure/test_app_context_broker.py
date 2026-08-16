"""app_context(broker=...) 复用外部 broker 的单元测试(worker 进程共享连接场景)。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from faststream.rabbit import RabbitBroker

from app.bootstrap.lifecycle import app_context
from app.config.settings import AppSettings


def _agen(value):
    async def _gen():
        yield value

    return _gen()


@pytest.mark.asyncio
async def test_app_context_reuses_provided_broker(test_settings: AppSettings):
    """外部传入 broker 时,app_context 直接复用、不调 build_broker。"""
    external_broker = MagicMock(spec=RabbitBroker)

    with (
        patch("app.bootstrap.lifecycle.configure_logging"),
        patch("app.bootstrap.lifecycle.engine_lifecycle", side_effect=lambda _: _agen(MagicMock())),
        patch("app.bootstrap.lifecycle.redis_lifecycle", side_effect=lambda _: _agen(MagicMock())),
        patch("app.bootstrap.lifecycle.build_session_factory", return_value=MagicMock()),
        patch("app.bootstrap.lifecycle.build_broker") as mock_build,
    ):
        async with app_context(settings=test_settings, broker=external_broker) as ctx:
            assert ctx.broker is external_broker

    mock_build.assert_not_called()
