"""engine 启动应声明 retry 拓扑并绑定 dispatcher。"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


async def test_start_consumers_declares_retry_topology(
    broker, session_factory, test_messaging_settings
):
    """start_consumers 后,retry queue 必须存在(passive declare 不抛即真存在)。"""
    from app.infrastructure.messaging.engine import Messaging
    from app.infrastructure.messaging.task_consumer import clear_pending_consumers, task_consumer

    clear_pending_consumers()

    @task_consumer("test.smoke.startup", inbox=False)
    async def handler(payload: dict) -> None:
        pass

    msg = Messaging(
        broker=broker,
        session_factory=session_factory,
        settings=test_messaging_settings,
    )
    try:
        await msg.start_consumers()
        # passive=True:queue 必须已存在,否则 404 CHANNEL_ERROR。这真正验证 engine 声明了拓扑
        q = await broker._channel.declare_queue(
            test_messaging_settings.retry_queue,
            durable=True,
            passive=True,
        )
        assert q is not None
        # 同样验证 retry exchange
        ex = await broker._channel.declare_exchange(
            test_messaging_settings.retry_exchange,
            passive=True,
        )
        assert ex is not None
        # 验证 dispatcher 已注入(engine 内部状态)
        assert msg._dispatcher is not None
    finally:
        await msg.stop()
        clear_pending_consumers()
