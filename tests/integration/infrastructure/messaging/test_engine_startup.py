"""engine 启动应声明 retry 拓扑并绑定 dispatcher;ASGI 组合路径不双 consume。"""

from __future__ import annotations

import asyncio
from urllib.parse import quote, urlparse

import httpx
import pytest

pytestmark = pytest.mark.integration


def _consumer_tags(messaging_settings, queue: str) -> list[str]:
    """管理接口取某队列当前活着的 consumer tag 列表。"""
    u = urlparse(messaging_settings.broker_url.get_secret_value())
    vhost = quote(u.path.lstrip("/") or "/", safe="")
    with httpx.Client(auth=(u.username or "", u.password or ""), timeout=5.0) as c:
        r = c.get(f"http://{u.hostname}:15672/api/queues/{vhost}/{quote(queue, safe='')}")
        r.raise_for_status()
        return [cd["consumer_tag"] for cd in r.json().get("consumer_details", [])]


async def _consumer_tags_settled(messaging_settings, queue: str) -> list[str]:
    """mgmt 统计有秒级可见延迟,轮询等 consumer 注册可见。"""
    for _ in range(10):
        tags = _consumer_tags(messaging_settings, queue)
        if tags:
            return tags
        await asyncio.sleep(1.0)
    return []


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


async def test_start_consumers_composed_with_broker_start_keeps_single_consumer(
    broker, session_factory, test_messaging_settings
):
    """ASGI 组合路径(engine 准备 + AsgiFastStream 的 _start_broker)下每队列恰 1 个 consumer。

    回归:engine.start_consumers 内部 start + 框架 _start_broker 再 start,
    同一 channel 上双 basic_consume,RabbitMQ per-consumer prefetch 使实际并发翻倍。
    """
    from app.infrastructure.messaging.engine import Messaging
    from app.infrastructure.messaging.task_consumer import clear_pending_consumers, task_consumer

    clear_pending_consumers()

    @task_consumer("test.smoke.single_consumer", inbox=False, concurrency=5)
    async def handler(payload: dict) -> None:
        pass

    msg = Messaging(
        broker=broker,
        session_factory=session_factory,
        settings=test_messaging_settings,
    )
    try:
        await msg.start_consumers(start_broker=False)
        await broker.start()  # AsgiFastStream._start_broker 的等价动作
        tags = await _consumer_tags_settled(test_messaging_settings, "test.smoke.single_consumer")
        assert len(tags) == 1
    finally:
        await msg.stop()
        clear_pending_consumers()
