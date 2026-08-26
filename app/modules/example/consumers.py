"""example 域 consumer:@task_consumer 处理 example.widget.requested / example.slowtask.requested。"""

from __future__ import annotations

import asyncio
import random
import time

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.messaging import RetryPolicy, task_consumer
from app.modules.example.events import ExampleSlowTaskRequested, ExampleWidgetRequested
from app.modules.example.repository import ExampleWidgetRepository
from app.modules.example.service import ExampleWidgetService

log = structlog.get_logger(__name__)

SLOW_TASK_SECONDS_MIN = 4.0
SLOW_TASK_SECONDS_MAX = 6.0


@task_consumer(
    "example.widget.requested",
    concurrency=1,
    retry=RetryPolicy(max_attempts=3, backoff="fixed"),
    inbox=True,
)
async def on_example_widget_requested(
    msg: ExampleWidgetRequested,
    *,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """从 msg 拿 widget_id,回库处理任务。"""
    service = ExampleWidgetService(
        session_factory=session_factory,
        repository=ExampleWidgetRepository(),
        events=None,
    )
    await service.process_widget(msg.widget_id)


@task_consumer("example.slowtask.requested", concurrency=10)
async def on_example_slow_task_requested(msg: ExampleSlowTaskRequested) -> None:
    """模拟慢消费:随机睡 4~6 秒;concurrency=10 为全集群全局上限,双 worker 部署下全局同时在执行的仍 ≤ 10。"""
    started = time.monotonic()
    log.info("example.slowtask.started", task_id=msg.task_id)
    await asyncio.sleep(random.uniform(SLOW_TASK_SECONDS_MIN, SLOW_TASK_SECONDS_MAX))
    log.info(
        "example.slowtask.finished",
        task_id=msg.task_id,
        elapsed_seconds=round(time.monotonic() - started, 2),
    )
