"""Worker 进程 lifespan 组装:app_context() + broker.start_consumers。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from faststream.rabbit import RabbitBroker

from app.modules.example import consumers as _example_consumers  # noqa: F401  触发 @task_consumer
from app.modules.example import events as _example_events  # noqa: F401  触发 @event 注册

from .container import WORKER_CLIENTS, integrations_lifecycle
from .lifecycle import AppContext, app_context


@asynccontextmanager
async def worker_lifespan(broker: RabbitBroker | None = None) -> AsyncGenerator[AppContext]:
    log = structlog.get_logger()
    async with app_context(broker=broker) as ctx, integrations_lifecycle(*WORKER_CLIENTS) as integrations:
        await ctx.messaging.start_consumers(integrations=integrations)
        log.info("worker.started", app_name=ctx.settings.app_name)
        try:
            yield ctx
        finally:
            log.info("worker.stopping")
            await ctx.messaging.stop()
