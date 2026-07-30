"""Scheduler 进程入口:APScheduler AsyncIOScheduler + outbox/inbox retention job。

启动:`python -m app.entrypoints.scheduler`
停止:SIGTERM 或 Ctrl-C(SIGINT)

资源经 bootstrap/scheduler.py 的 lifespan 构造;进程不接入 dishka、不连 DummyJSON。
"""

from __future__ import annotations

import asyncio
import contextlib
import signal

import structlog

from app.bootstrap.scheduler import scheduler_lifespan
from app.config.settings import get_app_settings
from app.infrastructure.observability.logging import configure_logging


async def main() -> None:
    settings = get_app_settings()
    configure_logging(settings)
    log = structlog.get_logger()

    async with scheduler_lifespan():
        log.info("scheduler.ready", app_name=settings.app_name)

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        # Windows ProactorEventLoop 不支持 add_signal_handler,逐个 suppress
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signal.SIGTERM, stop.set)
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signal.SIGINT, stop.set)

        await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
