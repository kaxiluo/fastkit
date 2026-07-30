"""FastStream worker 入口:consume + relay;ASGI 暴露 /health 与(dev/test)/asyncapi。

启动:`uvicorn app.entrypoints.worker:app --port 8001`

worker 进程不接入 dishka container —— FastStream 0.7 与 dishka 的官方集成
(`dishka-faststream`)未引入;资源经 bootstrap/worker.py 的 lifespan 构造,
语义等价、依赖最小。

broker 在 build_worker() 最外层构造，同时传给 AsgiFastStream（供 AsyncAPI 与
health check 使用）和 worker_lifespan（供 Messaging 绑定 consumer），确保两者
使用同一连接实例，AsyncAPI 文档能正确反映已注册的消费者。
"""

from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager

from faststream.asgi import AsgiFastStream, make_ping_asgi
from faststream.specification import AsyncAPI

from app.bootstrap.worker import worker_lifespan
from app.config.settings import get_app_settings
from app.infrastructure.messaging.broker import build_broker
from app.infrastructure.messaging.settings import get_messaging_settings
from app.infrastructure.observability.logging import configure_logging


def build_worker() -> AsgiFastStream:
    app_settings = get_app_settings()
    messaging_settings = get_messaging_settings()
    configure_logging(app_settings)
    broker = build_broker(messaging_settings)

    @asynccontextmanager
    async def _lifespan():
        async with AsyncExitStack() as stack:
            ctx = await stack.enter_async_context(worker_lifespan(broker=broker))
            yield ctx

    return AsgiFastStream(
        broker,
        asgi_routes=[("/health", make_ping_asgi(broker, timeout=5.0))],
        specification=AsyncAPI(),
        asyncapi_path="/asyncapi" if app_settings.app_env != "prod" else None,
        lifespan=_lifespan,
    )


app = build_worker()
