"""Scheduler 进程入口:ASGI app 承载 APScheduler 与 /health 探针。

启动:`uvicorn app.entrypoints.scheduler:app --port 8002`
停止:SIGTERM 或 Ctrl-C(SIGINT),由 uvicorn 托管

资源与 logging 经 bootstrap/scheduler.py 的 lifespan 构造;进程不接入 dishka。
/health 仅作存活探针(进程/事件循环活着即 200),不探测外部依赖 ——
scheduler 允许零装配 DB/Redis 启动,没有统一的就绪标准。
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.bootstrap.scheduler import scheduler_lifespan


@asynccontextmanager
async def _lifespan(_: FastAPI) -> AsyncGenerator[None]:
    async with scheduler_lifespan():
        yield


app = FastAPI(lifespan=_lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
