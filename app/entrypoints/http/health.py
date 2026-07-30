import asyncio

import structlog
from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from faststream.rabbit import RabbitBroker
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config.settings import AppSettings

router = APIRouter(route_class=DishkaRoute)


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def _probe_db(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def _probe_redis(client: Redis) -> None:
    await client.ping()


async def _probe_broker(broker: RabbitBroker, timeout: float) -> None:
    ok = await broker.ping(timeout=timeout)
    if not ok:
        raise RuntimeError("broker.ping returned False")


@router.get("/ready")
async def ready(
    settings: FromDishka[AppSettings],
    engine: FromDishka[AsyncEngine],
    redis: FromDishka[Redis],
    broker: FromDishka[RabbitBroker],
) -> JSONResponse:
    log = structlog.get_logger()
    probes = {
        "db": _probe_db(engine),
        "redis": _probe_redis(redis),
        "broker": _probe_broker(broker, settings.ready_broker_ping_timeout),
    }
    results = await asyncio.gather(*probes.values(), return_exceptions=True)

    checks: dict[str, str] = {}
    all_ok = True
    for name, result in zip(probes.keys(), results, strict=True):
        if isinstance(result, BaseException):
            all_ok = False
            checks[name] = f"error: {type(result).__name__}: {result}"
            log.warning("ready.probe_failed", probe=name, error=repr(result))
        else:
            checks[name] = "ok"

    status_code = 200 if all_ok else 503
    return JSONResponse(
        status_code=status_code,
        content={"status": "ok" if all_ok else "degraded", "checks": checks},
    )
