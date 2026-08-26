"""example 域 HTTP 入口(挂载于 /api/v1):POST /example/widgets, GET /example/widgets/{id}, POST /example/slow-tasks。"""

from __future__ import annotations

from uuid import uuid4

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.messaging import EventRegistry
from app.modules.example.events import ExampleSlowTaskRequested
from app.modules.example.repository import ExampleWidgetRepository
from app.modules.example.schemas import (
    CreateSlowTaskRequest,
    CreateWidgetRequest,
    SlowTaskResponse,
    WidgetResponse,
)
from app.modules.example.service import ExampleWidgetService

router = APIRouter(prefix="/example", tags=["example"], route_class=DishkaRoute)


def _build_service(
    session_factory: async_sessionmaker[AsyncSession],
    events: EventRegistry,
) -> ExampleWidgetService:
    return ExampleWidgetService(
        session_factory=session_factory,
        repository=ExampleWidgetRepository(),
        events=events,
    )


@router.post("/widgets", response_model=WidgetResponse, status_code=201)
async def create_widget(
    req: CreateWidgetRequest,
    session_factory: FromDishka[async_sessionmaker[AsyncSession]],
    events: FromDishka[EventRegistry],
) -> WidgetResponse:
    service = _build_service(session_factory, events)
    widget = await service.create_widget(payload=req.payload)
    return WidgetResponse.model_validate(widget, from_attributes=True)


@router.get("/widgets/{widget_id}", response_model=WidgetResponse)
async def get_widget(
    widget_id: int,
    session_factory: FromDishka[async_sessionmaker[AsyncSession]],
    events: FromDishka[EventRegistry],
) -> WidgetResponse:
    service = _build_service(session_factory, events)
    widget = await service.get_widget(widget_id)
    if widget is None:
        raise HTTPException(status_code=404, detail="widget not found")
    return WidgetResponse.model_validate(widget, from_attributes=True)


@router.post("/slow-tasks", response_model=SlowTaskResponse, status_code=202)
async def create_slow_tasks(
    req: CreateSlowTaskRequest,
    session_factory: FromDishka[async_sessionmaker[AsyncSession]],
    events: FromDishka[EventRegistry],
) -> SlowTaskResponse:
    """投递 count 条慢任务事件(同事务 outbox),供 slowtask consumer 消费。"""
    task_ids = [uuid4().hex[:8] for _ in range(req.count)]
    async with session_factory() as session, session.begin():
        for task_id in task_ids:
            await events.example_slowtask_requested.publish(
                session, ExampleSlowTaskRequested(task_id=task_id)
            )
    return SlowTaskResponse(task_ids=task_ids)
