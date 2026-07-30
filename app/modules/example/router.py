"""example 域 HTTP 入口(挂载于 /api/v1):POST /api/v1/example/widgets, GET /api/v1/example/widgets/{id}。"""

from __future__ import annotations

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.messaging import EventRegistry
from app.modules.example.repository import ExampleWidgetRepository
from app.modules.example.schemas import CreateWidgetRequest, WidgetResponse
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
