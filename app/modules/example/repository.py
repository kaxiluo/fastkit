"""ExampleWidgetRepository:example_widgets 表 CRUD。"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.modules.example.models import ExampleWidget


class ExampleWidgetRepository:
    async def create(
        self,
        session: AsyncSession,
        *,
        payload: dict,
    ) -> ExampleWidget:
        widget = ExampleWidget(payload=payload, status="pending", attempts=0)
        session.add(widget)
        await session.flush()
        return widget

    async def get(self, session: AsyncSession, widget_id: int) -> ExampleWidget | None:
        return await session.get(ExampleWidget, widget_id)

    async def get_for_update(
        self,
        session: AsyncSession,
        widget_id: int,
    ) -> ExampleWidget | None:
        stmt = select(ExampleWidget).where(ExampleWidget.id == widget_id).with_for_update()
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def update(
        self,
        session: AsyncSession,
        widget_id: int,
        **fields: Any,
    ) -> None:
        stmt = (
            update(ExampleWidget)
            .where(ExampleWidget.id == widget_id)
            .values(**fields, updated_at=func.now())
        )
        await session.execute(stmt)
