"""ExampleWidgetService:业务逻辑,所有入口(HTTP/consumer)共用。"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.messaging import EventRegistry
from app.modules.example.events import ExampleWidgetRequested
from app.modules.example.models import ExampleWidget
from app.modules.example.repository import ExampleWidgetRepository
from app.shared.exceptions import BusinessError, NotFoundError


class ExampleWidgetNotFoundError(NotFoundError):
    """业务查不到 widget;router 层 / 全局 handler 自动映射 404。"""


class ExampleWidgetFailedError(BusinessError):
    """业务受控失败(fail_until_attempt 触发);交给 consumer wrapper 处理 retry/DLQ。"""


class ExampleWidgetService:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        repository: ExampleWidgetRepository,
        events: EventRegistry | None = None,
    ):
        self._session_factory = session_factory
        self._repository = repository
        self._events = events

    async def create_widget(self, *, payload: dict) -> ExampleWidget:
        if self._events is None:
            raise RuntimeError("ExampleWidgetService created without events; cannot publish")
        async with self._session_factory() as session, session.begin():
            widget = await self._repository.create(session, payload=payload)
            await self._events.example_widget_requested.publish(
                session, ExampleWidgetRequested(widget_id=widget.id)
            )
        return widget

    async def get_widget(self, widget_id: int) -> ExampleWidget | None:
        async with self._session_factory() as session:
            return await self._repository.get(session, widget_id)

    async def process_widget(self, widget_id: int) -> None:
        """consumer 入口。"""
        async with self._session_factory() as session, session.begin():
            widget = await self._repository.get_for_update(session, widget_id)
            if widget is None:
                raise ExampleWidgetNotFoundError(f"widget {widget_id} not found")
            new_attempts = widget.attempts + 1
            fail_until = int(widget.payload.get("fail_until_attempt", 0))
            await self._repository.update(
                session, widget_id, status="running", attempts=new_attempts
            )

        # fail_until_attempt 是示范专用:控制 consumer 前 N 次抛异常,
        # 让 retry/DLQ 分支可测。真实业务不该在 payload 里带失败控制字段。
        if new_attempts <= fail_until:
            error_msg = (
                f"controlled failure: attempts={new_attempts} <= fail_until_attempt={fail_until}"
            )
            async with self._session_factory() as session, session.begin():
                await self._repository.update(
                    session, widget_id, status="failed", last_error=error_msg
                )
            raise ExampleWidgetFailedError(error_msg)

        async with self._session_factory() as session, session.begin():
            await self._repository.update(session, widget_id, status="finished", last_error=None)
