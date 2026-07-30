"""example 域 consumer:@task_consumer 处理 example.widget.requested。"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.messaging import RetryPolicy, task_consumer
from app.modules.example.events import ExampleWidgetRequested
from app.modules.example.repository import ExampleWidgetRepository
from app.modules.example.service import ExampleWidgetService


@task_consumer(
    "example.widget.requested",
    concurrency=1,
    retry=RetryPolicy(max_attempts=3, delay=30, backoff="fixed"),
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
