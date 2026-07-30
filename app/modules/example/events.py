"""example 域事件契约:@event 装饰触发 EventMeta 注册。"""

from __future__ import annotations

from pydantic import BaseModel

from app.infrastructure.messaging import event


@event("example.widget.requested")
class ExampleWidgetRequested(BaseModel):
    message_version: int = 1
    widget_id: int
