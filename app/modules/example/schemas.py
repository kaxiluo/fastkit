"""example 域 HTTP schemas:请求与响应契约。"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class CreateWidgetRequest(BaseModel):
    payload: dict = Field(default_factory=dict)


class CreateSlowTaskRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=1000)


class SlowTaskResponse(BaseModel):
    task_ids: list[str]


class WidgetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: str
    attempts: int
    last_error: str | None
    payload: dict
