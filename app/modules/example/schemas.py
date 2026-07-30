"""example 域 HTTP schemas:请求与响应契约。"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class CreateWidgetRequest(BaseModel):
    payload: dict = Field(default_factory=dict)


class WidgetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: str
    attempts: int
    last_error: str | None
    payload: dict
