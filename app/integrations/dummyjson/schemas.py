"""DummyJSON API 响应模型。容忍读:外部 API 加字段不爆。"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class DummyJsonProduct(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    title: str
    price: float
    description: str
    category: str
