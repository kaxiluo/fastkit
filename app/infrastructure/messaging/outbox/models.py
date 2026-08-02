"""Outbox ORM:relay 从这里扫未发布行 → publish → 更新 published_at。"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Identity, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.database.base import Base


class Outbox(Base):
    __tablename__ = "fastkit_outbox"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    aggregate: Mapped[str] = mapped_column(Text, nullable=False)
    routing_key: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    headers: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="pending")
    dead_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
