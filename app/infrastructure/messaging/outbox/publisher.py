"""业务同事务 publish:往当前 session 加一行 outbox。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.messaging.envelope import fill_envelope
from app.infrastructure.messaging.event import EventMeta, OutOfTransactionError
from app.infrastructure.messaging.outbox.models import Outbox


class TransactionalPublisher:
    """业务 code 通过 EventRegistry 间接调用;不直接实例化。"""

    def __init__(self, service_name: str):
        self._service_name = service_name

    async def publish(
        self,
        session: AsyncSession,
        meta: EventMeta,
        payload: BaseModel,
        *,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> None:
        if not session.in_transaction():
            raise OutOfTransactionError(
                "publish() requires an active transaction; wrap the call in "
                "`async with session.begin():` or `async with session_factory() "
                "as s, s.begin(): ...`"
            )
        envelope: dict[str, Any] = fill_envelope(
            routing_key=meta.routing_key,
            schema_version=meta.schema_version,
            producer=self._service_name,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )
        session.add(
            Outbox(
                aggregate=meta.aggregate,
                routing_key=meta.routing_key,
                payload=payload.model_dump(mode="json"),
                headers=dict(envelope),
            )
        )
