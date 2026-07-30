"""Inbox 幂等:ON CONFLICT DO NOTHING;True=新消息,False=重复。"""

from __future__ import annotations

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.messaging.inbox.models import Inbox


async def try_claim_message(
    handler_qualname: str,
    message_id: str,
    session_factory: async_sessionmaker,
) -> bool:
    """尝试写 inbox 行;若 (consumer, message_id) 已存在返回 False。"""
    async with session_factory() as session, session.begin():
        stmt = (
            pg_insert(Inbox)
            .values(consumer=handler_qualname, message_id=message_id)
            .on_conflict_do_nothing(index_elements=["consumer", "message_id"])
        )
        result = await session.execute(stmt)
        return (result.rowcount or 0) == 1
