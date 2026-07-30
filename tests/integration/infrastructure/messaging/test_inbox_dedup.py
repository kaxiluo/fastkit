import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.messaging.inbox.middleware import try_claim_message

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _truncate_inbox(db_engine):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE fastkit_inbox"))
    yield


async def test_first_claim_returns_true(session_factory: async_sessionmaker):
    ok = await try_claim_message("on_ping", "msg-1", session_factory)
    assert ok is True

    async with session_factory() as s:
        row = (
            (await s.execute(text("SELECT consumer, message_id FROM fastkit_inbox")))
            .mappings()
            .one()
        )
    assert row == {"consumer": "on_ping", "message_id": "msg-1"}


async def test_second_claim_same_key_returns_false(session_factory: async_sessionmaker):
    assert await try_claim_message("on_ping", "msg-1", session_factory) is True
    assert await try_claim_message("on_ping", "msg-1", session_factory) is False

    async with session_factory() as s:
        count = (await s.execute(text("SELECT COUNT(*) FROM fastkit_inbox"))).scalar_one()
    assert count == 1


async def test_different_consumers_can_claim_same_message_id(
    session_factory: async_sessionmaker,
):
    assert await try_claim_message("handler_a", "msg-1", session_factory) is True
    assert await try_claim_message("handler_b", "msg-1", session_factory) is True

    async with session_factory() as s:
        count = (await s.execute(text("SELECT COUNT(*) FROM fastkit_inbox"))).scalar_one()
    assert count == 2
