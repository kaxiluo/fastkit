import pytest
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.messaging.event import EventMeta, OutOfTransactionError
from app.infrastructure.messaging.outbox.publisher import TransactionalPublisher

pytestmark = pytest.mark.integration


class DemoPayload(BaseModel):
    message_version: int = 1
    id: str


META = EventMeta(
    routing_key="test.publisher_evt",
    aggregate="test",
    schema=DemoPayload,
    schema_version=1,
)


@pytest.fixture(autouse=True)
async def _truncate_outbox(db_engine):
    async with db_engine.begin() as conn:
        await conn.execute(text("TRUNCATE fastkit_outbox"))
    yield


async def test_publish_writes_row_when_transaction_commits(
    session_factory: async_sessionmaker,
):
    pub = TransactionalPublisher(service_name="fastkit-test")
    async with session_factory() as session, session.begin():
        await pub.publish(session, META, DemoPayload(id="a"))

    async with session_factory() as session:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT aggregate, routing_key, payload, headers, "
                        "published_at, attempts FROM fastkit_outbox"
                    )
                )
            )
            .mappings()
            .all()
        )
    assert len(rows) == 1
    r = rows[0]
    assert r["aggregate"] == "test"
    assert r["routing_key"] == "test.publisher_evt"
    assert r["payload"] == {"message_version": 1, "id": "a"}
    assert r["headers"]["routing_key"] == "test.publisher_evt"
    assert r["headers"]["producer"] == "fastkit-test"
    assert r["headers"]["message_version"] == 1
    assert r["published_at"] is None
    assert r["attempts"] == 0


async def test_publish_rollback_removes_row(session_factory):
    pub = TransactionalPublisher(service_name="fastkit-test")
    with pytest.raises(RuntimeError, match="boom"):
        async with session_factory() as session, session.begin():
            await pub.publish(session, META, DemoPayload(id="b"))
            raise RuntimeError("boom")

    async with session_factory() as session:
        count = (await session.execute(text("SELECT COUNT(*) FROM fastkit_outbox"))).scalar_one()
    assert count == 0


async def test_publish_without_active_transaction_raises(session_factory):
    pub = TransactionalPublisher(service_name="fastkit-test")
    async with session_factory() as session:  # 不进入 .begin()
        with pytest.raises(OutOfTransactionError):
            await pub.publish(session, META, DemoPayload(id="c"))
