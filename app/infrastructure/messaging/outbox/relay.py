"""Outbox relay:LISTEN/NOTIFY + 积压连续消化 + FOR UPDATE SKIP LOCKED + publisher confirm + 退避。"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.infrastructure.messaging.settings import MessagingSettings

log = logging.getLogger(__name__)

OUTBOX_NOTIFY_CHANNEL = "fastkit_outbox_new"


def _backoff(attempts: int, cap: int) -> int:
    return min(2**attempts, cap)


async def _drain_once(
    session_factory: async_sessionmaker,
    broker,
    settings: MessagingSettings,
) -> int:
    """扫一批 outbox 行 → publish → 更新;返回本批抓回行数(用于判断是否抓满以决定续抓)。

    失败分支:
      - new_attempts < max_attempts:attempts+1 + backoff
      - new_attempts >= max_attempts:标 status='dead' + dead_at=NOW()
    """
    fetched = 0
    async with session_factory() as session, session.begin():
        rows = (
            (
                await session.execute(
                    text(
                        """
                        SELECT id, routing_key, payload, headers, attempts
                        FROM fastkit_outbox
                        WHERE status = 'pending'
                          AND next_attempt_at <= NOW()
                          AND attempts < :max_attempts
                        ORDER BY next_attempt_at
                        LIMIT :batch_size
                        FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {
                        "max_attempts": settings.outbox_max_attempts,
                        "batch_size": settings.outbox_batch_size,
                    },
                )
            )
            .mappings()
            .all()
        )

        fetched = len(rows)
        for row in rows:
            try:
                await broker.publish(
                    row["payload"],
                    routing_key=row["routing_key"],
                    headers=row["headers"],
                )
                await session.execute(
                    text("UPDATE fastkit_outbox SET status = 'published', published_at = NOW() WHERE id = :id"),
                    {"id": row["id"]},
                )
            except Exception as e:
                new_attempts = row["attempts"] + 1
                if new_attempts >= settings.outbox_max_attempts:
                    # 达上限:标 dead
                    err_msg = str(e)[:500]
                    reason = f"max_attempts_exceeded: {type(e).__name__}"
                    await session.execute(
                        text(
                            """
                            UPDATE fastkit_outbox
                            SET status = 'dead',
                                attempts = :att,
                                last_error = :err,
                                dead_reason = :reason,
                                dead_at = NOW()
                            WHERE id = :id
                            """
                        ),
                        {
                            "id": row["id"],
                            "att": new_attempts,
                            "err": err_msg,
                            "reason": reason,
                        },
                    )
                    log.error(
                        "outbox.dead",
                        extra={
                            "outbox_id": row["id"],
                            "routing_key": row["routing_key"],
                            "attempts": new_attempts,
                            "error": repr(e),
                        },
                    )
                else:
                    # 未达上限:原有 backoff 逻辑(用已 +1 的 new_attempts,首次失败退避从 2s 起)
                    delay = _backoff(new_attempts, settings.outbox_backoff_max_seconds)
                    await session.execute(
                        text(
                            """
                            UPDATE fastkit_outbox
                            SET attempts = attempts + 1,
                                last_error = :err,
                                next_attempt_at = NOW() + make_interval(secs => :delay)
                            WHERE id = :id
                            """
                        ),
                        {"id": row["id"], "err": str(e)[:500], "delay": delay},
                    )
                    log.warning(
                        "outbox.publish_failed",
                        extra={
                            "outbox_id": row["id"],
                            "routing_key": row["routing_key"],
                            "attempts": new_attempts,
                            "delay": delay,
                            "error": repr(e),
                        },
                    )
    return fetched


async def _drain_until_empty(
    session_factory: async_sessionmaker,
    broker,
    settings: MessagingSettings,
) -> None:
    """反复 _drain_once 直到本轮 due 行清空(某批抓回 < batch_size)。

    失败行被 _drain_once 推到未来的 next_attempt_at,下一批 SELECT 不会再选中,
    因此 broker 全挂时也自然收敛,不会忙等。
    """
    while True:
        n = await _drain_once(session_factory, broker, settings)
        if n < settings.outbox_batch_size:
            break
        await asyncio.sleep(0)  # 让步,避免饿死同进程的 consumer 协程


async def relay_loop(
    session_factory: async_sessionmaker,
    broker,
    settings: MessagingSettings,
    shutdown_event: asyncio.Event,
) -> None:
    """常驻协程:LISTEN fastkit_outbox_new + 兜底轮询。"""
    drain = asyncio.Event()

    # 独立的 listen 用 session,不掺入正常读写路径
    async with session_factory() as listen_session:
        conn = await listen_session.connection()
        raw = await conn.get_raw_connection()
        asyncpg_conn = raw.driver_connection

        def _on_notify(*_args):
            drain.set()

        await asyncpg_conn.add_listener(OUTBOX_NOTIFY_CHANNEL, _on_notify)
        log.info("outbox.relay_started")

        try:
            # 启动时先扫一次,防止 listener 就绪前落表的行滞留
            await _drain_until_empty(session_factory, broker, settings)

            while not shutdown_event.is_set():
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        drain.wait(),
                        timeout=settings.outbox_poll_interval_seconds,
                    )
                drain.clear()
                if shutdown_event.is_set():
                    break
                await _drain_until_empty(session_factory, broker, settings)
        finally:
            await asyncpg_conn.remove_listener(OUTBOX_NOTIFY_CHANNEL, _on_notify)
            log.info("outbox.relay_stopped")
