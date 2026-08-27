"""supervised_relay_loop:异常退出自动重启、shutdown 可打断退避、正常关机不重启。"""

from __future__ import annotations

import asyncio

import app.infrastructure.messaging.outbox.relay as relay_module
from app.infrastructure.messaging.outbox.relay import supervised_relay_loop
from app.infrastructure.messaging.settings import MessagingSettings

_BROKER = "amqp://guest:guest@localhost/"


def _settings(poll_interval: float) -> MessagingSettings:
    return MessagingSettings(
        broker_url=_BROKER,
        app_name="fastkit",
        outbox_poll_interval_seconds=poll_interval,
    )


async def _wait_until(predicate, timeout: float = 2.0) -> None:
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met in time")


async def test_restarts_after_crash_and_recovers(monkeypatch):
    """第一次 relay_loop 抛异常(如表不存在),supervisor 退避后重启,
    第二次正常运行到 shutdown —— 恢复场景闭环。"""
    calls: list[int] = []
    shutdown = asyncio.Event()

    async def _flaky_relay(session_factory, broker, settings, sd):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("relation fastkit_outbox does not exist")
        await sd.wait()

    monkeypatch.setattr(relay_module, "relay_loop", _flaky_relay)

    task = asyncio.create_task(supervised_relay_loop(None, None, _settings(0.05), shutdown))
    try:
        await _wait_until(lambda: len(calls) >= 2)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)

    assert len(calls) == 2


async def test_shutdown_during_backoff_prevents_restart(monkeypatch):
    """退避等待期间收到 shutdown:不再重启,静默退出(关机路径无纠缠)。"""
    calls: list[int] = []
    shutdown = asyncio.Event()

    async def _crashing_relay(session_factory, broker, settings, sd):
        calls.append(1)
        raise RuntimeError("db unreachable")

    monkeypatch.setattr(relay_module, "relay_loop", _crashing_relay)

    task = asyncio.create_task(supervised_relay_loop(None, None, _settings(30.0), shutdown))
    try:
        await _wait_until(lambda: len(calls) == 1)
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)
    finally:
        task.cancel()

    assert len(calls) == 1


async def test_clean_exit_without_restart(monkeypatch):
    """relay_loop 正常返回(shutdown 触发):不进入退避、不重复启动。"""
    calls: list[int] = []
    shutdown = asyncio.Event()

    async def _normal_relay(session_factory, broker, settings, sd):
        calls.append(1)
        await sd.wait()

    monkeypatch.setattr(relay_module, "relay_loop", _normal_relay)

    task = asyncio.create_task(supervised_relay_loop(None, None, _settings(0.05), shutdown))
    await _wait_until(lambda: len(calls) == 1)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2.0)

    assert len(calls) == 1
