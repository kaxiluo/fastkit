"""RedisSemaphore 单元测试:mock Redis script,验证 acquire/release/timeout/renew。

集成测试(tests/integration/...)用真 redis 验证端到端;这里隔离 Lua 脚本,
覆盖 slot() 的成功/超时/重试分支,以及 _renew_loop 的租约丢失分支。
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.infrastructure.concurrency.redis_semaphore import RedisSemaphore

_SEM_MOD = "app.infrastructure.concurrency.redis_semaphore"


def _build_sem(
    *,
    acquire_return: int = -1,
    renew_return: int = 1,
    release_return: int = 1,
    capacity: int = 2,
    lease: int = 5,
    poll: float = 0.01,
) -> tuple[RedisSemaphore, AsyncMock, AsyncMock, AsyncMock]:
    """构造一个 RedisSemaphore,挂上三个可控的 mock 脚本。"""
    fake_redis = MagicMock()
    acquire = AsyncMock(return_value=acquire_return)
    renew = AsyncMock(return_value=renew_return)
    release = AsyncMock(return_value=release_return)
    fake_redis.register_script.side_effect = [acquire, renew, release]
    sem = RedisSemaphore(
        fake_redis,
        key_prefix="test:sem",
        capacity=capacity,
        lease_seconds=lease,
        poll_interval=poll,
    )
    return sem, acquire, renew, release


def test_capacity_below_one_rejected() -> None:
    fake_redis = MagicMock()
    fake_redis.register_script.side_effect = [AsyncMock()] * 3
    with pytest.raises(ValueError, match="capacity"):
        RedisSemaphore(fake_redis, key_prefix="x", capacity=0, lease_seconds=1, poll_interval=0.01)


async def testtry_acquire_returns_slot_index_and_forwards_args() -> None:
    sem, acquire, _, _ = _build_sem(acquire_return=2)
    assert await sem.try_acquire("token-x") == 2
    acquire.assert_awaited_once_with(keys=["test:sem"], args=[2, "token-x", 5])


async def test_slot_yields_index_then_releases(monkeypatch) -> None:
    # _renew_loop 后台 task 内部会 sleep,patch 掉避免真睡
    monkeypatch.setattr(f"{_SEM_MOD}.asyncio.sleep", AsyncMock())
    sem, _, _, release = _build_sem(acquire_return=1)

    async with sem.slot() as slot_i:
        assert slot_i == 1

    # 退出 with 时 release 必须以正确 key 调用一次;token 是内部 uuid4,不固定
    release.assert_awaited_once()
    assert release.await_args.kwargs["keys"] == ["test:sem:1"]


async def test_slot_no_timeout_loops_until_acquired(monkeypatch) -> None:
    """无 timeout:前两次返回 -1,第三次成功 → yield。"""
    monkeypatch.setattr(f"{_SEM_MOD}.asyncio.sleep", AsyncMock())

    calls = {"n": 0}

    async def _acquire_side_effect(*_args, **_kwargs):
        calls["n"] += 1
        return 1 if calls["n"] >= 3 else -1

    fake_redis = MagicMock()
    acquire = AsyncMock(side_effect=_acquire_side_effect)
    renew = AsyncMock(return_value=1)
    release = AsyncMock(return_value=1)
    fake_redis.register_script.side_effect = [acquire, renew, release]
    sem = RedisSemaphore(
        fake_redis,
        key_prefix="test:sem",
        capacity=2,
        lease_seconds=5,
        poll_interval=0.01,
    )

    async with sem.slot() as slot_i:
        assert slot_i == 1
    assert calls["n"] == 3  # 初次 + 两次重试


async def test_slot_timeout_raises_when_deadline_passed(monkeypatch) -> None:
    """timeout 到期:抛 TimeoutError,不进入 with 体。

    用 patch 后的 time.monotonic 让第二次调用必然超期,避免依赖 wall clock。
    """
    n = {"i": 0}

    def _fake_monotonic() -> float:
        n["i"] += 1
        return 0.0 if n["i"] == 1 else 100.0  # 第一次算 deadline=0.05,第二次检查立即超时

    monkeypatch.setattr(f"{_SEM_MOD}.time.monotonic", _fake_monotonic)
    monkeypatch.setattr(f"{_SEM_MOD}.asyncio.sleep", AsyncMock())

    sem, _, _, _ = _build_sem(acquire_return=-1)

    with pytest.raises(TimeoutError, match="semaphore slot timeout"):
        async with sem.slot(timeout=0.05):
            pytest.fail("slot 不应在超时后进入 with 体")


async def test_renew_loop_exits_when_lease_lost(monkeypatch) -> None:
    """renew 返回 0(租约被抢)→ 循环退出,不再续租。"""
    monkeypatch.setattr(f"{_SEM_MOD}.asyncio.sleep", AsyncMock())

    sem, _, renew, _ = _build_sem(renew_return=0)
    await sem._renew_loop("test:sem:1", "token-x")

    renew.assert_awaited_once_with(keys=["test:sem:1"], args=["token-x", 5])


async def test_renew_loop_continues_when_renew_succeeds(monkeypatch) -> None:
    """renew 返回非 0 → 循环,直到被外部 cancel 终止。"""
    # 模块里的 `asyncio.sleep` 通过 `redis_semaphore.asyncio.sleep` 拿到的就是 asyncio 模块,
    # patch 它等于改全局。我们换成"yield 一次"的快速 sleep,让 cancel 仍能在 await 点注入。
    real_sleep = asyncio.sleep

    async def _yield_once(_delay: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(f"{_SEM_MOD}.asyncio.sleep", _yield_once)

    sem, _, renew, _ = _build_sem(renew_return=1)
    task = asyncio.create_task(sem._renew_loop("test:sem:1", "token-x"))
    for _ in range(3):
        await real_sleep(0)  # 让 task 跑几轮,renew 至少被 await 一次

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    assert renew.await_count >= 1
