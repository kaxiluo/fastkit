"""RedisSemaphore:N 槽位并发原语(真 redis)。"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from app.infrastructure.concurrency.redis_semaphore import RedisSemaphore

pytestmark = pytest.mark.integration


def _sem(redis, capacity=2, lease=5, poll=0.05):
    return RedisSemaphore(
        redis,
        key_prefix=f"test:sem:{uuid4().hex}",
        capacity=capacity,
        lease_seconds=lease,
        poll_interval=poll,
    )


async def test_capacity_full_then_release(redis_client):
    sem = _sem(redis_client, capacity=2)
    async with sem.slot() as a:
        async with sem.slot() as b:
            assert {a, b} == {1, 2}
            assert await sem._try_acquire(uuid4().hex) == -1  # 满
        # b 释放后可再拿
        assert await sem._try_acquire(uuid4().hex) != -1


async def test_third_waiter_unblocks_on_release(redis_client):
    sem = _sem(redis_client, capacity=1, poll=0.02)
    got: list[int] = []

    async def worker(hold: float):
        async with sem.slot() as i:
            got.append(i)
            await asyncio.sleep(hold)

    t1 = asyncio.create_task(worker(0.3))
    await asyncio.sleep(0.05)
    t2 = asyncio.create_task(worker(0.0))
    await asyncio.sleep(0.05)
    assert got == [1]  # 第二个还在等
    await asyncio.gather(t1, t2)
    assert got == [1, 1]  # 释放后第二个拿到同一槽位


async def test_renewal_keeps_slot_alive(redis_client):
    sem = _sem(redis_client, capacity=1, lease=2, poll=0.02)
    async with sem.slot():
        await asyncio.sleep(3)  # > lease,续租应保住
        assert await sem._try_acquire(uuid4().hex) == -1


async def test_crash_expiry_frees_slot(redis_client):
    # 模拟崩溃:直接占槽但不起续租,等 TTL 过期
    sem = _sem(redis_client, capacity=1, lease=1, poll=0.02)
    token = uuid4().hex
    assert await sem._try_acquire(token) == 1
    await asyncio.sleep(1.3)
    assert await sem._try_acquire(uuid4().hex) == 1  # 已释放


async def test_token_guard_release(redis_client):
    sem = _sem(redis_client, capacity=1, lease=30, poll=0.02)
    token_a = uuid4().hex
    assert await sem._try_acquire(token_a) == 1
    key = f"{sem._prefix}:1"
    # 用错误 token release 不应删除
    await sem._release_script(keys=[key], args=["wrong-token"])
    assert await sem._try_acquire(uuid4().hex) == -1  # 仍被 A 持有
    # 正确 token release
    await sem._release_script(keys=[key], args=[token_a])
    assert await sem._try_acquire(uuid4().hex) == 1


async def test_slot_timeout_raises_when_capacity_full(redis_client):
    """容量占满后,带 timeout 的申请应在 deadline 后抛 TimeoutError。

    覆盖 slot() 的等待超时分支(redis_semaphore.py 的 deadline 判断);
    其余用例都走"最终拿到槽位"路径,唯独这条走"等不到就放弃"。
    """
    sem = _sem(redis_client, capacity=1, poll=0.02)
    async with sem.slot():  # 占满唯一槽位
        with pytest.raises(TimeoutError):
            async with sem.slot(timeout=0.3):
                pass  # 不应到达
