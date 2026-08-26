"""全局并发闸集成:真 Redis 全局上限 / 清空超发收敛 / 满载探针隔离 /
容量声明 / 冻结 unacked 停滞与恢复 / 冻结上限 nack 换投 / classic 队列类型。"""

from __future__ import annotations

import asyncio
import importlib
from urllib.parse import quote, urlparse

import httpx
import pytest
from redis.exceptions import RedisError
from structlog.testing import capture_logs

from app.infrastructure.concurrency.redis_semaphore import RedisSemaphore
from app.infrastructure.messaging.engine import Messaging
from app.infrastructure.messaging.task_consumer import (
    clear_pending_consumers,
    task_consumer,
)

# messaging/__init__.py 将 task_consumer 函数 re-export 为子模块属性,
# `import ... as tc` 会绑定到函数而非模块;须经 importlib 拿真模块才能
# monkeypatch 模块级常量(FREEZE_*_SECONDS)。
tc = importlib.import_module("app.infrastructure.messaging.task_consumer")

pytestmark = pytest.mark.integration


async def test_gate_caps_global_inflight_at_capacity(redis_client):
    """两个"副本"(两个 semaphore 实例共享 key)并发 6 路,全局峰值 ≤ 2。"""
    prefix = "itest:gate:cap2"
    sems = [
        RedisSemaphore(
            redis_client,
            key_prefix=prefix,
            capacity=2,
            lease_seconds=30,
            poll_interval=0.02,
        )
        for _ in range(2)
    ]
    state = {"inflight": 0, "peak": 0, "done": 0}

    async def worker(sem, n):
        async with sem.slot():
            state["inflight"] += 1
            state["peak"] = max(state["peak"], state["inflight"])
            await asyncio.sleep(0.1)
            state["inflight"] -= 1
            state["done"] += 1

    await asyncio.gather(*(worker(sems[n % 2], n) for n in range(6)))
    assert state["done"] == 6
    assert state["peak"] <= 2
    # 结束后槽位全部释放
    keys = [k async for k in redis_client.scan_iter(match=f"{prefix}:*")]
    assert keys == []


async def test_flush_causes_bounded_overissue_then_token_guard_converges(redis_client):
    """一次性清空语义:DEL 掉 slot key 模拟 Redis 无持久化重启 → 新副本可抢到
    (瞬时超发),旧 token 的 release 变 no-op(归属守卫),新持有期间他人拿不到。"""
    prefix = "itest:gate:flush"
    sem = RedisSemaphore(
        redis_client,
        key_prefix=prefix,
        capacity=1,
        lease_seconds=30,
        poll_interval=0.02,
    )
    token_a = "aaa"
    assert await sem.try_acquire(token_a) == 1
    await redis_client.delete(f"{prefix}:1")  # 模拟清空
    token_b = "bbb"
    assert await sem.try_acquire(token_b) == 1  # 超发窗口:B 也持有
    # 旧持有者的 release 是 no-op(不能误删 B 的 key)
    await sem._release_script(keys=[f"{prefix}:1"], args=[token_a])
    assert await sem.try_acquire("ccc") == -1  # B 仍持有
    await sem._release_script(keys=[f"{prefix}:1"], args=[token_b])


async def test_engine_starts_when_business_slots_exhausted(
    broker, session_factory, test_messaging_settings, redis_client
):
    """满载时新副本启动:探针前缀隔离,业务 semaphore 满载不误判为探活失败。"""
    clear_pending_consumers()

    @task_consumer("itest.gate.full", inbox=False, concurrency=1)
    async def handler(payload: dict) -> None:
        pass

    prefix = f"{test_messaging_settings.app_name}.concurrency.itest.gate.full"
    hold_sem = RedisSemaphore(
        redis_client,
        key_prefix=prefix,
        capacity=1,
        lease_seconds=30,
        poll_interval=0.02,
    )
    token = "blocker"
    assert await hold_sem.try_acquire(token) == 1  # 业务池占满
    msg = Messaging(
        broker=broker, session_factory=session_factory, settings=test_messaging_settings
    )
    try:
        await msg.start_consumers(redis=redis_client, start_broker=False)
        assert msg._semaphores["itest.gate.full"]._capacity == 1
    finally:
        await msg.stop()
        await hold_sem._release_script(keys=[f"{prefix}:1"], args=[token])
        clear_pending_consumers()


async def test_capacity_declaration_ttl_and_mismatch_warning(
    broker, session_factory, test_messaging_settings, redis_client
):
    clear_pending_consumers()

    @task_consumer("itest.gate.capdecl", inbox=False, concurrency=4)
    async def handler(payload: dict) -> None:
        pass

    prefix = f"{test_messaging_settings.app_name}.concurrency.itest.gate.capdecl"
    # 伪造一个存活旧副本声明了不同容量
    await redis_client.set(f"{prefix}.capacity.stale-worker", "9", ex=60)
    msg = Messaging(
        broker=broker, session_factory=session_factory, settings=test_messaging_settings
    )
    with capture_logs() as logs:
        await msg.start_consumers(redis=redis_client, start_broker=False)
    try:
        mismatches = [ev for ev in logs if ev["event"] == "concurrency.capacity_mismatch"]
        assert len(mismatches) == 1
        assert mismatches[0]["local_capacity"] == 4
        assert mismatches[0]["other_capacities"] == ["9"]
        # 自己的容量键已写入且带 TTL(scan_iter 产出 bytes key,统一解码后再过滤)
        keys = [
            k.decode() if isinstance(k, bytes) else k
            async for k in redis_client.scan_iter(match=f"{prefix}.capacity.*")
        ]
        own = [k for k in keys if "stale-worker" not in k]
        assert len(own) == 1
        assert await redis_client.get(own[0]) in (b"4", "4")
        assert 0 < await redis_client.ttl(own[0]) <= 90
    finally:
        await msg.stop()
        await redis_client.delete(f"{prefix}.capacity.stale-worker")
        clear_pending_consumers()


class _FlakyScript:
    """前 fail_n 次抛 RedisError,之后委托 inner——模拟 Redis 中断后恢复。"""

    def __init__(self, inner, fail_n: int):
        self._inner = inner
        self._n = fail_n

    async def __call__(self, *, keys, args):
        if self._n > 0:
            self._n -= 1
            raise RedisError("simulated outage")
        return await self._inner(keys=keys, args=args)


def _unacked(messaging_settings, queue: str) -> int:
    u = urlparse(messaging_settings.broker_url.get_secret_value())
    vhost = quote(u.path.lstrip("/") or "/", safe="")
    with httpx.Client(auth=(u.username or "", u.password or ""), timeout=5.0) as c:
        r = c.get(f"http://{u.hostname}:15672/api/queues/{vhost}/{quote(queue, safe='')}")
        r.raise_for_status()
        return r.json().get("messages_unacknowledged", 0)


async def _unacked_settled(
    messaging_settings, queue: str, expected: int, *, timeout: float = 15.0
) -> int:
    """mgmt 队列统计有秒级可见延迟,轮询等 unacked 达到预期值后返回。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last = -1
    while True:
        last = _unacked(messaging_settings, queue)
        if last == expected:
            return last
        if loop.time() >= deadline:
            return last
        await asyncio.sleep(0.5)


async def test_freeze_holds_unacked_then_resumes(
    broker, session_factory, test_messaging_settings, redis_client, monkeypatch
):
    """Redis 中断:消息冻结不丢(unacked 停滞、不进 retry 队列、wait_timeout 不计时),
    恢复后 ≤ 探测间隔续跑完成。"""
    clear_pending_consumers()
    monkeypatch.setattr(tc, "FREEZE_PROBE_INTERVAL_SECONDS", 0.3)

    done = asyncio.Event()

    @task_consumer("itest.gate.freeze", inbox=False, concurrency=1)
    async def handler(payload: dict) -> None:
        done.set()

    msg = Messaging(
        broker=broker, session_factory=session_factory, settings=test_messaging_settings
    )
    await msg.start_consumers(redis=redis_client)
    try:
        sem = msg._semaphores["itest.gate.freeze"]
        real = sem._acquire_script
        # 冻结 ~9s(30 次 × 0.3s),须 > mgmt 统计延迟(约 5s),轮询才能观察到 unacked=1
        sem._acquire_script = _FlakyScript(real, fail_n=30)
        await broker.publish(
            {"n": 1},
            routing_key="itest.gate.freeze",
            headers={"message_id": "fz-1", "routing_key": "itest.gate.freeze"},
        )
        # 冻结中:handler 未执行,消息 unacked 停滞(轮询等统计刷新)
        assert (await _unacked_settled(test_messaging_settings, "itest.gate.freeze", 1)) == 1
        assert not done.is_set()
        # 恢复(Flaky 耗尽失败次数后自动委托真脚本)→ 续跑完成
        await asyncio.wait_for(done.wait(), timeout=15.0)
        assert (await _unacked_settled(test_messaging_settings, "itest.gate.freeze", 0)) == 0
        sem._acquire_script = real
    finally:
        await msg.stop()
        clear_pending_consumers()
        monkeypatch.undo()


async def test_freeze_cap_requeues_without_ack_or_retry(
    broker, session_factory, test_messaging_settings, redis_client, monkeypatch
):
    """冻结满上限:nack(requeue) 换投——重新投递后执行成功,恰好一次业务效果,
    不 ack、不进 retry 队列、不经 _route_failure(inbox 行数 = 1 佐证同 message_id)。"""
    from sqlalchemy import text

    clear_pending_consumers()
    monkeypatch.setattr(tc, "FREEZE_PROBE_INTERVAL_SECONDS", 0.1)
    monkeypatch.setattr(tc, "FREEZE_MAX_SECONDS", 0.4)

    deliveries = {"n": 0}

    @task_consumer("itest.gate.freeze2", inbox=True, concurrency=1)
    async def handler(payload: dict) -> None:
        deliveries["n"] += 1

    msg = Messaging(
        broker=broker, session_factory=session_factory, settings=test_messaging_settings
    )
    await msg.start_consumers(redis=redis_client)
    try:
        sem = msg._semaphores["itest.gate.freeze2"]
        # 前若干次 acquire 失败:覆盖首轮冻结(0.4s 上限,0.1s 探测 → 第 4 次探测时
        # 达上限 nack 换投);重投后恢复,消息正常获槽执行
        sem._acquire_script = _FlakyScript(sem._acquire_script, fail_n=5)
        await broker.publish(
            {"n": 1},
            routing_key="itest.gate.freeze2",
            headers={"message_id": "fz-2", "routing_key": "itest.gate.freeze2"},
        )
        for _ in range(60):
            if deliveries["n"] >= 1:
                break
            await asyncio.sleep(0.2)
        assert deliveries["n"] == 1  # 恰好一次业务效果
        await asyncio.sleep(1.0)
        # 同 message_id 重投(inbox 只有一行);retry 队列为空
        async with session_factory() as s:
            cnt = (
                await s.execute(text("SELECT COUNT(*) FROM fastkit_inbox WHERE message_id='fz-2'"))
            ).scalar_one()
        assert cnt == 1
        retry_q = test_messaging_settings.retry_queue
        u = urlparse(test_messaging_settings.broker_url.get_secret_value())
        vhost = quote(u.path.lstrip("/") or "/", safe="")
        with httpx.Client(auth=(u.username or "", u.password or ""), timeout=5.0) as c:
            r = c.get(f"http://{u.hostname}:15672/api/queues/{vhost}/{quote(retry_q, safe='')}")
            r.raise_for_status()
            assert r.json().get("messages", 0) == 0
    finally:
        await msg.stop()
        clear_pending_consumers()
        monkeypatch.undo()


async def test_queues_declared_classic(
    broker, session_factory, test_messaging_settings, redis_client
):
    """业务 / retry / DLQ 三处队列 effective_queue_type=classic(mgmt API 实证)。"""
    clear_pending_consumers()

    @task_consumer("itest.gate.classic", inbox=False)
    async def handler(payload: dict) -> None:
        pass

    msg = Messaging(
        broker=broker, session_factory=session_factory, settings=test_messaging_settings
    )
    await msg.start_consumers(redis=redis_client)
    try:
        u = urlparse(test_messaging_settings.broker_url.get_secret_value())
        vhost = quote(u.path.lstrip("/") or "/", safe="")
        with httpx.Client(auth=(u.username or "", u.password or ""), timeout=5.0) as c:
            for q in (
                "itest.gate.classic",
                test_messaging_settings.retry_queue,
                test_messaging_settings.dlq_queue,
            ):
                r = c.get(f"http://{u.hostname}:15672/api/queues/{vhost}/{quote(q, safe='')}")
                r.raise_for_status()
                assert r.json()["type"] == "classic", q
    finally:
        await msg.stop()
        clear_pending_consumers()
