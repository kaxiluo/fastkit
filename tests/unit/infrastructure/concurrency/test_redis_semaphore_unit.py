"""RedisSemaphore 缺陷修复单测:续租 RedisError 续命 / Lua 0 才 lease_lost / release suppress。"""

from __future__ import annotations

import asyncio

import pytest
from redis.exceptions import RedisError
from structlog.testing import capture_logs

from app.infrastructure.concurrency.redis_semaphore import RedisSemaphore


class _FakeScript:
    """按调用序列返回结果;Exception 元素表示该次调用抛出。序列耗尽后重复最后一个元素。"""

    def __init__(self, results: list):
        self._results = list(results)
        self.calls: list[tuple[list, list]] = []

    async def __call__(self, *, keys, args):
        self.calls.append((keys, args))
        v = self._results[min(len(self.calls) - 1, len(self._results) - 1)]
        if isinstance(v, Exception):
            raise v
        return v


class _FakeRedis:
    def __init__(self, acquire_results=None, renew_results=None, release_results=None):
        self.acquire = _FakeScript(acquire_results or [1])
        self.renew = _FakeScript(renew_results or [1])
        self.release = _FakeScript(release_results or [1])

    def register_script(self, script: str):
        if "SET" in script:  # _ACQUIRE_LUA 含 SET NX EX
            return self.acquire
        if "EXPIRE" in script:  # _RENEW_LUA
            return self.renew
        return self.release  # _RELEASE_LUA 含 DEL


def _sem(fake_redis, *, lease=30, poll=0.01) -> RedisSemaphore:
    return RedisSemaphore(
        fake_redis,  # type: ignore[arg-type]
        key_prefix="test:sem:unit",
        capacity=1,
        lease_seconds=lease,
        poll_interval=poll,
    )


async def test_renew_redis_error_keeps_loop_alive_then_recovers():
    """缺陷 1 修复:续租 RedisError 不杀死 task,下一轮成功续住。"""
    fake = _FakeRedis(renew_results=[RedisError("net glitch"), 1])
    sem = _sem(fake, lease=30)
    sem._renew_interval = 0.01  # 加速测试(生产为 lease/2)
    task = asyncio.create_task(sem._renew_loop("test:sem:unit:1", "tok"))
    await asyncio.sleep(0.05)
    assert not task.done()  # 抖动后仍活着
    assert len(fake.renew.calls) >= 2  # 失败后确实重试了
    task.cancel()


async def test_renew_lua_zero_means_lease_lost_and_stops():
    """缺陷 1 修复:Lua 返回 0(key 过期/被抢占)才是 lease_lost,告警后放弃。"""
    fake = _FakeRedis(renew_results=[0])
    sem = _sem(fake, lease=30)
    sem._renew_interval = 0.01
    with capture_logs() as logs:
        await asyncio.wait_for(sem._renew_loop("test:sem:unit:1", "tok"), timeout=1.0)
    assert any(ev["event"] == "redis_semaphore.lease_lost" for ev in logs)


async def test_hold_suppresses_release_redis_error():
    """缺陷 2 修复:release 失败不顶替业务异常,记日志,配额靠 TTL 回收。"""
    fake = _FakeRedis(release_results=[RedisError("conn reset")])
    sem = _sem(fake, lease=30)
    with capture_logs() as logs:
        async with sem.hold(1, "tok"):
            pass  # 正常业务路径
    assert any(ev["event"] == "redis_semaphore.release_failed" for ev in logs)


async def test_hold_does_not_mask_business_exception():
    """业务异常穿透 hold,不被 release 失败顶替(失败路由不被改变)。"""
    fake = _FakeRedis(release_results=[RedisError("conn reset")])
    sem = _sem(fake, lease=30)

    async def business():
        async with sem.hold(1, "tok"):
            raise ValueError("business failure")

    with pytest.raises(ValueError, match="business failure"):
        await business()


async def test_slot_timeout_branch_uses_try_acquire():
    """slot() 组合实现后,quota 满 + timeout 仍抛 TimeoutError(原语义回归)。"""
    fake = _FakeRedis(acquire_results=[-1])  # 一直满
    sem = _sem(fake, poll=0.01)
    with pytest.raises(TimeoutError):
        async with sem.slot(timeout=0.05):
            pass


async def test_probe_runs_acquire_renew_release():
    fake = _FakeRedis()
    sem = _sem(fake, lease=30)
    await sem.probe()
    assert len(fake.acquire.calls) == 1
    assert len(fake.renew.calls) == 1
    assert len(fake.release.calls) == 1


async def test_probe_propagates_redis_error():
    fake = _FakeRedis(acquire_results=[RedisError("EVAL denied by ACL")])
    sem = _sem(fake, lease=30)
    with pytest.raises(RedisError):
        await sem.probe()


def test_acquire_script_passes_token_as_set_value():
    """互斥守卫:_ACQUIRE_LUA 必须把 token 作为 SET 的 value 并带 NX/EX。

    漏掉 token 时 'NX' 会被当成 value(SET 恒成功、互斥失效),且 GET 恒不等于
    token → 续租/释放守卫同时失效。FakeRedis 按脚本文本区分脚本,抓不住这类
    抄录错误,故静态断言(真语义由 Task 9 真实 Redis 集成兜底)。
    """
    from app.infrastructure.concurrency.redis_semaphore import _ACQUIRE_LUA

    assert "redis.call('SET', prefix .. ':' .. i, token, 'NX', 'EX', lease)" in _ACQUIRE_LUA
