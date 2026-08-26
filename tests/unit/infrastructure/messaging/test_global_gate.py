"""全局闸编排:quota 满豁免重投 / RedisError 耐心冻结(预算暂停、上限换投)/
slot 先于 inbox claim / 释放不吞业务异常。"""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest
from redis.exceptions import RedisError

from app.infrastructure.messaging.task_consumer import (
    _build_wrapped,
    clear_pending_consumers,
    get_pending_consumers,
    task_consumer,
)

# messaging/__init__.py 将 task_consumer 函数 re-export 为子模块属性,
# `import ... as tc` 会绑定到函数而非模块;须经 importlib 拿真模块才能
# monkeypatch 模块级常量(FREEZE_*_SECONDS)。
tc = importlib.import_module("app.infrastructure.messaging.task_consumer")


@pytest.fixture(autouse=True)
def _clear():
    clear_pending_consumers()
    yield
    clear_pending_consumers()


@dataclass
class _FakeDispatcher:
    republished: list[dict] = field(default_factory=list)
    dead_lettered: list[dict] = field(default_factory=list)

    async def republish_delayed(self, payload: Any, *, original_queue: str, envelope: dict):
        self.republished.append({"queue": original_queue, "envelope": envelope})

    async def dead_letter(self, payload: Any, *, envelope: dict):
        self.dead_lettered.append(envelope)


class _FakeSemaphore:
    """按脚本序列回放:try_acquire 依次弹出元素;-1=满、int=槽位、Exception=抛出。
    序列只剩最后一个元素后永远重复它。"""

    def __init__(self, script: list, poll_interval: float = 0.001):
        self._script = list(script)
        self.poll_interval = poll_interval
        self.released: list[int] = []

    async def try_acquire(self, token: str) -> int:
        v = self._script.pop(0) if len(self._script) > 1 else self._script[0]
        if isinstance(v, Exception):
            raise v
        return v

    @asynccontextmanager
    async def hold(self, slot_i: int, token: str) -> AsyncGenerator[int]:
        try:
            yield slot_i
        finally:
            self.released.append(slot_i)


def _spec(inbox: bool = False):
    @task_consumer("t.gate.q", retry=False, inbox=inbox)
    async def handler(payload: dict) -> None:
        return None

    return get_pending_consumers()[-1]


async def _noop_nack(**kwargs) -> None:
    pass


async def _run(
    spec,
    semaphore,
    handler_body=None,
    *,
    envelope=None,
    nack=_noop_nack,
    wait_timeout=10.0,
    dispatcher=None,
):
    """nack 默认给 noop(闸的必需依赖);显式传 nack=None 的用例专测编程错误路径。"""
    if handler_body is not None:
        spec.handler = handler_body
        spec.handler_qualname = handler_body.__qualname__
    wrapped = _build_wrapped(
        spec,
        inbox_enabled=spec.inbox_enabled,
        dispatcher=dispatcher if dispatcher is not None else _FakeDispatcher(),
        wait_timeout=wait_timeout,
        semaphore=semaphore,
    )
    return await wrapped(
        {"a": 1},
        envelope=envelope or {"message_id": "m-1", "routing_key": "t.gate.q", "attempts": 1},
        session_factory=None,
        nack=nack,
    )


async def test_quota_full_until_budget_raises_deferral():
    """quota 持续满 → 预算耗尽 → ConcurrencyWaitTimeout → 豁免重投不烧 attempts。"""
    sem = _FakeSemaphore([-1], poll_interval=0.001)
    dispatcher = _FakeDispatcher()
    spec = _spec()  # retry=False 也要生效:豁免不依赖 RetryPolicy
    result = await _run(
        spec,
        sem,
        wait_timeout=0.05,
        dispatcher=dispatcher,
        envelope={"message_id": "m-1", "routing_key": "t.gate.q", "attempts": 3},
    )
    assert result.kind == "ABORT"
    assert result.reason == "concurrency_wait_deferred"
    assert len(dispatcher.republished) == 1
    env = dispatcher.republished[0]["envelope"]
    assert env["attempts"] == 3  # 不烧 attempts
    assert env["message_id"] != "m-1"
    assert env["original_message_id"] == "m-1"
    assert dispatcher.dead_lettered == []


async def test_acquire_success_runs_handler_inside_slot():
    calls = []

    async def body(payload):
        calls.append("run")

    sem = _FakeSemaphore([1])
    spec = _spec()
    result = await _run(spec, sem, handler_body=body)
    assert result.kind == "FINISHED"
    assert calls == ["run"]
    assert sem.released == [1]  # 正常路径释放


async def test_redis_error_freezes_then_recovers_and_budget_paused():
    """冻结期 wait_timeout 暂停:冻结耗时(探测间隔 0.04s)超过等待预算(0.03s),
    若预算在冻结期被消耗,恢复后首次 quota 满即会超时;实际应正常获槽完成。"""
    # 序列:1 次 RedisError(冻结 0.04s)→ 1 次 -1(quota 满,验证预算未烧)→ 获槽
    sem = _FakeSemaphore([RedisError("down"), -1, 1])
    sem.poll_interval = 0.001
    orig = tc.FREEZE_PROBE_INTERVAL_SECONDS
    tc.FREEZE_PROBE_INTERVAL_SECONDS = 0.04
    try:
        spec = _spec()
        result = await _run(spec, sem, wait_timeout=0.03)
        # 若冻结期烧了预算,0.03s 预算 < 0.04s 探测,恢复后 -1 即会 ABORT
        assert result.kind == "FINISHED"
    finally:
        tc.FREEZE_PROBE_INTERVAL_SECONDS = orig


async def test_freeze_deadline_counts_consecutive_failures_only():
    """冻结上限计的是"连续 Redis 故障"时长:先等配额超过假上限时长,随后一次
    RedisError + 恢复 → 不得因排队历史被立即 nack 换投(修复:deadline 从首次
    RedisError 起算,任何正常应答(含 -1)重置)。"""
    # 序列:满 5 次(排队 5×0.02s=0.1s > 假上限 0.05s)→ 1 次 RedisError → 获槽
    sem = _FakeSemaphore([-1] * 5 + [RedisError("down"), 1], poll_interval=0.02)
    orig_probe, orig_cap = tc.FREEZE_PROBE_INTERVAL_SECONDS, tc.FREEZE_MAX_SECONDS
    tc.FREEZE_PROBE_INTERVAL_SECONDS = 0.02
    tc.FREEZE_MAX_SECONDS = 0.05  # 若从进入闸时起算,RedisError 到达时早已"超限"
    try:
        spec = _spec()
        result = await _run(spec, sem, wait_timeout=10.0)
        assert result.kind == "FINISHED"  # 不是 ABORT("freeze_requeued")
    finally:
        tc.FREEZE_PROBE_INTERVAL_SECONDS = orig_probe
        tc.FREEZE_MAX_SECONDS = orig_cap


async def test_freeze_until_cap_nacks_requeue_and_aborts():
    """冻结满上限:nack(requeue=True) 换投,返回 freeze_requeued,不进重试/DLQ。"""
    nacks: list[dict] = []

    async def nack(**kwargs):
        nacks.append(kwargs)

    sem = _FakeSemaphore([RedisError("down")])
    orig_probe, orig_cap = tc.FREEZE_PROBE_INTERVAL_SECONDS, tc.FREEZE_MAX_SECONDS
    tc.FREEZE_PROBE_INTERVAL_SECONDS = 0.02
    tc.FREEZE_MAX_SECONDS = 0.06
    try:
        dispatcher = _FakeDispatcher()
        spec = _spec()
        result = await _run(spec, sem, dispatcher=dispatcher, nack=nack)
        assert result.kind == "ABORT"
        assert result.reason == "freeze_requeued"
        assert nacks == [{"requeue": True}]
        assert dispatcher.republished == []  # 不经 _route_failure
        assert dispatcher.dead_lettered == []
    finally:
        tc.FREEZE_PROBE_INTERVAL_SECONDS = orig_probe
        tc.FREEZE_MAX_SECONDS = orig_cap


async def test_missing_nack_with_semaphore_raises_programming_error():
    """闸启用时 nack 缺失 = 编程错误,进入闸即抛:否则冻结上限路径会 auto-ack
    静默丢消息、还伪称 freeze_requeued(fail-fast,不允许占位)。"""
    sem = _FakeSemaphore([1])
    spec = _spec()
    with pytest.raises(RuntimeError, match="nack"):
        await _run(spec, sem, nack=None)


async def test_slot_released_on_handler_failure():
    """handler 业务失败:先释放 slot,再走失败路由(dead_letter 可见)。"""

    async def body(payload):
        raise ValueError("boom")

    sem = _FakeSemaphore([1])
    dispatcher = _FakeDispatcher()
    spec = _spec()
    result = await _run(spec, sem, handler_body=body, dispatcher=dispatcher)
    assert result.reason == "dead_lettered"
    assert sem.released == [1]


async def test_deferral_happens_before_inbox_claim():
    """slot 等待在 inbox claim 之前:豁免轮次不写 inbox(session_factory 未被触碰)。"""
    sem = _FakeSemaphore([-1], poll_interval=0.001)

    class _Boom:
        def __call__(self):
            raise AssertionError("豁免轮次不应触碰 session_factory")

    spec = _spec(inbox=True)
    # 不给 message_id 会让 inbox 分支先 ABORT——必须带 message_id 且断言 session 未被用
    wrapped = _build_wrapped(
        spec,
        inbox_enabled=True,
        dispatcher=_FakeDispatcher(),
        wait_timeout=0.03,
        semaphore=sem,
    )
    result = await wrapped(
        {"a": 1},
        envelope={"message_id": "m-inbox", "routing_key": "t.gate.q", "attempts": 1},
        session_factory=_Boom(),
        nack=_noop_nack,
    )
    assert result.reason == "concurrency_wait_deferred"


async def test_wait_timeout_none_waits_indefinitely_when_full():
    """显式 None = 无限等:0.1s 观察窗内既不返回也不豁免。"""
    sem = _FakeSemaphore([-1], poll_interval=0.005)
    spec = _spec()
    wrapped = _build_wrapped(
        spec,
        inbox_enabled=False,
        dispatcher=_FakeDispatcher(),
        wait_timeout=None,
        semaphore=sem,
    )
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(
            wrapped(
                {"a": 1},
                envelope={"message_id": "m-inf", "routing_key": "t.gate.q", "attempts": 1},
                session_factory=None,
                nack=_noop_nack,
            ),
            timeout=0.1,
        )


async def test_semaphore_none_passes_through():
    """semaphore=None(装饰期占位):无闸直通,行为与现状一致。"""
    spec = _spec()
    wrapped = _build_wrapped(spec, inbox_enabled=False, dispatcher=_FakeDispatcher())
    result = await wrapped(
        {"a": 1},
        envelope={"message_id": "m-plain", "routing_key": "t.gate.q", "attempts": 1},
        session_factory=None,
    )
    assert result.kind == "FINISHED"
