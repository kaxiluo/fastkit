"""RateLimiter 单元测试:mock MovingWindowRateLimiter,验证 allow/acquire/timeout 行为。

集成测试(tests/integration/...)用真 redis + MovingWindow 验证端到端;这里隔离
strategy,覆盖 acquire 的立即成功/重试/超时分支,以及 reset_time/max_wait/poll 计算路径。
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

from limits import RateLimitItemPerSecond
from limits.aio.strategies import MovingWindowRateLimiter

from app.infrastructure.ratelimit.limiter import RateLimiter

_LIM_MOD = "app.infrastructure.ratelimit.limiter"


def _limiter(strategy: MovingWindowRateLimiter, *, max_wait: float = 5.0) -> RateLimiter:
    return RateLimiter(
        strategy,
        namespace="test-ns",
        default_poll=0.1,
        max_wait=max_wait,
    )


def _strategy(hit_values, *, reset_time: float = 1000.0) -> AsyncMock:
    """hit 按序返回 hit_values;get_window_stats 返回带 reset_time 的对象。"""
    strategy = AsyncMock(spec=MovingWindowRateLimiter)
    strategy.hit = AsyncMock(side_effect=list(hit_values))
    strategy.get_window_stats = AsyncMock(return_value=SimpleNamespace(reset_time=reset_time))
    return strategy


def _patch_time(monkeypatch, *, monotonic_seq=(), time_value: float = 500.0) -> None:
    """patch limiter 模块内的 time.monotonic / time.time。

    monotonic_seq:按序返回;用完后继续返回最后一个值。
    time_value:wall clock 固定返回值,用于 reset_time 计算等待。
    """
    seq = list(monotonic_seq)
    idx = {"i": 0}

    def _monotonic() -> float:
        if not seq:
            idx["i"] += 1
            return float(idx["i"])
        i = idx["i"]
        idx["i"] += 1
        return seq[min(i, len(seq) - 1)]

    monkeypatch.setattr(f"{_LIM_MOD}.time.monotonic", _monotonic)
    monkeypatch.setattr(f"{_LIM_MOD}.time.time", lambda: time_value)


async def test_allow_passes_through_hit_result_true() -> None:
    strategy = AsyncMock(spec=MovingWindowRateLimiter)
    strategy.hit = AsyncMock(return_value=True)
    rl = _limiter(strategy)
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.allow(item, "k1") is True
    strategy.hit.assert_awaited_once_with(item, "test-ns", "k1")


async def test_allow_passes_through_hit_result_false() -> None:
    strategy = AsyncMock(spec=MovingWindowRateLimiter)
    strategy.hit = AsyncMock(return_value=False)
    rl = _limiter(strategy)
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.allow(item, "k1") is False


async def test_acquire_succeeds_on_first_hit() -> None:
    strategy = _strategy([True])
    rl = _limiter(strategy)
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.acquire(item, "k1") is True
    strategy.hit.assert_awaited_once()
    strategy.get_window_stats.assert_not_awaited()  # 命中即返,不查 stats


async def test_acquire_retries_until_success(monkeypatch) -> None:
    monkeypatch.setattr(f"{_LIM_MOD}.asyncio.sleep", AsyncMock())
    _patch_time(monkeypatch, time_value=1500.0)  # reset_time=1000 < now → wait 走 poll 分支
    strategy = _strategy([False, False, True])
    rl = _limiter(strategy)
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.acquire(item, "k1") is True
    assert strategy.hit.await_count == 3
    assert strategy.get_window_stats.await_count == 2  # 两次失败各查一次 stats


async def test_acquire_timeout_returns_false(monkeypatch) -> None:
    """deadline 到期 → 返回 False。用 patch monotonic 让第二次检查必然超期。"""
    monkeypatch.setattr(f"{_LIM_MOD}.asyncio.sleep", AsyncMock())
    _patch_time(
        monkeypatch,
        monotonic_seq=[0.0, 100.0],  # deadline=0+0.05=0.05,二次检查 100 已超
        time_value=500.0,
    )
    # hit 持续 False,确保不会因 hit=True 提前结束
    strategy = AsyncMock(spec=MovingWindowRateLimiter)
    strategy.hit = AsyncMock(return_value=False)
    strategy.get_window_stats = AsyncMock(return_value=SimpleNamespace(reset_time=1000.0))
    rl = _limiter(strategy)
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.acquire(item, "k1", timeout=0.05) is False


async def test_acquire_sleeps_reset_time_delta_when_in_future(monkeypatch) -> None:
    """reset_time - now > 0 → sleep 该差值。"""
    slept: list[float] = []

    async def _record(d: float) -> None:
        slept.append(d)

    monkeypatch.setattr(f"{_LIM_MOD}.asyncio.sleep", _record)
    _patch_time(monkeypatch, time_value=500.0)  # reset_time=502 → wait=2
    strategy = _strategy([False, True], reset_time=502.0)
    rl = _limiter(strategy)
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.acquire(item, "k1") is True
    assert slept == [2.0]


async def test_acquire_caps_wait_at_max_wait(monkeypatch) -> None:
    """reset_time - now > max_wait → sleep 被 max_wait 截断。"""
    slept: list[float] = []

    async def _record(d: float) -> None:
        slept.append(d)

    monkeypatch.setattr(f"{_LIM_MOD}.asyncio.sleep", _record)
    _patch_time(monkeypatch, time_value=500.0)  # reset_time=510 → wait=min(10, max_wait=5)
    strategy = _strategy([False, True], reset_time=510.0)
    rl = _limiter(strategy, max_wait=5.0)
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.acquire(item, "k1") is True
    assert slept == [5.0]


async def test_acquire_falls_back_to_default_poll_when_wait_zero(monkeypatch) -> None:
    slept: list[float] = []

    async def _record(d: float) -> None:
        slept.append(d)

    monkeypatch.setattr(f"{_LIM_MOD}.asyncio.sleep", _record)
    _patch_time(monkeypatch, time_value=1500.0)  # reset_time=1000 < now → wait <= 0
    strategy = _strategy([False, True], reset_time=1000.0)
    rl = _limiter(strategy)  # default_poll=0.1
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.acquire(item, "k1") is True
    assert slept == [0.1]


async def test_acquire_falls_back_to_custom_poll(monkeypatch) -> None:
    slept: list[float] = []

    async def _record(d: float) -> None:
        slept.append(d)

    monkeypatch.setattr(f"{_LIM_MOD}.asyncio.sleep", _record)
    _patch_time(monkeypatch, time_value=1500.0)
    strategy = _strategy([False, True], reset_time=1000.0)
    rl = _limiter(strategy)
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.acquire(item, "k1", poll=0.3) is True
    assert slept == [0.3]


async def test_acquire_within_timeout_caps_wait_to_remaining(monkeypatch) -> None:
    """带 timeout 时,wait 不能超过 deadline - monotonic(剩余时间)。"""
    slept: list[float] = []

    async def _record(d: float) -> None:
        slept.append(d)

    monkeypatch.setattr(f"{_LIM_MOD}.asyncio.sleep", _record)
    # monotonic: 0(算 deadline=0+1=1)→ 0(检查未超)→ ...
    # time.time=500,reset_time=502 → wait=2,但 deadline-monotonic=1-0=1,取 min=1
    _patch_time(
        monkeypatch,
        monotonic_seq=[0.0, 0.0, 0.0],
        time_value=500.0,
    )
    strategy = _strategy([False, True], reset_time=502.0)
    rl = _limiter(strategy)
    item = RateLimitItemPerSecond(1, 1)

    assert await rl.acquire(item, "k1", timeout=1.0) is True
    assert slept == [1.0]
