"""容量存活声明:per-worker 键 + TTL 重申 + 不一致 warning + 退出清理。"""

from __future__ import annotations

import asyncio
import fnmatch

from structlog.testing import capture_logs

import app.infrastructure.concurrency.capacity_announcer as capacity_announcer_module
from app.infrastructure.concurrency.capacity_announcer import CapacityAnnouncer


def _match(key: str, pattern: str) -> bool:
    return fnmatch.fnmatch(key, pattern)


class _FakeScan:
    def __init__(self, data: dict[str, str], pattern: str):
        self._items = [k for k in data if _match(k, pattern)]

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


class _FakeRedis:
    def __init__(self):
        self.data: dict[str, str] = {}

    async def set(self, key, value, ex=None):
        self.data[key] = str(value)

    async def get(self, key):
        return self.data.get(key)

    async def delete(self, *keys):
        for k in keys:
            self.data.pop(k, None)

    def scan_iter(self, match=None, count=None):
        return _FakeScan(self.data, match)


async def test_declare_writes_per_worker_keys_with_ttl():
    fake = _FakeRedis()
    announcer = CapacityAnnouncer(
        fake,  # type: ignore[arg-type]
        capacities={"fastkit.concurrency.example.slowtask.requested": 10},
    )
    await announcer.declare_and_check()
    keys = list(fake.data)
    assert len(keys) == 1
    assert keys[0].startswith("fastkit.concurrency.example.slowtask.requested.capacity.")
    assert fake.data[keys[0]] == "10"


async def test_mismatched_live_capacity_logs_warning_but_not_raise():
    fake = _FakeRedis()
    prefix = "fastkit.concurrency.t.q"
    fake.data[f"{prefix}.capacity.other-worker"] = "5"  # 存活旧副本声明了 5
    announcer = CapacityAnnouncer(fake, capacities={prefix: 2})  # type: ignore[arg-type]
    with capture_logs() as logs:
        await announcer.declare_and_check()
    mismatches = [ev for ev in logs if ev["event"] == "concurrency.capacity_mismatch"]
    assert len(mismatches) == 1
    assert mismatches[0]["local_capacity"] == 2
    assert mismatches[0]["other_capacities"] == ["5"]


async def test_matching_capacity_logs_nothing():
    fake = _FakeRedis()
    prefix = "fastkit.concurrency.t.q"
    fake.data[f"{prefix}.capacity.other-worker"] = "2"
    announcer = CapacityAnnouncer(fake, capacities={prefix: 2})  # type: ignore[arg-type]
    with capture_logs() as logs:
        await announcer.declare_and_check()
    assert not [ev for ev in logs if ev["event"] == "concurrency.capacity_mismatch"]


async def test_stop_deletes_own_keys_only():
    fake = _FakeRedis()
    prefix = "fastkit.concurrency.t.q"
    foreign = f"{prefix}.capacity.other-worker"
    fake.data[foreign] = "2"  # 预置他副本键:stop 只删自己的,他键必须保留
    announcer = CapacityAnnouncer(fake, capacities={prefix: 2})  # type: ignore[arg-type]
    await announcer.declare_and_check()
    announcer.start()
    await asyncio.sleep(0.01)
    await announcer.stop()
    assert fake.data == {foreign: "2"}  # 只删自己的


async def test_redeclare_loop_keeps_key_alive(monkeypatch):
    # 重申间隔 30s 的真实节奏对单测太慢:把模块常量缩小,验证"删掉后循环写回"
    monkeypatch.setattr(capacity_announcer_module, "REDECLARE_INTERVAL_SECONDS", 0.05)

    fake = _FakeRedis()
    prefix = "fastkit.concurrency.t.q"
    foreign = f"{prefix}.capacity.other-worker"
    fake.data[foreign] = "2"
    announcer = CapacityAnnouncer(fake, capacities={prefix: 2})  # type: ignore[arg-type]
    await announcer.declare_and_check()
    announcer.start()
    await asyncio.sleep(capacity_announcer_module.REDECLARE_INTERVAL_SECONDS + 0.1)
    key = next(k for k in fake.data if k != foreign)
    # 重申即重新 SET(fake 不实现 TTL 过期,验证重申发生:删掉后循环应写回)
    fake.data.pop(key)
    await asyncio.sleep(capacity_announcer_module.REDECLARE_INTERVAL_SECONDS + 0.1)
    assert key in fake.data
    await announcer.stop()
