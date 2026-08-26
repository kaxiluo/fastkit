"""engine 接线:redis 必需、每 spec 构建 semaphore、classic 队列声明、nack 透传、
重复 routing_key 注册拒绝。"""

from __future__ import annotations

import pytest

from app.infrastructure.messaging.engine import Messaging
from app.infrastructure.messaging.settings import MessagingSettings
from app.infrastructure.messaging.task_consumer import (
    clear_pending_consumers,
    task_consumer,
)

_BROKER = "amqp://guest:guest@localhost/"


@pytest.fixture(autouse=True)
def _clear():
    clear_pending_consumers()
    yield
    clear_pending_consumers()


class _FakeRobustQueue:
    async def bind(self, exchange, routing_key=""):
        pass


class _CaptureBroker:
    def __init__(self):
        self.queues = []

    def subscriber(self, queue, channel=None):
        self.queues.append(queue)

        def _register(fn):
            return fn

        return _register

    # 完整 start_consumers 路径需要:connect / 拓扑声明 / stop
    async def connect(self):
        pass

    async def start(self):
        pass

    async def stop(self):
        pass

    async def declare_exchange(self, exchange):
        return _FakeRobustQueue()

    async def declare_queue(self, queue):
        return _FakeRobustQueue()


class _FakeScript:
    async def __call__(self, *, keys, args):
        return 1


class _FakeRedis:
    def register_script(self, script: str):
        return _FakeScript()


def _make_messaging(settings: MessagingSettings, redis=None) -> Messaging:
    m = Messaging(broker=_CaptureBroker(), session_factory=object(), settings=settings)  # type: ignore[arg-type]
    m._redis = redis if redis is not None else _FakeRedis()  # type: ignore[assignment]
    m._dispatcher = object()  # type: ignore[assignment]
    return m


def test_bind_declares_classic_queue_type():
    @task_consumer("t.wire.classic", inbox=False)
    async def h(payload: dict) -> None:
        return None

    m = _make_messaging(MessagingSettings(broker_url=_BROKER, app_name="fastkit"))
    m._bind_pending_consumers()  # type: ignore[attr-defined]
    queue = m._broker.queues[-1]  # type: ignore[attr-defined]
    assert queue.arguments["x-queue-type"] == "classic"
    assert queue.arguments["x-dead-letter-exchange"] == "fastkit.dlx"


def test_duplicate_routing_key_registration_rejected():
    """同 routing_key 双注册:同队列双绑定会分流消息(语义未定义),且并发闸与
    容量声明按 routing_key 唯一 keyed(不同 concurrency 时后者静默覆盖前者,
    slot 池行为混乱)——绑定期直接拒绝,不区分 concurrency 是否相同。"""

    @task_consumer("t.wire.dup", inbox=False, concurrency=2)
    async def h1(payload: dict) -> None:
        return None

    @task_consumer("t.wire.dup", inbox=False, concurrency=5)
    async def h2(payload: dict) -> None:
        return None

    m = _make_messaging(MessagingSettings(broker_url=_BROKER, app_name="fastkit"))
    with pytest.raises(ValueError, match="t.wire.dup"):
        m._bind_pending_consumers()  # type: ignore[attr-defined]


async def test_start_consumers_without_redis_raises():
    @task_consumer("t.wire.noredis", inbox=False)
    async def h(payload: dict) -> None:
        return None

    m = _make_messaging(MessagingSettings(broker_url=_BROKER, app_name="fastkit"))
    # 不传 redis:start_consumers 内部 self._redis = None → ValueError(手工预置会被覆盖,别预置)
    with pytest.raises(ValueError, match="redis"):
        await m.start_consumers(start_broker=False)  # type: ignore[arg-type]


async def test_start_consumers_probes_semaphore_and_declares_capacity(monkeypatch):
    import app.infrastructure.messaging.engine as engine_module

    async def _fake_relay_loop(*args, **kwargs):
        return None

    # 真 relay_loop 会拿 object() session_factory 打 DB,崩了还会让 stop() 的
    # wait_for re-raise —— 完整 start_consumers 路径必须替换掉
    monkeypatch.setattr(engine_module, "relay_loop", _fake_relay_loop)

    @task_consumer("t.wire.probe", inbox=False, concurrency=3)
    async def h(payload: dict) -> None:
        return None

    m = _make_messaging(MessagingSettings(broker_url=_BROKER, app_name="fastkit"))

    # probe 与 announcer 走真实现 + 假 redis:FakeRedis.register_script 足够 probe
    # 通过;announcer 需要 set/get/scan_iter/delete,扩展 FakeRedis:
    class _AnnounceFakeRedis(_FakeRedis):
        async def set(self, key, value, ex=None):
            pass

        async def get(self, key):
            return None

        async def delete(self, *keys):
            pass

        def scan_iter(self, match=None, count=None):
            async def _gen():
                return
                yield  # pragma: no cover

            return _gen()

    # redis 经参数传入(手工设 m._redis 会被 start_consumers(redis=None) 覆盖)
    await m.start_consumers(redis=_AnnounceFakeRedis(), start_broker=False)  # type: ignore[arg-type]
    try:
        assert m._semaphores["t.wire.probe"]._capacity == 3  # type: ignore[attr-defined]
        assert m._announcer is not None  # type: ignore[attr-defined]
    finally:
        await m.stop()
