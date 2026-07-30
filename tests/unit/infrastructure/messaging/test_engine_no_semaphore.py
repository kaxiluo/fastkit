"""重构守护:Messaging / _make_entry 不再依赖 RedisSemaphore 与 redis。"""

from __future__ import annotations

import inspect

from app.infrastructure.messaging.engine import Messaging, _make_entry


def test_messaging_init_has_no_redis_param():
    params = inspect.signature(Messaging.__init__).parameters
    assert "redis" not in params


def test_make_entry_has_no_semaphore_param():
    params = inspect.signature(_make_entry).parameters
    assert "semaphore" not in params
