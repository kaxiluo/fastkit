"""per-replica prefetch 摊薄:ceil(concurrency/replica_count)+1,每副本多留 1 条缓冲。"""

from __future__ import annotations

import pytest

from app.infrastructure.messaging.engine import _resolve_prefetch
from app.infrastructure.messaging.settings import MessagingSettings


@pytest.mark.parametrize(
    "concurrency, replica_count, expected",
    [
        (10, 1, 11),  # 单副本:prefetch = concurrency + 1
        (10, 4, 4),  # ceil(10/4)=3,不整除向上取整后 +1
        (10, 5, 3),  # ceil(10/5)=2,整除后 +1
        (4, 10, 2),  # N < R:ceil(4/10)=1 再 +1(每副本至少 1 条缓冲)
        (1, 1, 2),
        (3, 2, 3),  # ceil(3/2)=2 再 +1
    ],
)
def test_resolve_prefetch(concurrency, replica_count, expected):
    assert _resolve_prefetch(concurrency, replica_count) == expected


def test_replica_count_defaults_to_one():
    s = MessagingSettings(broker_url="amqp://guest:guest@localhost/", app_name="fastkit")
    assert s.replica_count == 1


def test_replica_count_reads_worker_replicas(monkeypatch):
    monkeypatch.setenv("WORKER_REPLICAS", "4")
    s = MessagingSettings(broker_url="amqp://guest:guest@localhost/", app_name="fastkit")
    assert s.replica_count == 4
