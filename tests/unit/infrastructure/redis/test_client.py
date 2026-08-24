"""build_redis 固定 RESP2:redis-py 默认握手发 HELLO(RESP3 协商),
服务端 <6.0 不认识该命令直接报错,固定 protocol=2 兼容全版本服务端。"""

from __future__ import annotations

from pydantic import SecretStr

from app.infrastructure.redis.client import build_redis
from app.infrastructure.redis.settings import RedisSettings


def test_build_redis_pins_resp2() -> None:
    settings = RedisSettings(url=SecretStr("redis://localhost:6379/0"))
    client = build_redis(settings)
    assert client.connection_pool.connection_kwargs["protocol"] == 2
