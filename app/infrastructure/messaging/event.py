"""事件契约:@event 装饰 → EventMeta;EventRegistry 提供属性式访问。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

_EVENT_REGISTRY: dict[str, EventMeta] = {}


class EventConflictError(Exception):
    """routing_key 已被注册。"""


class OutOfTransactionError(Exception):
    """publish 时 session 未开事务。"""


@dataclass(frozen=True)
class EventMeta:
    routing_key: str
    aggregate: str
    schema: type[BaseModel]
    schema_version: int


class _PublisherProtocol(Protocol):
    async def publish(
        self,
        session: Any,
        meta: EventMeta,
        payload: BaseModel,
        *,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> None: ...


def event(routing_key: str) -> Callable[[type[T]], type[T]]:
    """装饰 Pydantic 类,登记 EventMeta 到全局 registry。"""

    def decorator(cls: type[T]) -> type[T]:
        if routing_key in _EVENT_REGISTRY:
            raise EventConflictError(
                f"routing_key {routing_key!r} already registered by "
                f"{_EVENT_REGISTRY[routing_key].schema.__name__}"
            )
        aggregate = routing_key.split(".", 1)[0]
        version = cls.model_fields["message_version"].default
        meta = EventMeta(
            routing_key=routing_key,
            aggregate=aggregate,
            schema=cls,
            schema_version=version,
        )
        _EVENT_REGISTRY[routing_key] = meta
        cls.__event_meta__ = meta
        return cls

    return decorator


def get_registered_events() -> dict[str, EventMeta]:
    return dict(_EVENT_REGISTRY)


class EventPublisher:
    def __init__(self, meta: EventMeta, backend: _PublisherProtocol):
        self.meta = meta
        self._backend = backend

    async def publish(
        self,
        session: Any,
        payload: BaseModel,
        *,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> None:
        await self._backend.publish(
            session,
            self.meta,
            payload,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )


class EventRegistry:
    def __init__(self, backend: _PublisherProtocol):
        self._backend = backend
        self._events: dict[str, EventPublisher] = {}

    def register(self, meta: EventMeta) -> None:
        attr = meta.routing_key.replace(".", "_")
        self._events[attr] = EventPublisher(meta, self._backend)

    def __getattr__(self, name: str) -> EventPublisher:
        events = self.__dict__.get("_events") or {}
        if name in events:
            return events[name]
        raise AttributeError(
            f"no event named {name!r} registered (attribute names use "
            f"underscores; routing_key 'a.b' → attr 'a_b')"
        )
