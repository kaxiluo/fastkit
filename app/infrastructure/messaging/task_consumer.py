"""@task_consumer:注册 handler spec;实际 broker.subscriber 绑定由 Messaging 完成。"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import wraps
from typing import Any, get_type_hints
from uuid import uuid4

from pydantic import BaseModel, TypeAdapter

from app.infrastructure.messaging.envelope import FailureInfo
from app.infrastructure.messaging.inbox.middleware import try_claim_message
from app.infrastructure.messaging.retry_policy import RetryPolicy
from app.infrastructure.messaging.task_result import TaskResult

log = logging.getLogger(__name__)


class _UnsetType:
    """timeout 未指定的哨兵类型,区别于显式 None(关闭超时)。"""

    _instance: _UnsetType | None = None

    def __new__(cls) -> _UnsetType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "_UNSET"


_UNSET = _UnsetType()


class TaskTimeout(TimeoutError):  # noqa: N818 - 承袭 TimeoutError 命名,与父类一致
    """handler 执行超过配置超时;当作失败走 retry/DLQ,DLQ failure.type 由此可辨识。"""


def _resolve_timeout(override: float | None | _UnsetType, default: float) -> float | None:
    """override 为 _UNSET → 用全局 default;否则用 override(含显式 None=关闭)。"""
    if override is _UNSET:
        return default
    return override


@dataclass
class _ConsumerSpec:
    routing_key: str
    handler: Callable
    wrapped: Callable
    concurrency: int
    inbox_enabled: bool
    retry_policy: RetryPolicy | None
    timeout_override: float | None | _UnsetType = _UNSET
    handler_qualname: str = field(init=False)
    payload_adapter: TypeAdapter | None = field(init=False, default=None)
    accepts_session_factory: bool = field(init=False, default=False)
    accepts_envelope: bool = field(init=False, default=False)

    def __post_init__(self):
        self.handler_qualname = self.handler.__qualname__
        sig = inspect.signature(self.handler)
        params = sig.parameters
        first_param = next(iter(params.values()), None)
        if first_param is not None:
            hints = get_type_hints(self.handler)
            first_type = hints.get(first_param.name)
            if isinstance(first_type, type) and issubclass(first_type, BaseModel):
                self.payload_adapter = TypeAdapter(first_type)
        self.accepts_session_factory = "session_factory" in params
        self.accepts_envelope = "envelope" in params


_PENDING_CONSUMERS: list[_ConsumerSpec] = []


def get_pending_consumers() -> list[_ConsumerSpec]:
    return list(_PENDING_CONSUMERS)


def clear_pending_consumers() -> None:
    _PENDING_CONSUMERS.clear()


def task_consumer(
    routing_key: str,
    *,
    concurrency: int = 1,
    retry: bool | RetryPolicy = False,
    inbox: bool = True,
    timeout: float | None | _UnsetType = _UNSET,
):
    """注册一个 consumer spec。

    Args:
        concurrency: 每副本本地并发上限(>=1),映射为 Channel(prefetch_count);
            全局并发 = 副本数 × concurrency,靠 broker round-robin 均衡;默认 1(串行)。
        retry:
            - False(默认):handler 抛异常 → log.exception + ack + TaskResult.ABORT("handler_exception")
            - True:使用 RetryPolicy() 默认(max_attempts=3, delay=30)
            - RetryPolicy(...):显式策略
        timeout: handler 执行超时秒数。
            - 不传(_UNSET):用全局 MessagingSettings.consumer_timeout_seconds
            - None:关闭该 consumer 的超时
            - float:显式秒数
    """
    if concurrency < 1:
        raise ValueError(f"concurrency must be >= 1, got {concurrency}")

    if retry is True:
        retry_policy: RetryPolicy | None = RetryPolicy()
    elif isinstance(retry, RetryPolicy):
        retry_policy = retry
    elif retry is False:
        retry_policy = None
    else:
        raise TypeError(f"retry must be bool or RetryPolicy, got {type(retry).__name__}")

    def decorator(handler: Callable):
        spec = _ConsumerSpec(
            routing_key=routing_key,
            handler=handler,
            wrapped=None,  # type: ignore[arg-type]
            concurrency=concurrency,
            inbox_enabled=inbox,
            retry_policy=retry_policy,
            timeout_override=timeout,
        )
        # dispatcher 在 engine 绑定阶段注入;这里先建 spec,wrapper 由 engine 层重建
        spec.wrapped = _build_wrapped(spec, inbox_enabled=inbox, dispatcher=None)
        _PENDING_CONSUMERS.append(spec)
        handler.__consumer_spec__ = spec
        return handler

    return decorator


def _build_failure_info(exc: BaseException) -> FailureInfo:
    return FailureInfo(
        type=f"{type(exc).__module__}.{type(exc).__qualname__}",
        message=str(exc)[:500],
        at=datetime.now(UTC).isoformat(),
    )


def _build_wrapped(
    spec: _ConsumerSpec,
    *,
    inbox_enabled: bool,
    dispatcher: Any,  # RetryDispatcher | None;None 时降级为仅 ack
    timeout: float | None = None,
) -> Callable:
    """返回一个协程:接受 (payload, *, envelope, session_factory) → TaskResult。

    异常三分支:
      1. handler 成功 → FINISHED
      2. handler 抛异常 + retry_policy 存在 + attempts < max_attempts → 重投 retry queue
      3. handler 抛异常 + (无 retry_policy or attempts >= max_attempts) → 转 DLQ
    """
    handler = spec.handler
    handler_qualname = spec.handler_qualname
    adapter = spec.payload_adapter
    retry_policy = spec.retry_policy
    original_queue = spec.routing_key

    @wraps(handler)
    async def wrapped(payload, *, envelope: dict, session_factory: Any):
        async def _route_failure(exc: BaseException) -> TaskResult:
            attempts = max(1, int(envelope.get("attempts", 1)))
            # 无 dispatcher(未经过 engine 绑定):仅 ack,不进 retry/DLQ
            if dispatcher is None:
                log.exception(
                    "task.handler_raised",
                    extra={
                        "handler": handler_qualname,
                        "message_id": envelope.get("message_id"),
                        "routing_key": envelope.get("routing_key"),
                    },
                )
                return TaskResult.ABORT("handler_exception")

            # 分支 A:达上限或无 retry 策略 → DLQ 转投
            if retry_policy is None or attempts >= retry_policy.max_attempts:
                failure = _build_failure_info(exc)
                new_envelope = {**envelope, "attempts": attempts, "failure": failure}
                await dispatcher.dead_letter(payload, envelope=new_envelope)
                log.exception(
                    "task.dead_lettered",
                    extra={
                        "handler": handler_qualname,
                        "message_id": envelope.get("message_id"),
                        "attempts": attempts,
                        "max_attempts": retry_policy.max_attempts if retry_policy else 1,
                    },
                )
                return TaskResult.ABORT("dead_lettered")

            # 分支 B:未达上限 → 重投 retry queue,attempts +1
            # retry 用新 message_id,避免 inbox=True 时被 (consumer, message_id)
            # dedup 拦下;original_message_id 沿追溯链保留最初的业务 message_id。
            original_message_id = envelope.get("original_message_id") or envelope.get(
                "message_id", ""
            )
            new_envelope = {
                **envelope,
                "attempts": attempts + 1,
                "message_id": str(uuid4()),
                "original_message_id": original_message_id,
            }
            await dispatcher.republish_delayed(
                payload,
                original_queue=original_queue,
                envelope=new_envelope,
            )
            log.warning(
                "task.retry_scheduled",
                extra={
                    "handler": handler_qualname,
                    "message_id": envelope.get("message_id"),
                    "attempts_after": attempts + 1,
                    "delay_seconds": retry_policy.delay,
                },
            )
            return TaskResult.ABORT("retry_scheduled")

        if adapter is not None and isinstance(payload, dict):
            payload = adapter.validate_python(payload)

        if inbox_enabled:
            message_id = envelope.get("message_id", "")
            if not message_id:
                log.warning(
                    "task.no_message_id",
                    extra={"handler": handler_qualname, "envelope": envelope},
                )
                return TaskResult.ABORT("missing_message_id")
            ok = await try_claim_message(handler_qualname, message_id, session_factory)
            if not ok:
                log.info(
                    "task.aborted",
                    extra={
                        "handler": handler_qualname,
                        "message_id": message_id,
                        "reason": "duplicate_message",
                    },
                )
                return TaskResult.ABORT("duplicate_message")

        handler_kwargs: dict[str, Any] = {}
        if spec.accepts_session_factory:
            handler_kwargs["session_factory"] = session_factory
        if spec.accepts_envelope:
            handler_kwargs["envelope"] = envelope

        try:
            if timeout is None:
                result = await handler(payload, **handler_kwargs)
            else:
                async with asyncio.timeout(timeout) as cm:
                    result = await handler(payload, **handler_kwargs)
        except TimeoutError as exc:
            if timeout is not None and cm.expired():
                log.warning(
                    "task.timeout",
                    extra={
                        "handler": handler_qualname,
                        "message_id": envelope.get("message_id"),
                        "timeout_seconds": timeout,
                    },
                )
                return await _route_failure(TaskTimeout(f"handler exceeded {timeout}s"))
            return await _route_failure(exc)
        except Exception as exc:
            return await _route_failure(exc)

        if result is None:
            return TaskResult.FINISHED()
        if isinstance(result, TaskResult):
            if result.kind == "ABORT":
                log.info(
                    "task.aborted",
                    extra={
                        "handler": handler_qualname,
                        "message_id": envelope.get("message_id"),
                        "reason": result.reason,
                    },
                )
            return result
        raise TypeError(
            f"handler {handler_qualname} returned unexpected type "
            f"{type(result).__name__}; must return None or TaskResult"
        )

    return wrapped
