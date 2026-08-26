"""@task_consumer:注册 handler spec;实际 broker.subscriber 绑定由 Messaging 完成。"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import wraps
from typing import Any, get_type_hints
from uuid import uuid4

import structlog
from pydantic import BaseModel, TypeAdapter
from redis.exceptions import RedisError

from app.infrastructure.concurrency.redis_semaphore import RedisSemaphore
from app.infrastructure.messaging.envelope import FailureInfo
from app.infrastructure.messaging.inbox.middleware import try_claim_message
from app.infrastructure.messaging.retry_policy import RetryPolicy
from app.infrastructure.messaging.task_result import TaskResult

log = structlog.get_logger(__name__)


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

# 全局并发闸内部常量:不进 Settings、不进 @task_consumer 签名
FREEZE_PROBE_INTERVAL_SECONDS = 5.0
# 冻结上限须 < RabbitMQ consumer_timeout(broker 端 rabbitmq.conf 配置,默认 30min;
# 它不是队列参数,客户端无法经队列声明设置——classic 声明只防 vhost 默认漂移成
# quorum,不保证该值)。否则 broker 会先按 unacked 超时断 channel。
FREEZE_MAX_SECONDS = 20 * 60.0
SLOT_LEASE_SECONDS = 30
SLOT_POLL_INTERVAL_SECONDS = 0.05


class _FreezeExhausted(Exception):  # noqa: N818 - 内部控制流信号,非用户可见异常类型
    """冻结达上限且已完成 nack(requeue) 换投;内部信号,转 ABORT("freeze_requeued")。"""


class TaskTimeout(TimeoutError):  # noqa: N818 - 承袭 TimeoutError 命名,与父类一致
    """handler 执行超过配置超时;当作失败走 retry/DLQ,DLQ failure.type 由此可辨识。"""


class ConcurrencyWaitTimeout(TimeoutError):  # noqa: N818 - 承袭 TimeoutError 命名,与 TaskTimeout 同款
    """等待全局并发配额超时。不走业务失败语义:触发豁免重投回主队列排队,
    不烧 attempts、不进 DLQ。"""


def _resolve_timeout(override: float | None | _UnsetType, default: float) -> float | None:
    """override 为 _UNSET → 用全局 default;否则用 override(含显式 None=关闭)。"""
    if override is _UNSET:
        return default
    return override


def _resolve_wait_timeout(
    wait_override: float | None | _UnsetType,
    timeout_override: float | None | _UnsetType,
    default: float,
) -> float | None:
    """wait_timeout 三级解析:显式值优先;未写 → 跟随 timeout 解析值;
    timeout 解析为 None(执行不限时)→ 回落全局 default(等待必须有限)。

    默认跟随 timeout 的理由:slot 释放周期 ≈ 任务时长上界 = timeout,
    等一个"最长任务时长"内配额必有机会流转,快慢任务自动适配。
    """
    if wait_override is not _UNSET:
        return wait_override
    effective_timeout = _resolve_timeout(timeout_override, default)
    if effective_timeout is None:
        return default
    return effective_timeout


@dataclass
class _ConsumerSpec:
    routing_key: str
    handler: Callable
    wrapped: Callable
    concurrency: int
    inbox_enabled: bool
    retry_policy: RetryPolicy | None
    timeout_override: float | None | _UnsetType = _UNSET
    wait_timeout_override: float | None | _UnsetType = _UNSET
    handler_qualname: str = field(init=False)
    payload_adapter: TypeAdapter | None = field(init=False, default=None)
    accepts_session_factory: bool = field(init=False, default=False)
    accepts_envelope: bool = field(init=False, default=False)
    accepts_integrations: bool = field(init=False, default=False)
    accepts_databases: bool = field(init=False, default=False)
    accepts_redis: bool = field(init=False, default=False)
    accepts_attempts: bool = field(init=False, default=False)
    accepts_max_attempts: bool = field(init=False, default=False)

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
        self.accepts_integrations = "integrations" in params
        self.accepts_databases = "databases" in params
        self.accepts_redis = "redis" in params
        self.accepts_attempts = "attempts" in params
        self.accepts_max_attempts = "max_attempts" in params


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
    wait_timeout: float | None | _UnsetType = _UNSET,
):
    """注册一个 consumer spec。

    Args:
        concurrency: 全集群全局并发上限(>=1),Redis N-slot 信号量实现;
            2 副本、10 副本全局并发都是 N,与部署拓扑解耦。提高吞吐 = 显式
            调大本值;加副本 = 高可用 + 分摊负载,不放大并发。正常 Redis
            条件下的软上限(租约丢失窗口可能短暂超出);
            每副本 AMQP prefetch 仍 = N,单副本可吃满全局配额。默认 1(全局串行)。
        retry:
            - False(默认):handler 抛异常 → log.exception + ack + TaskResult.ABORT("handler_exception")
            - True:使用 RetryPolicy() 默认(max_attempts=3)
            - RetryPolicy(...):显式策略(延迟不由 RetryPolicy 控制,见类 docstring)
        timeout: handler 执行超时秒数。
            - 不传(_UNSET):用全局 MessagingSettings.consumer_timeout_seconds
            - None:关闭该 consumer 的超时
            - float:显式秒数
            超上限合成 TaskTimeout 走业务失败(烧 attempts);配比见开发指南。
        wait_timeout: 全局并发配额等待超时秒数(超时参数族,与并发容量无关)。
            - 不传(_UNSET):跟随 timeout 解析值(timeout=None 时回落全局
              consumer_timeout_seconds,执行可不限时但等待必须有限)
            - None:无限等(逃生舱;警示:slot 泄漏场景消息将卡在本地 unacked)
            - float:显式秒数
            超时触发拥堵豁免重投(retry_ttl_ms 延迟后回主队列排尾,不烧
            attempts、不进 DLQ);框架级 ConcurrencyWaitTimeout 不走
            overload_exceptions 匹配。
    """
    if concurrency < 1:
        raise ValueError(f"concurrency must be >= 1, got {concurrency}")

    if isinstance(wait_timeout, (int, float)) and wait_timeout < 0:
        raise ValueError(f"wait_timeout must be >= 0 or None, got {wait_timeout}")

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
            wait_timeout_override=wait_timeout,
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


@asynccontextmanager
async def _gated_slot(
    semaphore: RedisSemaphore,
    *,
    wait_timeout: float | None,
    nack: Callable[..., Awaitable[None]] | None,
    handler_qualname: str,
    message_id: str | None,
) -> AsyncGenerator[int]:
    """全局闸:quota 满消耗 wait_timeout 预算;RedisError 耐心冻结(预算暂停、
    5s 探测、连续故障满 20min nack 换投)。quota 满(Lua 返回 -1)与 Redis 挂
    (异常)是两种信号,处理路径不同。

    nack 是闸的必需依赖:冻结达上限的唯一出路是 nack(requeue) 换投;缺失时
    消息会被 auto-ack 静默丢弃却伪称已换投,故进入闸即抛编程错误(fail-fast)。
    """
    if nack is None:
        raise RuntimeError(
            "global concurrency gate requires a nack callback "
            "(engine._make_entry must forward msg.nack to the wrapped consumer)"
        )
    token = uuid4().hex
    budget = wait_timeout
    # 冻结上限计"连续 Redis 故障"时长:首次 RedisError 起算,任何正常应答(含
    # quota 满 -1)重置为 None。不能在进入闸时一次定型——长期等配额的消息遇
    # 短暂抖动会被立即 nack 换投。
    freeze_deadline: float | None = None
    while True:
        iter_started = time.monotonic()
        try:
            slot_i = await semaphore.try_acquire(token)
        except RedisError:
            now = time.monotonic()
            if freeze_deadline is None:
                freeze_deadline = now + FREEZE_MAX_SECONDS
                log.error(
                    "task.concurrency_frozen",
                    handler=handler_qualname,
                    message_id=message_id,
                    probe_in_seconds=FREEZE_PROBE_INTERVAL_SECONDS,
                    freeze_cap_seconds=FREEZE_MAX_SECONDS,
                )
            elif now >= freeze_deadline:
                await nack(requeue=True)
                log.warning(
                    "task.freeze_requeued",
                    handler=handler_qualname,
                    message_id=message_id,
                    frozen_seconds=FREEZE_MAX_SECONDS,
                )
                raise _FreezeExhausted from None
            # 冻结:消息保持 unacked 原地探测,不进重试队列、零持久层写入;
            # 等待预算只在 Redis 正常应答期间推进,冻结期暂停(恢复后顺延)
            await asyncio.sleep(FREEZE_PROBE_INTERVAL_SECONDS)
            continue
        freeze_deadline = None  # 正常应答(含 -1):冻结期结束,下次故障重新计时
        if slot_i >= 0:
            break
        # 配额满、Redis 正常应答:消耗等待预算后轮询
        await asyncio.sleep(semaphore.poll_interval)
        if budget is not None:
            budget -= time.monotonic() - iter_started
            if budget <= 0:
                raise ConcurrencyWaitTimeout(
                    f"no free concurrency slot within {wait_timeout}s for {handler_qualname}"
                )
    async with semaphore.hold(slot_i, token):
        yield slot_i


def _build_wrapped(
    spec: _ConsumerSpec,
    *,
    inbox_enabled: bool,
    dispatcher: Any,  # RetryDispatcher | None;None 时降级为仅 ack
    timeout: float | None = None,
    wait_timeout: float | None = None,
    semaphore: RedisSemaphore | None = None,
) -> Callable:
    """返回一个协程:接受 (payload, *, envelope, session_factory, integrations,
    databases, redis, nack) → TaskResult。

    全局并发闸(semaphore 非 None 时):slot 等待在 inbox claim 之前、
    asyncio.timeout 之外。异常分支:

      1. handler 成功 → FINISHED
      2. handler 抛过载异常(overload_exceptions 命中且未达豁免上限)→ 重投
         retry queue,attempts 不变(环境性失败不烧重试预算)
      3. handler 抛异常 + retry_policy 存在 + attempts < max_attempts → 重投 retry queue
      4. handler 抛异常 + (无 retry_policy or attempts >= max_attempts) → 转 DLQ
      闸层分支(先于以上):
      - slot 等待超时(ConcurrencyWaitTimeout)→ 豁免重投回主队列排队,不烧 attempts
      - 冻结满上限(_FreezeExhausted,已完成 nack requeue)→ ABORT("freeze_requeued")
    """
    handler = spec.handler
    handler_qualname = spec.handler_qualname
    adapter = spec.payload_adapter
    retry_policy = spec.retry_policy
    original_queue = spec.routing_key

    @wraps(handler)
    async def wrapped(
        payload,
        *,
        envelope: dict,
        session_factory: Any,
        integrations: Any = None,
        databases: Any = None,
        redis: Any = None,
        nack: Callable[..., Awaitable[None]] | None = None,
    ):
        async def _route_failure(exc: BaseException) -> TaskResult:
            attempts = max(1, int(envelope.get("attempts", 1)))
            # 无 dispatcher(未经过 engine 绑定):仅 ack,不进 retry/DLQ
            if dispatcher is None:
                log.exception(
                    "task.handler_raised",
                    handler=handler_qualname,
                    message_id=envelope.get("message_id"),
                    routing_key=envelope.get("routing_key"),
                )
                return TaskResult.ABORT("handler_exception")

            # 分支 O:环境性过载(如上游限流 429)→ 重投不烧 attempts。
            # 失败源于环境而非消息本身,重试预算(attempts)不应被消耗;
            # overload_retries 独立计数,达豁免上限后回落下面常规分支,
            # 防上游配额永久降级导致同一条消息无限轮询。
            overload_retries = int(envelope.get("overload_retries", 0))
            if (
                retry_policy is not None
                and retry_policy.overload_exceptions
                # 框架合成的 TaskTimeout(handler 执行超时)不是"外部过载",应烧
                # attempts 走业务失败;否则 overload_exceptions=(TimeoutError,)
                # 会误把框架超时当限流豁免,永不消耗重试预算
                and not isinstance(exc, TaskTimeout)
                and isinstance(exc, retry_policy.overload_exceptions)
                and overload_retries < retry_policy.overload_retry_limit
            ):
                original_message_id = envelope.get("original_message_id") or envelope.get(
                    "message_id", ""
                )
                new_envelope = {
                    **envelope,
                    "overload_retries": overload_retries + 1,
                    "message_id": str(uuid4()),
                    "original_message_id": original_message_id,
                }
                await dispatcher.republish_delayed(
                    payload,
                    original_queue=original_queue,
                    envelope=new_envelope,
                )
                log.warning(
                    "task.overload_retry_scheduled",
                    handler=handler_qualname,
                    message_id=envelope.get("message_id"),
                    attempts=attempts,
                    overload_retries_after=overload_retries + 1,
                )
                return TaskResult.ABORT("overload_retry_scheduled")

            # 分支 A:达上限或无 retry 策略 → DLQ 转投
            if retry_policy is None or attempts >= retry_policy.max_attempts:
                failure = _build_failure_info(exc)
                new_envelope = {**envelope, "attempts": attempts, "failure": failure}
                await dispatcher.dead_letter(payload, envelope=new_envelope)
                log.exception(
                    "task.dead_lettered",
                    handler=handler_qualname,
                    message_id=envelope.get("message_id"),
                    attempts=attempts,
                    max_attempts=retry_policy.max_attempts if retry_policy else 1,
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
                handler=handler_qualname,
                message_id=envelope.get("message_id"),
                attempts_after=attempts + 1,
            )
            return TaskResult.ABORT("retry_scheduled")

        async def _consume() -> TaskResult:
            if inbox_enabled:
                message_id = envelope.get("message_id", "")
                if not message_id:
                    log.warning(
                        "task.no_message_id",
                        handler=handler_qualname,
                        envelope=envelope,
                    )
                    return TaskResult.ABORT("missing_message_id")
                ok = await try_claim_message(handler_qualname, message_id, session_factory)
                if not ok:
                    log.info(
                        "task.aborted",
                        handler=handler_qualname,
                        message_id=message_id,
                        reason="duplicate_message",
                    )
                    return TaskResult.ABORT("duplicate_message")

            handler_kwargs: dict[str, Any] = {}
            if spec.accepts_session_factory:
                handler_kwargs["session_factory"] = session_factory
            if spec.accepts_envelope:
                handler_kwargs["envelope"] = envelope
            if spec.accepts_integrations:
                handler_kwargs["integrations"] = integrations
            if spec.accepts_databases:
                handler_kwargs["databases"] = databases
            if spec.accepts_redis:
                handler_kwargs["redis"] = redis
            if spec.accepts_attempts:
                handler_kwargs["attempts"] = max(1, int(envelope.get("attempts", 1)))
            if spec.accepts_max_attempts:
                handler_kwargs["max_attempts"] = retry_policy.max_attempts if retry_policy else 1

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
                        handler=handler_qualname,
                        message_id=envelope.get("message_id"),
                        timeout_seconds=timeout,
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
                        handler=handler_qualname,
                        message_id=envelope.get("message_id"),
                        reason=result.reason,
                    )
                return result
            raise TypeError(
                f"handler {handler_qualname} returned unexpected type "
                f"{type(result).__name__}; must return None or TaskResult"
            )

        if adapter is not None and isinstance(payload, dict):
            payload = adapter.validate_python(payload)

        if semaphore is None:
            return await _consume()

        try:
            async with _gated_slot(
                semaphore,
                wait_timeout=wait_timeout,
                nack=nack,
                handler_qualname=handler_qualname,
                message_id=envelope.get("message_id"),
            ):
                return await _consume()
        except ConcurrencyWaitTimeout:
            # 拥堵=排队:豁免重投回主队列排尾,复用 republish_delayed 原样
            # (retry_ttl + jitter);不烧 attempts、无预算、不进 DLQ
            if dispatcher is None:
                log.warning(
                    "task.concurrency_wait_deferred_no_dispatcher",
                    handler=handler_qualname,
                    message_id=envelope.get("message_id"),
                )
                return TaskResult.ABORT("concurrency_wait_deferred")
            original_message_id = envelope.get("original_message_id") or envelope.get(
                "message_id", ""
            )
            new_envelope = {
                **envelope,
                "message_id": str(uuid4()),
                "original_message_id": original_message_id,
            }
            await dispatcher.republish_delayed(
                payload,
                original_queue=original_queue,
                envelope=new_envelope,
            )
            log.info(
                "task.concurrency_wait_deferred",
                handler=handler_qualname,
                message_id=envelope.get("message_id"),
                wait_timeout_seconds=wait_timeout,
            )
            return TaskResult.ABORT("concurrency_wait_deferred")
        except _FreezeExhausted:
            return TaskResult.ABORT("freeze_requeued")

    return wrapped
