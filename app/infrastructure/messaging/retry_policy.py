"""RetryPolicy:消费重试策略。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class RetryPolicy:
    """消费失败重试策略。

    重试延迟不由本类控制:它由 ``MESSAGING_RETRY_TTL_MS`` 全局设置决定,
    通过 retry queue 的 ``x-message-ttl`` 实现(见 ``retry/topology.py``)。
    每条重投消息还会施加 ``[0.9, 1.0] * ttl`` 的 per-message jitter,见
    ``RetryDispatcher.republish_delayed``。

    Attributes:
        max_attempts: 尝试次数上限(含首次)。默认 3。
        backoff: 退避策略。仅支持 "fixed";"exponential" 需要多级 TTL 延迟队列,
            尚未实现(会在 ``__post_init__`` 抛 NotImplementedError)。
        overload_exceptions: 环境性过载异常类型(如上游限流 429)。命中时重投
            不递增 attempts——失败源于环境而非消息本身,不应消耗重试预算;
            次数记在 envelope.overload_retries,达 ``overload_retry_limit``
            后回落常规 attempts 语义(防上游配额永久降级导致无限轮询)。默认空。
        overload_retry_limit: 过载豁免的重投次数上限。默认 100。
    """

    max_attempts: int = 3
    backoff: Literal["fixed", "exponential"] = "fixed"
    overload_exceptions: tuple[type[Exception], ...] = ()
    overload_retry_limit: int = 100

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.backoff != "fixed":
            raise NotImplementedError(
                "backoff='exponential' requires multi-tier delay queues; not implemented yet"
            )
        if self.overload_retry_limit < 1:
            raise ValueError(f"overload_retry_limit must be >= 1, got {self.overload_retry_limit}")
