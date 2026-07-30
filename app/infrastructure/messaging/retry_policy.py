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
    """

    max_attempts: int = 3
    backoff: Literal["fixed", "exponential"] = "fixed"

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.backoff != "fixed":
            raise NotImplementedError(
                "backoff='exponential' requires multi-tier delay queues; not implemented yet"
            )
