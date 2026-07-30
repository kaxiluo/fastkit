"""RetryPolicy:消费重试策略。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class RetryPolicy:
    """消费失败重试策略。

    Attributes:
        max_attempts: 尝试次数上限(含首次)。默认 3。
        delay: 每次重试延迟(秒)。仅 fixed backoff 使用。默认 30。
        backoff: 退避策略。仅支持 "fixed";"exponential" 需要多级 TTL 延迟队列,尚未实现。
    """

    max_attempts: int = 3
    delay: int = 30
    backoff: Literal["fixed", "exponential"] = "fixed"

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        if self.backoff != "fixed":
            raise NotImplementedError(
                "backoff='exponential' requires multi-tier delay queues; not implemented yet"
            )
