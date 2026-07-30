"""Messaging 引擎对外 API。业务代码只 import 这里的名字。"""

from app.infrastructure.messaging.engine import Messaging
from app.infrastructure.messaging.event import (
    EventConflictError,
    EventPublisher,
    EventRegistry,
    OutOfTransactionError,
    event,
)
from app.infrastructure.messaging.retry_policy import RetryPolicy
from app.infrastructure.messaging.task_consumer import task_consumer
from app.infrastructure.messaging.task_result import TaskResult

__all__ = [
    "EventConflictError",
    "EventPublisher",
    "EventRegistry",
    "Messaging",
    "OutOfTransactionError",
    "RetryPolicy",
    "TaskResult",
    "event",
    "task_consumer",
]
