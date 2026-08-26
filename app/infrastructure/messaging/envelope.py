"""消息 Envelope:publisher 补全、consumer 解析,中间件层透明使用。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TypedDict
from uuid import uuid4


class FailureInfo(TypedDict):
    """消费失败摘要,DLQ 消息 headers.failure 字段。完整堆栈不入,统一进日志。"""

    type: str  # 异常类型全名,如 "httpx.ConnectError"
    message: str  # str(exc) 截断到 500 字节
    at: str  # ISO8601 UTC


class Envelope(TypedDict, total=False):
    message_id: str
    message_version: int
    correlation_id: str | None
    causation_id: str | None
    producer: str
    published_at: str
    routing_key: str
    attempts: int
    overload_retries: int
    original_message_id: str
    failure: FailureInfo | None


def fill_envelope(
    routing_key: str,
    schema_version: int,
    producer: str,
    *,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    attempts: int = 1,
    failure: FailureInfo | None = None,
) -> Envelope:
    return Envelope(
        message_id=str(uuid4()),
        message_version=schema_version,
        correlation_id=correlation_id,
        causation_id=causation_id,
        producer=producer,
        published_at=datetime.now(UTC).isoformat(),
        routing_key=routing_key,
        attempts=attempts,
        failure=failure,
    )


def parse_envelope(headers: dict) -> Envelope:
    """从 AMQP headers dict 解析 Envelope;缺字段前向兼容。"""
    return Envelope(
        message_id=headers.get("message_id", ""),
        message_version=headers.get("message_version", 1),
        correlation_id=headers.get("correlation_id"),
        causation_id=headers.get("causation_id"),
        producer=headers.get("producer", ""),
        published_at=headers.get("published_at", ""),
        routing_key=headers.get("routing_key", ""),
        attempts=headers.get("attempts", 1),
        overload_retries=headers.get("overload_retries", 0),
        original_message_id=headers.get("original_message_id", ""),
        failure=headers.get("failure"),
    )
