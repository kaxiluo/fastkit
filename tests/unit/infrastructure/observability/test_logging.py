"""configure_logging 单测:回归 QueueHandler 把 structlog dict ``record.msg`` str 化的 bug。"""

from __future__ import annotations

import io
import json
import logging
import queue
import sys

import pytest
import structlog


@pytest.fixture
def _restore_logging():
    """快照并还原全局 logging / structlog 状态,避免污染其他测试。"""
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    yield
    import app.infrastructure.observability.logging as logmod

    if logmod._listener is not None:
        logmod._listener.stop()
        logmod._listener = None
    root.handlers = saved_handlers
    root.setLevel(saved_level)
    structlog.reset_defaults()


def test_passthrough_queue_handler_keeps_dict_msg():
    """透传 prepare 必须原样返回 record,保留 dict ``msg``(默认实现会 str 化)。"""
    from app.infrastructure.observability.logging import _PassthroughQueueHandler

    handler = _PassthroughQueueHandler(queue.Queue(-1))
    record = logging.makeLogRecord({"msg": {"event": "sample", "k": 1}})

    prepared = handler.prepare(record)

    assert prepared is record
    assert prepared.msg == {"event": "sample", "k": 1}


def test_configure_logging_emits_valid_json(_restore_logging, monkeypatch):
    """端到端:日志经 QueueHandler → QueueListener → ProcessorFormatter 后仍是合法 JSON。"""
    from app.config.settings import AppSettings
    from app.infrastructure.observability.logging import configure_logging

    buf = io.StringIO()
    monkeypatch.setattr(sys, "stderr", buf)  # StreamHandler 构造时绑定当前 stderr

    configure_logging(AppSettings(log_format="json", log_level="INFO"))
    structlog.get_logger().info("test.event", answer=42)

    import app.infrastructure.observability.logging as logmod

    logmod._listener.stop()  # 停 listener 以 flush 后台队列

    out = buf.getvalue()
    assert "Logging error" not in out
    assert "has no attribute" not in out

    line = next(ln for ln in out.splitlines() if "test.event" in ln)
    parsed = json.loads(line)
    assert parsed["event"] == "test.event"
    assert parsed["answer"] == 42
    assert parsed["level"] == "info"
    assert parsed["timestamp"]
    assert not parsed["timestamp"].endswith("Z"), "timestamp must not end with bare Z"
