"""验证 example consumer 注册到 pending_consumers。"""

from __future__ import annotations


def test_example_consumer_registered():
    from app.modules.example.consumers import on_example_widget_requested

    spec = on_example_widget_requested.__consumer_spec__
    assert spec.routing_key == "example.widget.requested"
    assert spec.accepts_session_factory is True
    assert spec.retry_policy is not None
    assert spec.retry_policy.max_attempts == 3
    assert spec.inbox_enabled is True
    assert spec.concurrency == 1
