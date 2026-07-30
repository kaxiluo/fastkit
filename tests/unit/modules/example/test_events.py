"""验证 example.widget.requested 注册到 event registry。"""

from __future__ import annotations


def test_example_widget_requested_registered():
    from app.modules.example.events import ExampleWidgetRequested

    meta = ExampleWidgetRequested.__event_meta__
    assert meta.routing_key == "example.widget.requested"
    assert meta.schema is ExampleWidgetRequested
    assert meta.aggregate == "example"
    assert meta.schema_version == 1


def test_example_widget_requested_fields():
    from app.modules.example.events import ExampleWidgetRequested

    msg = ExampleWidgetRequested(widget_id=42)
    assert msg.message_version == 1
    assert msg.widget_id == 42
