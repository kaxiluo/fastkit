"""验证 ExampleWidget ORM 定义。"""

from __future__ import annotations


def test_example_widget_model_definition():
    from app.infrastructure.database.base import Base
    from app.modules.example.models import ExampleWidget

    assert ExampleWidget.__tablename__ == "example_widgets"
    columns = {c.name for c in ExampleWidget.__table__.columns}
    assert columns == {
        "id",
        "payload",
        "status",
        "attempts",
        "last_error",
        "created_at",
        "updated_at",
    }
    assert ExampleWidget.__table__.metadata is Base.metadata
