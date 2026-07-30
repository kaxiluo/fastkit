"""验证 app.infrastructure.database.base.Base 可作为 ORM 模型基类。"""

from __future__ import annotations


def test_base_can_declare_model():
    from sqlalchemy import Column, Integer

    from app.infrastructure.database.base import Base

    class _Sample(Base):
        __tablename__ = "_sample_probe"
        id = Column(Integer, primary_key=True)

    assert _Sample.__tablename__ == "_sample_probe"
    assert hasattr(Base, "metadata")
