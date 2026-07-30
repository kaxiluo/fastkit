from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """业务 ORM 模型的统一继承点。"""
