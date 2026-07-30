"""modules.example 模块对外唯一入口。其他模块只能 import 这里的名字。"""

from app.modules.example.service import ExampleWidgetService

__all__ = ["ExampleWidgetService"]
