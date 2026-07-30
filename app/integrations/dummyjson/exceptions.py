"""DummyJSON 集成异常体系。与 AppError 平行,不继承 AppError。"""

from __future__ import annotations


class DummyJsonError(Exception):
    """DummyJSON 集成异常基类。"""


class DummyJsonNotFoundError(DummyJsonError):
    """资源不存在(HTTP 404)。"""


class DummyJsonApiError(DummyJsonError):
    """DummyJSON 返回非 2xx 非 404 错误。"""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(f"DummyJSON API error {status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail
