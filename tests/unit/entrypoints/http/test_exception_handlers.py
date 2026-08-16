"""exception_handlers 单元测试。

通过最小 FastAPI app + TestClient,覆盖:
- AppError 各子类到 HTTP 状态码的映射
- 未登记子类回退 500
- 未捕获 Exception 走 _unexpected handler
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from app.entrypoints.http.exception_handlers import register_exception_handlers
from app.shared.exceptions import (
    AppError,
    BusinessError,
    ConflictError,
    NotFoundError,
    ValidationError,
)


def _client_with_raiser(exc: Exception) -> TestClient:
    app = FastAPI()

    @app.get("/", response_model=None)
    def _raise() -> Any:
        raise exc

    register_exception_handlers(app)
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    "exc, expected_status, expected_code",
    [
        (NotFoundError("x"), 404, "not_found"),
        (ConflictError("x"), 409, "conflict"),
        (ValidationError("x"), 422, "validation_error"),
        (BusinessError("x"), 400, "business_error"),
    ],
)
def test_app_error_subclasses_map_to_status(
    exc: AppError, expected_status: int, expected_code: str
) -> None:
    resp = _client_with_raiser(exc).get("/")

    assert resp.status_code == expected_status
    body = resp.json()
    assert body["type"] == expected_code
    assert body["title"] == type(exc).__name__


def test_app_error_unknown_subclass_falls_back_to_500() -> None:
    class _UnknownError(AppError):
        code = "unknown"

    resp = _client_with_raiser(_UnknownError("x")).get("/")

    assert resp.status_code == 500
    body = resp.json()
    assert body["type"] == "unknown"
    assert body["title"] == "_UnknownError"


def test_unexpected_exception_returns_500() -> None:
    resp = _client_with_raiser(RuntimeError("boom")).get("/")

    assert resp.status_code == 500
    body = resp.json()
    assert body["type"] == "internal_error"
    assert body["title"] == "InternalServerError"
    assert body["detail"] == "unexpected server error"
