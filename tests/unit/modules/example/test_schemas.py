"""验证 Pydantic 契约。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError


def test_create_widget_request_defaults():
    from app.modules.example.schemas import CreateWidgetRequest

    req = CreateWidgetRequest()
    assert req.payload == {}


def test_create_widget_request_accepts_payload():
    from app.modules.example.schemas import CreateWidgetRequest

    req = CreateWidgetRequest(payload={"fail_until_attempt": 2})
    assert req.payload == {"fail_until_attempt": 2}


def test_create_widget_request_rejects_non_dict_payload():
    from app.modules.example.schemas import CreateWidgetRequest

    with pytest.raises(ValidationError):
        CreateWidgetRequest(payload="not-a-dict")  # type: ignore[arg-type]


def test_widget_response_from_attributes():
    from app.modules.example.schemas import WidgetResponse

    class _Fake:
        id = 1
        status = "pending"
        attempts = 0
        last_error = None
        payload = {}

    resp = WidgetResponse.model_validate(_Fake(), from_attributes=True)
    assert resp.id == 1
    assert resp.status == "pending"
