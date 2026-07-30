"""DummyJsonClient 单元测试:respx mock httpx,无网络。"""

from __future__ import annotations

import httpx
import pytest
import respx

from app.integrations.dummyjson.client import DummyJsonClient
from app.integrations.dummyjson.exceptions import (
    DummyJsonApiError,
    DummyJsonNotFoundError,
)

_BASE_URL = "https://dummyjson.com"


@pytest.fixture
def client() -> DummyJsonClient:
    return DummyJsonClient(httpx.AsyncClient(base_url=_BASE_URL))


@respx.mock
async def test_get_product_ok(client: DummyJsonClient):
    respx.get(f"{_BASE_URL}/products/1").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": 1,
                "title": "iPhone 9",
                "price": 549.99,
                "description": "An apple mobile",
                "category": "smartphones",
                "extra_field": "ignored",
            },
        )
    )
    product = await client.get_product(1)
    assert product.id == 1
    assert product.title == "iPhone 9"
    assert product.price == 549.99
    assert product.category == "smartphones"


@respx.mock
async def test_get_product_not_found(client: DummyJsonClient):
    respx.get(f"{_BASE_URL}/products/999").mock(
        return_value=httpx.Response(404, json={"message": "not found"})
    )
    with pytest.raises(DummyJsonNotFoundError):
        await client.get_product(999)


@respx.mock
async def test_get_product_server_error(client: DummyJsonClient):
    respx.get(f"{_BASE_URL}/products/1").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    with pytest.raises(DummyJsonApiError) as exc_info:
        await client.get_product(1)
    assert exc_info.value.status_code == 500


@respx.mock
async def test_get_product_ignores_extra_fields(client: DummyJsonClient):
    """容忍读:响应含未声明字段不应抛异常。"""
    respx.get(f"{_BASE_URL}/products/1").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": 1,
                "title": "Phone",
                "price": 99.0,
                "description": "...",
                "category": "smartphones",
                "discountPercentage": 8.7,
                "rating": 4.5,
                "stock": 100,
            },
        )
    )
    product = await client.get_product(1)
    assert product.id == 1
