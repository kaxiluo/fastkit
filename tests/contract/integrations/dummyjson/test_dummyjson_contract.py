"""DummyJSON contract 测试:调真实 API,验证响应可解析、关键字段存在。

不在常规 CI 跑(addopts 已排除 contract)。手动运行:
    uv run pytest -m contract
"""

from __future__ import annotations

import httpx
import pytest

from app.integrations.dummyjson.client import DummyJsonClient

_BASE_URL = "https://dummyjson.com"
_TIMEOUT = 10.0

pytestmark = pytest.mark.contract


async def test_get_product_contract():
    async with httpx.AsyncClient(base_url=_BASE_URL, timeout=_TIMEOUT) as http:
        client = DummyJsonClient(http)
        product = await client.get_product(1)
    assert product.id == 1
    assert product.title
    assert product.price > 0
    assert product.category
