"""DummyJSON 客户端:接收注入的 httpx.AsyncClient,不自行创建。"""

from __future__ import annotations

import httpx

from .exceptions import DummyJsonApiError, DummyJsonNotFoundError
from .schemas import DummyJsonProduct


class DummyJsonClient:
    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self._client = http_client

    async def get_product(self, product_id: int) -> DummyJsonProduct:
        resp = await self._client.get(f"/products/{product_id}")
        if resp.status_code == 404:
            raise DummyJsonNotFoundError(f"product {product_id} not found")
        if not resp.is_success:
            raise DummyJsonApiError(resp.status_code, resp.text)
        return DummyJsonProduct.model_validate(resp.json())
