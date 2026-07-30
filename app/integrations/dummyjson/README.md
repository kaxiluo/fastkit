# DummyJSON Integration

公开测试 API（https://dummyjson.com），用于演示顶层 `integrations/` 层的命名约定。

## 使用方式

通过 dishka 注入（API 进程）：

```python
from dishka.integrations.fastapi import FromDishka
from app.integrations.dummyjson.client import DummyJsonClient
from app.integrations.dummyjson.schemas import DummyJsonProduct


@router.get("/products/{product_id}")
async def get_product(
    product_id: int,
    client: FromDishka[DummyJsonClient],
) -> DummyJsonProduct:
    return await client.get_product(product_id)
```

Worker / Scheduler 进程当前不接入此 client。

## 配置

`base_url` 与 `timeout` 通过 `app/integrations/dummyjson/settings.py` 的 `DummyJsonSettings` 声明，`bootstrap/container.py:dummyjson_client_ctx()` 内 `get_dummyjson_settings()` 读取：

| env var | 默认 | 说明 |
|---|---|---|
| `DUMMYJSON_BASE_URL` | `https://dummyjson.com` | API 根地址 |
| `DUMMYJSON_TIMEOUT` | `10.0` | HTTP 超时秒数 |

## 新增 Integration 参考步骤

1. 在 `integrations/<provider>/` 新建目录
2. `schemas.py`：`ConfigDict(extra="ignore")` 容忍读，只映射业务需要的字段
3. `exceptions.py`：`ProviderError`（基类）→ `ProviderNotFoundError` / `ProviderApiError`
4. `client.py`：接收注入的 `httpx.AsyncClient`；无状态
5. `settings.py`：定义 `<Provider>Settings(BaseSettings)`，env_prefix 取 `<PROVIDER>_`，`base_url` / `timeout` 等调参项走 env var；进程级单例 `get_<provider>_settings()` 套 `lru_cache`
6. `bootstrap/container.py`：加 `<provider>_client_ctx()` async context manager，内部调 `get_<provider>_settings()` 取配置（provider 模块前缀 `app.integrations.<provider>`）
7. `bootstrap/api.py`（或使用的进程文件）：在 lifespan 里 `async with <provider>_client_ctx() as client:` 开启；在 `_ContextProvider` 加 `@provide` 暴露给 dishka
8. 单元测试用 `respx` mock；contract 测试打 `@pytest.mark.contract` 调真实 API
