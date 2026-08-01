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

Worker consumer / Scheduler cron **若业务需要**,可在 handler 签名声明 `integrations: Integrations`(从 `app.integrations.bundle` import)获取任意 client(dummyjson 为 demo,此处仅示范通路),例:
```python
from app.integrations.bundle import Integrations

@task_consumer("some.routing.key")
async def handler(msg: SomeEvent, *, integrations: Integrations) -> None:
    product = await integrations.dummyjson.get_product(1)
```

## 移除本示例

dummyjson 是可删示例;bundle 机制(`bundle.py` / `integrations_lifecycle()` / 三进程穿线)是框架,**保留**。

`grep -rin dummyjson` 看全部提及;删标了 `demo:dummyjson` 的接线行 + `app/integrations/dummyjson/` 包及其 tests,再 `uv run ruff check --fix` 清 now-unused import。删空后 `Integrations` 空 dataclass 合法。

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
6. `bootstrap/container.py`：加 `<provider>_client_ctx()` async context manager，内部调 `get_<provider>_settings()` 取配置（provider 模块前缀 `app.integrations.<provider>`）；在 `app/integrations/bundle.py` 的 `Integrations` 加 `<provider>: <Provider>Client` 字段；在 `integrations_lifecycle()` 里 `provider = await stack.enter_async_context(<provider>_client_ctx())` 多 enter 一层。
7. 三进程自动可用——API 在 `_ContextProvider` 加一行 per-client `@provide`(返回 `self._integrations.<provider>`);Worker/Scheduler 无需改管道,handler 声明 `integrations` 参数即可。**不要**再在 lifespan 里单独 `async with <provider>_client_ctx() as client:`——client 已由 `integrations_lifecycle()` 内部统一 enter,重复开会另起一个连接池。
8. 单元测试用 `respx` mock；contract 测试打 `@pytest.mark.contract` 调真实 API
