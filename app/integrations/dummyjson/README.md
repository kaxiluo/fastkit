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

Worker consumer / Scheduler cron **若业务需要**,handler 声明 `*, integrations: Integrations`(从 `app.integrations.bundle` import),内部用 `integrations.get(DummyJsonClient)` 取 client:

注意:dummyjson 只在 API/Worker 进程装配(见 `bootstrap/container.py` 的 `API_CLIENTS` / `WORKER_CLIENTS` 清单含 `dummyjson_client_ctx`);Scheduler 进程的 `SCHEDULER_CLIENTS` 显式空,零装配。在未装配 dummyjson 的进程里 `.get(DummyJsonClient)` 会抛 `ClientNotRegisteredError`。

```python
from app.integrations.bundle import Integrations
from app.integrations.dummyjson.client import DummyJsonClient

@task_consumer("some.routing.key")
async def handler(msg: SomeEvent, *, integrations: Integrations) -> None:
    client = integrations.get(DummyJsonClient)
    product = await client.get_product(1)
```

> handler 按参数名注入 `integrations`(详见 `task_consumer` / `cron_job` 文档),不走 dishka `Depends`。

## 移除本示例

dummyjson 是可删示例;bundle 机制(`bundle.py` / `integrations_lifecycle()` / `*_CLIENTS` 清单 / 三进程 lifespan 接入)是框架,**保留**。

`grep -rin dummyjson` 看全部提及;删标了 `demo:dummyjson` 的接线行(含 `*_CLIENTS` 清单里的 `dummyjson_client_ctx`)+ `app/integrations/dummyjson/` 包及其 tests,再 `uv run ruff check --fix` 清 now-unused import。删空后 `Integrations` 仍是合法的空 registry(`__init__` 不强制任何 client),`*_CLIENTS` 清单留空,调用方 `integrations_lifecycle(*<NAME>_CLIENTS)` 零装配即可。

## 配置

`base_url` 与 `timeout` 通过 `app/integrations/dummyjson/settings.py` 的 `DummyJsonSettings` 声明，`bootstrap/container.py:dummyjson_client_ctx()` 内 `get_dummyjson_settings()` 读取：

| env var | 默认 | 说明 |
|---|---|---|
| `DUMMYJSON_BASE_URL` | `https://dummyjson.com` | API 根地址 |
| `DUMMYJSON_TIMEOUT` | `10.0` | HTTP 超时秒数 |

> 这些变量有默认值，且 dummyjson 是可删 demo，**不入 `.env.example`**；
> 真实业务 integration 出现时，其 env var 才进 `.env.example`。

## 新增 Integration

完整步骤见 [`docs/development-guide.md`](../../../docs/development-guide.md) "加外部 HTTP 集成"。dummyjson 本身就是按该流程实现的样例,可对照参考。
