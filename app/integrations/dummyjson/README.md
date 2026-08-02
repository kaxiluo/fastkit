# DummyJSON Integration

可删示例（dummyjson.com 公开测试 API），演示顶层 `integrations/` 层的命名约定。完整接入与使用模式见 [`docs/development-guide.md`](../../../docs/development-guide.md) "加外部 HTTP 集成"，dummyjson 本身就是按该流程实现的样例。

## 装配

只在 API/Worker 进程装配（`app/bootstrap/container.py` 的 `API_CLIENTS` / `WORKER_CLIENTS` 含 `dummyjson_client_ctx`）；Scheduler 的 `SCHEDULER_CLIENTS` 显式空。未装配的进程 `.get(DummyJsonClient)` 抛 `ClientNotRegisteredError`（机制见 development-guide "加定时任务"）。

## 配置

| env var | 默认 | 说明 |
|---|---|---|
| `DUMMYJSON_BASE_URL` | `https://dummyjson.com` | API 根地址 |
| `DUMMYJSON_TIMEOUT` | `10.0` | HTTP 超时秒数 |

有默认值且 dummyjson 是可删 demo，**不入 `.env.example`**；真实业务 integration 出现时，其 env var 才进 `.env.example`。

## 移除本示例

bundle 机制（`bundle.py` / `integrations_lifecycle()` / `*_CLIENTS` 清单 / 三进程 lifespan 接入）是框架，**保留**。删标了 `demo:dummyjson` 的接线行（含 `*_CLIENTS` 清单里的 `dummyjson_client_ctx`）+ `app/integrations/dummyjson/` 包及其 tests，再 `uv run ruff check --fix` 清 unused import。删空后 `Integrations` 仍是合法空 registry，`*_CLIENTS` 清单留空，`integrations_lifecycle(*<NAME>_CLIENTS)` 零装配即可。
