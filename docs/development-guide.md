# 开发指南

> 操作步骤手册。约束与规则见 [coding-standards.md](./coding-standards.md)，迁移操作见 [alembic/README.md](../alembic/README.md)，测试规范见 [tests/README.md](../tests/README.md)。

## 新增业务模块

参考 `app/modules/example/` 的完整实现。

1. 建 `app/modules/<域>/` 目录，创建以下基础文件（必建）；`cron.py` 按需，不需要就不建：

   ```
   __init__.py
   models.py
   repository.py
   service.py
   schemas.py
   router.py
   consumers.py
   events.py
   public.py
   ```

2. 按依赖顺序写实现：`models` → `repository` → `service` → `schemas`

3. 挂线三处（缺一不加载；机制与"为什么不自动扫描"见 [`framework-decisions.md`](./framework-decisions.md)）：

   **`app/entrypoints/http/router.py`** — 注册 HTTP 路由：
   ```python
   from app.modules.<域>.router import router as <域>_router
   router.include_router(<域>_router)
   ```

   **`app/bootstrap/api.py`**：
   ```python
   from app.modules.<域> import events as _<域>_events  # noqa: F401
   ```

   **`app/bootstrap/worker.py`**：
   ```python
   from app.modules.<域> import consumers as _<域>_consumers  # noqa: F401
   from app.modules.<域> import events as _<域>_events  # noqa: F401
   ```

4. 加数据库迁移（见 [alembic/README.md](../alembic/README.md)）

5. 验证：

   ```bash
   uv run pytest tests/unit/<域>/        # 默认跑 unit
   uv run pytest -m integration           # 写了 integration 测试再补这条
   ```

---

## 加环境变量 / 配置

参考 `app/integrations/dummyjson/settings.py` 的实现。

1. 在对应模块目录下新建 `settings.py`，继承 `BaseSettings`，设 `env_prefix`：

   ```python
   from functools import lru_cache
   from pydantic_settings import BaseSettings, SettingsConfigDict

   class FooSettings(BaseSettings):
       model_config = SettingsConfigDict(
           env_prefix="FOO_",
           env_file=".env",
           env_file_encoding="utf-8",
           extra="ignore",
           case_sensitive=False,
       )

       some_url: str = "https://example.com"
       timeout: float = 10.0

   @lru_cache
   def get_foo_settings() -> FooSettings:
       return FooSettings()
   ```

2. 在 `app/bootstrap/api.py` 的 `_ContextProvider` 里添加 `@provide` 方法暴露给 dishka：

   ```python
   from app.modules.<域>.settings import FooSettings, get_foo_settings

   @provide
   def foo_settings(self) -> FooSettings:
       return get_foo_settings()
   ```

3. router 里用 `FromDishka[FooSettings]` 取，通过 service 构造参数传入（service 本身不是 dishka 入口，不直接 `FromDishka`）

4. 在 `.env.example` 补充字段（加注释说明必填/选填）：

   ```bash
   FOO_SOME_URL=https://example.com
   FOO_TIMEOUT=10.0
   ```

---

## 加定时任务

1. 在模块目录下建 `cron.py`（或写入已有合适文件），用 `@cron_job` 装饰：

   ```python
   from apscheduler.triggers.cron import CronTrigger
   from app.infrastructure.scheduler.registry import cron_job

   @cron_job(
       CronTrigger(hour="*/1"),
       job_id="<域>.my_job",
   )
   async def my_job() -> None:
       ...
   ```

   需要访问数据库时，加 `session_factory` 参数（框架自动注入，无需额外配置）：

   ```python
   @cron_job(CronTrigger(minute="*/5"), job_id="<域>.my_job")
   async def my_job(session_factory) -> None:
       async with session_factory() as session:
           ...
   ```

   其余注入项（`integrations` / `databases` / `redis`）同 `session_factory` 一样按参数名自动注入：`integrations.get(<Client>)` 取外部 client（见"加外部 HTTP 集成"第 4 步）、`databases.get(<Name>Db)` 取业务库（见"接入新业务库"第 3 步）、`redis` 为进程共享客户端。完整清单见下节「worker 消费者」的注入表。

   > Scheduler 默认零装配（`SCHEDULER_CLIENTS = ()`、`SCHEDULER_DATABASES = ()`），`.get(<Client>)` / `.get(<Name>Db)` 会抛 `ClientNotRegisteredError` / `DatabaseNotRegisteredError`；把对应 `*_client_ctx` / `*_db_ctx` 加进 `SCHEDULER_CLIENTS` / `SCHEDULER_DATABASES`（见"加外部 HTTP 集成"第 4 步、"接入新业务库"第 2 步）才能使用。

2. 在 `app/bootstrap/scheduler.py` import 该模块：

   ```python
   from app.modules.<域> import cron as _<域>_cron  # noqa: F401
   ```

3. 验证（日志出现 `scheduler.started` 且无报错即可）：

   ```bash
   uv run uvicorn app.entrypoints.scheduler:app --port 8002
   ```

---

## worker 消费者（@task_consumer）

用 `@task_consumer("routing.key", ...)` 声明一个消费者，处理业务队列的消息。

| 参数 | 语义 |
|---|---|
| `routing_key` | 必填。消息路由键（即业务队列名） |
| `concurrency` | **全集群全局并发上限**（默认 1）。与副本数解耦：2 副本、10 副本都是 N。提高吞吐调大它；加副本只是高可用 + 分摊负载，不放大并发 |
| `timeout` | handler 执行超时秒数。不写跟随全局 `MESSAGING_CONSUMER_TIMEOUT_SECONDS`（默认 180），`None` 关闭。超时走业务失败（烧 attempts） |
| `wait_timeout` | 抢并发配额的等待超时。不写跟随 `timeout` 解析值；显式 `None` 无限等。超时 → 消息回主队列排队，不烧 attempts、不进 DLQ |
| `retry` | `False`（默认，失败记日志后丢弃）/ `True`（`RetryPolicy()` 默认 max_attempts=3）/ `RetryPolicy(...)` |
| `inbox` | 是否启用幂等去重（默认 True） |

> **长任务参数配比**：`timeout` 按任务时长上限 ~2 倍取；`wait_timeout` 设为 ≥ 副本数×任务时长上限。例：任务 ≤60s、部署 4 副本 → `timeout=120, wait_timeout=300`。

handler 按参数名自动注入依赖（声明了才传）：

| 参数 | 说明 |
|---|---|
| `session_factory` | DB 会话工厂 |
| `envelope` | 消息信封（`message_id` / `attempts` / `original_message_id` 等） |
| `redis` | 进程共享的 `redis.asyncio.Redis` 客户端 |
| `integrations` | 外部 client 聚合，`.get(<Client>)` 取 |
| `databases` | 业务库聚合，`.get(<Name>Db)` 取 |
| `attempts` | 当前是第几次尝试（从 1 起） |
| `max_attempts` | `RetryPolicy.max_attempts`；无 retry 策略时为 1 |

> `integrations` / `databases` / `redis` 对 cron job（`@cron_job`）同样按参数名注入，见「加定时任务」。

`attempts` / `max_attempts` 的典型用法是**重试耗尽前业务闭环**：最后一次机会不再 re-raise，直接把业务对象标记失败并推进状态，避免消息进 DLQ 后业务状态悬空：

```python
@task_consumer("some.event", retry=RetryPolicy(max_attempts=3))
async def on_event(msg, *, attempts: int, max_attempts: int) -> None:
    try:
        ...
    except TransientError:
        if attempts >= max_attempts:
            ...  # 最后一次：业务闭环（标 failed + 推进状态），不再抛
            return
        raise  # 未用尽 → 框架按 RetryPolicy 重投
```

### 重试 / 过载 / 拥堵语义

- 普通失败按 `RetryPolicy` 重投，`attempts` 递增；达 `max_attempts` 进 DLQ。
- **外部过载**（上游限流 429 类）：用 `overload_exceptions=(Some429Error,)` 声明，命中重投不烧 attempts，独立计数达上限（默认 100）回落常规重试。
- **拥堵 = 排队**：并发配额满、等待超时的消息回主队列排尾，不烧 attempts、永不因拥堵进 DLQ。
- **Redis 故障**：worker 启动时探活失败会拒绝启动；运行期消息不会丢失，恢复后继续处理。

---

## 加外部 HTTP 集成

参考 `app/integrations/dummyjson/` 的完整实现。

1. 建 `app/integrations/<provider>/` 目录，典型结构：

   ```
   __init__.py
   settings.py   # BaseSettings + @lru_cache（见"加配置"一节）
   client.py     # 接收注入的 httpx.AsyncClient，只封装请求
   schemas.py    # 响应模型（extra="ignore" 容忍外部 API 加字段）
   exceptions.py # 集成异常体系，独立于 AppError
   ```

2. `client.py` 接收 `httpx.AsyncClient` 注入，不自行创建：

   ```python
   import httpx
   from .schemas import FooItem

   class FooClient:
       def __init__(self, http: httpx.AsyncClient) -> None:
           self._http = http

       async def get_item(self, item_id: int) -> FooItem:
           resp = await self._http.get(f"/items/{item_id}")
           resp.raise_for_status()
           return FooItem.model_validate(resp.json())
   ```

3. 在 `app/bootstrap/container.py` 添加 `<provider>_client_ctx()`，并加入 `__all__`：

   ```python
   from app.integrations.<provider>.client import FooClient
   from app.integrations.<provider>.settings import get_foo_settings

   @asynccontextmanager
   async def foo_client_ctx() -> AsyncGenerator[FooClient]:
       cfg = get_foo_settings()
       async with httpx.AsyncClient(base_url=cfg.some_url, timeout=cfg.timeout) as http:
           yield FooClient(http)
   ```

4. **三进程按需显式装配**：
   - 把 `foo_client_ctx` 加进真正用到 `FooClient` 的进程清单：API 用 → `API_CLIENTS`；Worker 用 → `WORKER_CLIENTS`；Scheduler 用 → `SCHEDULER_CLIENTS`（tuple，逗号分隔）。**未列入的进程不会读 `FooSettings`，零配置可启动**（机制与 cron handler 一致，详见"加定时任务"）。
   - API DI：`_ContextProvider` 加 `@provide def foo_client(self) -> FooClient: return self._integrations.get(FooClient)`，路由 `FromDishka[FooClient]`。
   - Worker/Scheduler handler：声明 `*, integrations: Integrations`，内部 `client = integrations.get(FooClient)`。
   - 多实例：拆成不同的独立类（如 `FooAClient` / `FooBClient`），各自 settings / client_ctx；不要用 `(type, name)` 二元 key。

---

## 启用限流器

骨架自带的 `RateLimiter`（`allow()` 超限即拒 / `acquire()` 满了就等，语义见 `app/infrastructure/ratelimit/limiter.py`）默认不装配。把 `rate_limiter_ctx` 加进需要它的进程 `*_CLIENTS`，机制同外部集成 client（借道 integrations 通道、列入即读 settings、未列入零配置），handler 里 `integrations.get(RateLimiter)` 取用：

```python
WORKER_CLIENTS = (..., rate_limiter_ctx)
```

> 连接池惰性建连：Redis 不可达不在启动期暴露，而在首次限流调用时。

---

## 接入新业务库

接入一个新业务库 = 一个小文件（3 个声明 + 1 行工厂）+ 往进程清单加一项。**主库（`DATABASE_URL`）不动**，业务方按需接入 N 个额外业务库（同构 PG 或异构 MySQL）。

> 参考实现：`app/infrastructure/database/business/secondary.py`（demo:secondary-db）

### 1. 创建业务库声明文件

`app/infrastructure/database/business/<name>.py`：

```python
from pydantic_settings import SettingsConfigDict
from sqlalchemy.orm import DeclarativeBase

from app.infrastructure.database.business.handle import BusinessDb, business_db_ctx
from app.infrastructure.database.settings import DatabaseSettings


class <Name>DatabaseSettings(DatabaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DATABASE_<NAME>_",   # 统一挂 DATABASE_ 命名空间
        env_file=".env",                 # 生产读 .env；测试侧通过 conftest 的 _Test* 子类覆盖为 .env.test
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


class <Name>Base(DeclarativeBase):
    """<name> 库 ORM 模型的独立继承点，metadata 与主库隔离。"""


class <Name>Db(BusinessDb):
    """<name> 库注入类型。仅为 DI 类型区分，无新增字段。"""


<name>_db_ctx = business_db_ctx(<Name>Db, <Name>DatabaseSettings)
```

### 2. 注册到进程清单

`app/bootstrap/container.py`：

```python
from app.infrastructure.database.business.<name> import <name>_db_ctx

API_DATABASES       = (..., <name>_db_ctx)
WORKER_DATABASES    = (..., <name>_db_ctx)
SCHEDULER_DATABASES = ()
```

只在真正使用该库的进程列入。**列入即要求该进程启动时有对应 DSN（fail-fast）**，未列入则 settings 不被读、进程零配置可启（机制与 integration client 一致）。

### 3. 在业务代码中注入

**API 进程（dishka 直注，按 `<Name>Db` 类型）**：

```python
from app.infrastructure.database.business.<name> import <Name>Db
from dishka.integrations.fastapi import FromDishka

async def handler(db: FromDishka[<Name>Db]):
    async with db.session_factory() as s, s.begin():
        ...
```

**Worker consumer / Scheduler cron（Databases bundle，按需 `.get()`）**：

```python
from app.infrastructure.database.business.<name> import <Name>Db
from app.infrastructure.database.business.handle import Databases

@task_consumer("some.event", inbox=True)  # 或 @cron_job(...)
async def on_event(msg, *, databases: Databases) -> None:
    db = databases.get(<Name>Db)
    async with db.session_factory() as s, s.begin():
        ...
```

### 4. 添加环境变量

`.env`（生产）/ `.env.test`（测试）：

```
DATABASE_<NAME>_URL=postgresql+asyncpg://...
DATABASE_<NAME>_POOL_SIZE=10   # 可选，继承默认值
```

### 注意事项

- 业务库迁移由业务方自管（框架不提供 alembic 多库配置）
- 跨库事务不支持（[`coding-standards.md`](./coding-standards.md) §8）；outbox 也只保证主库事务内原子
- `/ready` 不探活业务库；K8s 滚动更新如有强依赖，业务方自建就绪探针
- MySQL：URL 方言用 `mysql+asyncmy://`，需 `uv sync --extra mysql`（asyncmy 为可选依赖，PG-only 不安装）
