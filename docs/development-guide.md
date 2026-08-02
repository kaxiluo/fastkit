# 开发指南

> 操作步骤手册。约束与规则见 [coding-standards.md](./coding-standards.md)，迁移操作见 [alembic/README.md](../alembic/README.md)，测试规范见 [tests/README.md](../tests/README.md)。

## 新增业务模块

参考 `app/modules/example/` 的完整实现。

1. 建 `app/modules/<域>/` 目录，创建以下文件（router / consumers / events / cron 按需，不需要的留空）：

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

3. 挂线三处（缺一不加载）：

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
   uv run pytest -m integration
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

3. service 里通过 `FromDishka[FooSettings]` 注入使用

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

   需要外部 client 时，加 `*, integrations: Integrations` 参数（同 `session_factory` 一样由框架按参数名自动注入），内部用 `.get(<Client>)` 取：

   ```python
   from app.integrations.bundle import Integrations
   from app.integrations.<provider>.client import FooClient

   @cron_job(CronTrigger(minute="*/5"), job_id="<域>.my_job")
   async def my_job(*, integrations: Integrations) -> None:
       client = integrations.get(FooClient)
       ...
   ```

   > Scheduler 默认零装配（`SCHEDULER_CLIENTS = ()`），`.get(<Client>)` 会抛 `ClientNotRegisteredError`；把对应 `*_client_ctx` 加进 `SCHEDULER_CLIENTS`（见"加外部 HTTP 集成"第 4 步）才能使用。

2. 在 `app/bootstrap/scheduler.py` import 该模块：

   ```python
   from app.modules.<域> import cron as _<域>_cron  # noqa: F401
   ```

3. 验证（日志出现 `scheduler.started` 且无报错即可）：

   ```bash
   uv run python -m app.entrypoints.scheduler
   ```

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
   - 把 `foo_client_ctx` 加进用到的进程对应的清单：API 用 → 加进 `API_CLIENTS`；Worker 用 → 加进 `WORKER_CLIENTS`；Scheduler 用 → 加进 `SCHEDULER_CLIENTS`（tuple，逗号分隔）。
   - Scheduler 默认零装配（`SCHEDULER_CLIENTS = ()`）；只有当某 cron 真要用 `FooClient` 时才加。未装配的进程不会被 `FooSettings` 缺失阻塞。
   - API DI：`_ContextProvider` 加 `@provide def foo_client(self) -> FooClient: return self._integrations.get(FooClient)`，路由 `FromDishka[FooClient]`。
   - Worker/Scheduler handler：声明 `*, integrations: Integrations`，内部 `client = integrations.get(FooClient)`。
   - 多实例：拆成不同的独立类（如 `FooAClient` / `FooBClient`），各自 settings / client_ctx；不要用 `(type, name)` 二元 key。

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

API_DATABASES       = (..., <name>_db_ctx)   # 仅在真正用到的进程列入
WORKER_DATABASES    = (..., <name>_db_ctx)
SCHEDULER_DATABASES = ()
```

只在真正使用该库的进程列入。**列入即要求该进程启动时有对应 DSN（fail-fast）**，未列入则 settings 不被读、进程零配置可启。

### 3. 在业务代码中注入

**API 进程（dishka 直注，按 `<Name>Db` 类型）**：

```python
from app.infrastructure.database.business.<name> import <Name>Db
from dishka.integrations.fastapi import FromDishka

async def handler(db: FromDishka[<Name>Db]):
    async with db.session_factory() as s, s.begin():
        ...
```

**Worker consumer（Databases bundle，按需 `.get()`）**：

```python
from app.infrastructure.database.business.<name> import <Name>Db
from app.infrastructure.database.business.handle import Databases

@task_consumer("some.event", inbox=True)
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
- `cron job` 暂不支持直接注入 `BusinessDb`，有需求时另开 spec
- 跨库事务不支持，见 coding-standards.md "多库事务边界"
- `/ready` 不探活业务库；K8s 滚动更新场景下如有强依赖，需业务方自建就绪探针
- 异构 MySQL：设计支持（URL 方言改为 `mysql+asyncmy://`），v1 不内置驱动，真正接入时选型 `asyncmy` vs `aiomysql` 后再加依赖
