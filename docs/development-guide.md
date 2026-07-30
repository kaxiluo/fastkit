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

3. 在 `app/bootstrap/container.py` 添加客户端生命周期：

   ```python
   from app.integrations.<provider>.client import FooClient
   from app.integrations.<provider>.settings import get_foo_settings

   @asynccontextmanager
   async def foo_client_ctx() -> AsyncGenerator[FooClient]:
       cfg = get_foo_settings()
       async with httpx.AsyncClient(base_url=cfg.some_url, timeout=cfg.timeout) as http:
           yield FooClient(http)
   ```

4. 接入 DI：参考 `app/bootstrap/api.py` 中 dummyjson 的完整接入方式
