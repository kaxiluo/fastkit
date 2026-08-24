# fastkit

纯后端异步服务骨架(FastAPI + FastStream + RabbitMQ + PostgreSQL + Redis + dishka DI)。主库 PostgreSQL,业务库按需接入 N 个额外库(同构 PG 或异构 MySQL)。

仓库内有三个独立进程,各自有 lifespan、互不依赖:

| 进程 | 入口 | 职责 |
| --- | --- | --- |
| HTTP API | `app.entrypoints.http.app:app` | 对外 REST,跑 FastAPI + dishka |
| Worker | `app.entrypoints.worker:app` | RabbitMQ consume + outbox relay,ASGI 暴露 `/health` 与 `/asyncapi` |
| Scheduler | `app.entrypoints.scheduler:app` | APScheduler 定时任务进程,ASGI 暴露 `/health`(目前跑 outbox/inbox retention) |

## 前置依赖

- **Python 3.13+** 与 **[uv](https://docs.astral.sh/uv/)**
- **外部基础设施(已就绪)**:PostgreSQL、Redis、RabbitMQ
  - 连接串写在 `.env`
  - RabbitMQ 新 vhost(可选):参考 `scripts/rabbitmq-init.sh`

## 快速启动

```bash
# 1. 安装依赖
uv sync

# 2. 配置环境(必填 DATABASE_URL / REDIS_URL / BROKER_URL)
cp .env.example .env

# 3. 跑数据库迁移(详见 alembic/README.md)
uv run alembic upgrade head

# 4. 三个进程(本地开发)—— Makefile 别名见 `make help`
make dev-http        # = uv run uvicorn app.entrypoints.http.app:app   --reload --port 8000
make dev-worker      # = uv run uvicorn app.entrypoints.worker:app     --port 8001
make dev-worker-2    # 同上,--workers 2(共享 :8001)
make dev-scheduler   # = uv run uvicorn app.entrypoints.scheduler:app --port 8002
# 生产参数(Dockerfile / 编排用):
# uvicorn app.entrypoints.http.app:app   --host 0.0.0.0 --port 8000 --workers 4 --proxy-headers --forwarded-allow-ips '*' --no-access-log
# uvicorn app.entrypoints.worker:app     --host 0.0.0.0 --port 8001
```

跑通后:
- HTTP: `curl -s localhost:8000/health` → `{"status":"ok"}`
- 示例业务接口: `POST localhost:8000/api/v1/example/widgets`、`GET localhost:8000/api/v1/example/widgets/{id}`
- Worker AsyncAPI 文档: 浏览器打开 `http://localhost:8001/asyncapi`
- Scheduler: `curl -s localhost:8002/health` → `{"status":"ok"}`

---

## HTTP API 进程

```bash
uv run uvicorn app.entrypoints.http.app:app --reload --port 8000
```

- **健康检查**
  - `GET /health` — 进程存活,不查依赖,返回 `{"status":"ok"}`
  - `GET /ready` — 探针 db / redis / broker,全部 ok 返回 200,任一失败返回 503,响应体含 `checks` 详情
- **关键环境变量**: `APP_NAME`、`APP_ENV`(db/redis/broker 连接串见 `.env.example`)

## Worker 进程

```bash
uv run uvicorn app.entrypoints.worker:app --port 8001
```

- **健康检查**: `GET /health` — 基于 `broker.ping(timeout=5s)`,broker 不通则返回非 200
- **AsyncAPI 文档**: 非 prod 挂载 `/asyncapi`(含 Try It Out)
- **关键环境变量**: `BROKER_URL`、`MESSAGING_OUTBOX_*`(轮询间隔/最大重试/批量大小,均有默认)、`MESSAGING_CONSUMER_TIMEOUT_SECONDS`(handler 超时秒数,默认 180;per-consumer 可用 `@task_consumer(timeout=...)` 覆盖)
- **`uvicorn --workers`**:不建议,outbox relay 的 `LISTEN` 监听会被复制 N 份;扩容走容器多副本

## Scheduler 进程

```bash
uv run uvicorn app.entrypoints.scheduler:app --port 8002
```

- **职责**: APScheduler `AsyncIOScheduler` 定时任务进程
- **健康检查**: `GET /health` — 进程存活,不查依赖;scheduler 允许零装配 DB/Redis 启动,无统一就绪标准,故不设 `/ready`
- **当前 job**: outbox/inbox retention(见 `app/bootstrap/scheduler.py`)
- **多副本**:必须单副本;`AsyncIOScheduler` 无分布式锁

---

## 测试

测试分四级,默认只跑 unit:

```bash
uv run pytest                       # 默认:只跑 unit(零基础设施依赖)
uv run pytest -m integration        # 单跑集成(需真实 PG/Redis/RabbitMQ)
uv run pytest -m ""                 # 跑全部(含 contract / e2e)
uv run pytest tests/unit/core/      # 单跑某目录
```

markers: `unit` / `integration` / `contract` / `e2e`(注册于 `pyproject.toml`,按 `tests/{unit,integration,contract,e2e}/` 目录自动应用)。
集成测试从 `.env.test` 读连接串,缺关键配置时自动 skip,本地零基础设施也能跑 unit。
目录结构与规则详见 [`tests/README.md`](./tests/README.md)。

## 项目结构

```
app/
├── entrypoints/        # 三进程入口 + HTTP 子路由
│   ├── http/           # FastAPI app + health + router + exception_handlers
│   ├── worker.py       # FastStream AsgiFastStream
│   └── scheduler.py    # ASGI app:APScheduler lifespan + /health
├── bootstrap/          # 各进程 lifespan + dishka container
├── config/             # Settings(pydantic-settings)
├── infrastructure/     # database(主库 + business/ 多业务库) / redis / messaging(outbox/inbox/dlq/retry) / observability / ratelimit / concurrency
├── integrations/       # 对外 HTTP 客户端收敛点(如 dummyjson)
├── modules/<域>/       # 业务模块(扁平七文件 + public.py)
└── shared/             # 跨层共享原语
alembic/versions/       # 业务迁移
docs/                   # coding-standards.md / development-guide.md / framework-decisions.md
scripts/                # rabbitmq-init.sh 等
tests/                  # unit / integration / contract(e2e 按需新增)
```

- 业务开发指南(新增模块 / 加配置 / 加定时任务 / 加集成)→ [`docs/development-guide.md`](./docs/development-guide.md)
- 编码规范(async 优先 / 模块边界 / 事务 / 异常 / 消息契约 / 表设计)→ [`docs/coding-standards.md`](./docs/coding-standards.md)
- 数据迁移(双轨目录 / 命令 / 约定)→ [`alembic/README.md`](./alembic/README.md)
