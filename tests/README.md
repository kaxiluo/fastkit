# 测试目录约定

目录与 `app/` 同名镜像,先按层级、再按模块 / 基础设施划分;按需新增。

## 目录结构

```
tests/
├── README.md                 # 本文档
├── conftest.py               # 根 conftest:加载 .env.test、按目录自动打 marker、Settings 缓存清理
├── unit/                     # 纯单元测试,不依赖外部基础设施
│   ├── config/               # 配置项校验
│   ├── infrastructure/       # 基础设施镜像
│   │   ├── messaging/        # 消息引擎:publisher / consumer / outbox / inbox / retry 等
│   │   └── test_db_base.py
│   ├── integrations/         # 外部 API 客户端
│   │   └── dummyjson/
│   └── modules/              # 业务模块(与 app/modules/ 同名镜像)
│       └── example/
├── integration/              # 集成测试,真实连 DB / Redis / Broker
│   ├── conftest.py           # DB / Redis / Broker fixture,env 缺失自动 skip
│   ├── infrastructure/
│   │   ├── messaging/        # 消息引擎真实拓扑 / ACK / DLX / 重投 / 迁移 / 留存
│   │   ├── concurrency/      # Redis 信号量
│   │   └── ratelimit/        # 限流器
│   └── modules/              # 业务模块集成
│       └── example/
└── contract/                 # 外部接口契约测试
    ├── infrastructure/
    └── integrations/
        └── dummyjson/
```

`e2e/` 目录暂未落地;待核心业务链路成型时按需新增。

## 分层边界

### `unit/`

纯业务逻辑。**不启动 FastAPI、不连 DB / Redis / Broker、不通过 dishka 取对象**。直接构造业务对象,Fake / Double 注入依赖。

覆盖:domain 实体 / 值对象、service 业务逻辑、状态流转、参数校验、消息 schema 解析、限流 / 重试算法、FastStream `TestRabbitBroker` 内存测试(publisher 调用、consumer 触发、消息解析、依赖注入)。

### `integration/`

验证代码与基础设施边界。**真实连依赖**(连接信息由 `.env.test` 决定)。

覆盖:FastAPI 路由 + DI、dishka 容器装配、SQLAlchemy Repository、Redis 操作、FastStream 真实 RabbitMQ(拓扑 / ACK / DLX / 持久化 / 重投)、Worker 生命周期、`/ready` 探针。

### `contract/`

系统边界协议:外部 API 请求结构、回调签名、OpenAPI、消息 schema 兼容性。仅放对外部系统契约的固化。

### `e2e/`(按需新增)

核心业务链路(如「创建任务 → DB → MQ → Worker → 模型 API → 状态更新」)。只放少量端到端用例,异常组合放 unit / integration。

## conftest 与测试 env

### conftest 分层

- `tests/conftest.py`(根):
  - 加载 `.env.test` 构造会话级 `test_settings` fixture;文件缺失自动 `pytest.skip`。
  - 每个测试后清各组件 settings getter(`get_app_settings` / `get_database_settings` / `get_redis_settings` / `get_messaging_settings`)的 lru_cache,避免测试间污染。
  - `pytest_collection_modifyitems` 按目录自动打 marker:`integration/` → `integration`、`contract/` → `contract`、`e2e/` → `e2e`、其他(含 `unit/`)→ `unit`;显式 marker 优先。
- `tests/integration/conftest.py`:DB / Redis / Broker fixture,分别从 `test_database_settings` / `test_redis_settings` / `test_messaging_settings` 读连接串;关键配置缺失时 integration 自动 `pytest.skip`。另有 session 级 `clean_test_vhost`(autouse),开跑前清空 test vhost 全部队列——见下「测试 env」。
- 模块级 conftest:特定子目录独享的 fixture,放最小适用范围。

fixture 按最小适用范围分散,不堆进根目录;避免大量 `autouse=True`;fixture 只负责资源准备 / 清理,不做业务断言。

### 测试 env

- 测试读取专用 `.env.test`(从 `.env.test.example` 复制),不复用开发 `.env`。
- 集成测试 fixture 从 `.env.test` 读 `DATABASE_URL` / `REDIS_URL` / `BROKER_URL`,使用独立 db / Redis db 号 / RabbitMQ vhost,避免污染开发数据。
- `.env.test` 缺失时 integration 自动 skip——本地零基础设施也能跑 `unit`。
- durable 队列跨 pytest 会话持久:上次被中断的 run 可能把带 `attempts` 的消息经 retry TTL dead-letter 回业务队列静躺,污染下一次 run(表现为消费次数多一次)。`clean_test_vhost` fixture 在 session 开跑前经 RabbitMQ management API 删光 test vhost 全部队列兜底(零维护,不逐个列队列名)。管理端口用 RabbitMQ 默认 15672,host 取自 `BROKER_URL`。前提是 test vhost 已与开发 vhost 隔离,整库清空才安全。

## `pyproject.toml` 关键配置

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
addopts = [
    "-ra",
    "--strict-config",
    "--strict-markers",
    "--import-mode=importlib",
    "-m", "not integration and not contract and not e2e",   # 默认只跑 unit
]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
asyncio_default_test_loop_scope = "function"
markers = [
    "unit: 纯单元测试,不依赖外部基础设施",
    "integration: 与真实基础设施的集成测试",
    "contract: 外部接口契约测试",
    "e2e: 端到端业务链路测试",
]
```

- 默认 `pytest` 只跑 unit;`pytest -m integration` 单跑集成;`pytest -m ""` 跑全部。
- `import-mode=importlib`:不修改 `sys.path`,允许同名测试文件,公共 fixture 走 conftest。
- `pythonpath = ["."]`:让 `from app...` 在测试中可直接导入。
- `asyncio_mode = "auto"`:免除每个异步测试手动加 marker。
