# Alembic 迁移

异步驱动(asyncpg)+ 单库;DDL 全部手写原生 SQL(`op.execute`),不用 autogen。`env.py` 从 `app.infrastructure.database.settings.get_database_settings()` 读 `url`,其余配置见 `alembic.ini`。

## 环境前置

- 已 `uv sync` 安装依赖
- `.env` 里 `DATABASE_URL` 指向可达的 PostgreSQL(测试用 `.env.test`)
- 库已 `CREATE DATABASE`,权限允许 DDL

## 常用命令

所有命令前缀 `uv run`,例如 `uv run alembic upgrade head`。

| 场景 | 命令 |
|---|---|
| 升级到最新 | `alembic upgrade head` |
| 回退一步 | `alembic downgrade -1` |
| 查看当前版本 | `alembic current` |
| 查看历史 | `alembic history` |
| 生成空迁移文件 | `alembic revision -m "<message>"` |
| 自动生成(仅参考) | `alembic revision --autogenerate -m "<message>"` |
| 标记当前库到某版本(不执行 SQL) | `alembic stamp <revision>` |

`--autogenerate` 仅用作 diff 参考,生成的 `op.create_table / op.add_column` 不会被采用,最终落地必须改写成原生 SQL,理由:autogen 不会带 `COMMENT ON COLUMN`,也无法表达部分索引、触发器、`pg_notify` 函数等。

## 迁移目录组织

fastkit 的迁移分两层,通过 `alembic.ini` 的 `version_locations` 合成一个 DAG:

| 层 | 路径 | 命名约定 | 谁来写 |
|---|---|---|---|
| **框架迁移** | `app/infrastructure/messaging/migrations/versions/` | 序号前缀(`0001_`、`0002_`...), revision id 用语义字符串(如 `fastkit_outbox_inbox`) | **fastkit 维护者**, 业务方**禁止修改** |
| **业务迁移** | `alembic/versions/` | 日期前缀(`YYYYMMDD_`), revision id 用语义字符串 | 业务方 |

框架迁移随 fastkit 包发布, 升级 fastkit 后业务方 `alembic upgrade head` 即可零侵入继承新表; 框架迁移的 revision id 自然成为业务迁移链的起点, 业务方第一份迁移的 `down_revision` 应指向最新框架迁移。

## 本项目约定

- **文件命名**: 业务迁移用日期前缀 + 简短描述(如 `20260727_create_example_widgets.py`); revision id 用描述性字符串(如 `create_example_widgets`), 不用 alembic 默认 hash。框架迁移的命名见上一节。
- **COMMENT**:`CREATE TABLE` 配套 `COMMENT ON TABLE`,每个字段配套 `COMMENT ON COLUMN`;同表的 COMMENT 合并到一个多语句 `op.execute` 里。
- **标点全 ASCII 半角**(`,:;()`),中文全角 `，（）：；` 会让 asyncpg prepared statement 炸 `syntax error at or near ")"`。
- **upgrade/downgrade 对称**:upgrade 建什么,downgrade 按反序 DROP 什么。`DROP TABLE` / `DROP COLUMN` 自动级联 COMMENT,无需单独 DROP。
- **状态枚举字段**:在 COMMENT 里列全可选值(如 `active=待发/重试中, dead=...`)。

## 异常处理:测试库 `alembic_version` 失配

迁移文件改名或合并后,旧库的 `alembic_version` 表可能与链路对不上(`No creation function found for '...'` 等)。两种重置方式:

```bash
# 方式 1:drop 重建(干净)
psql -c "DROP DATABASE IF EXISTS fastkit_test && CREATE DATABASE fastkit_test"
uv run alembic upgrade head

# 方式 2:清版本表后 stamp
psql -d fastkit_test -c "DELETE FROM alembic_version"
uv run alembic stamp create_example_widgets
```
