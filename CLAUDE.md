# fastkit

纯后端异步服务骨架(FastAPI + FastStream + RabbitMQ + PostgreSQL + Redis + dishka DI)。

完整说明见 [README.md](./README.md)。

## 文档

- 编码规范 [`docs/coding-standards.md`](./docs/coding-standards.md) — 异常/事务/模块边界/消息契约/表设计
- 开发指南 [`docs/development-guide.md`](./docs/development-guide.md) — 新增模块/配置/定时任务/集成

## 代码与文档对齐

代码是单一事实来源。改代码时,所有提到该代码的文档、配置、注释、测试必须同步更新,不允许只改一边的提交。

判断方法:每次改动后问自己"还有哪些地方提到了它?",把这些位点一起改掉。
