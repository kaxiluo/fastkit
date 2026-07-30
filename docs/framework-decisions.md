# 技术决策

> 重要技术选型与设计决策的记录。
>
> 当前已记录：
>
> 1. **业务模块显式注册（不自动扫描）** — HTTP / Worker / Scheduler 四入口为何都要求在 bootstrap 显式 import

---

## 1. 业务模块显式注册（不自动扫描）

**背景**：新增 HTTP 路由 / RabbitMQ 消费者 / 事件 / 定时任务时，业务方都必须在 `app/bootstrap/` 对应进程文件里写一行显式 import 触发装饰器注册。常被问"为什么不能自动扫描"。

**决策**：保持显式 import，不引入目录扫描。

**机制**：装饰器（`@router` / `@task_consumer` / `@event` / `@cron_job`）只在模块被 import 时才执行。Python 没有内置包扫描；不 import 装饰器就不跑，进程启动时拿不到任何业务定义，静默地"什么都没加载"。

四入口挂线对照：

| 入口 | 挂线文件 | 触发的装饰器 |
| --- | --- | --- |
| HTTP | `app/bootstrap/api.py` | `@event` |
| HTTP 路由 | `app/entrypoints/http/router.py` | `router.include_router` |
| Worker | `app/bootstrap/worker.py` | `@task_consumer` + `@event` |
| Scheduler | `app/bootstrap/scheduler.py` | `@cron_job` |

连 infrastructure 自己的 inbox/outbox retention 都得显式 import——不是只针对业务方的约束。

**理由**：

1. 与 FastAPI / FastStream / APScheduler 默认一致，硬上自动扫描要和三个框架作对
2. 加载链可追溯——出问题时 grep `bootstrap/*.py` 就知道加载了什么
3. 失败前置——拼错、import 错误在启动时立刻暴露，不会让任务悄无声息不跑
4. 测试隔离简单——registry 手动清就行
5. 四入口对称——单独给 cron 开自动扫描反而认知成本变高

**业界对照**：

- **显式派（Python 主流）**：FastAPI / FastStream / APScheduler 默认、Django URL、Celery beat。哲学"Explicit is better than implicit"（PEP 20）。代价是每加功能要改 bootstrap。
- **自动扫描派**：Celery `autodiscover_tasks`、Django AppConfig、Spring Boot `@ComponentScan`、Rails。哲学"Convention over Configuration"。代价是隐式、拼错不暴露、启动慢、循环依赖排查难。

**何时重新评估**：

- 业务 cron 任务 10+ 个，`bootstrap/scheduler.py` 频繁改动引发 merge conflict
- 多模块并行开发，bootstrap 文件成为协作瓶颈
- 团队对"忘改 bootstrap 导致任务不跑"感到痛苦

升级方向借鉴 Celery `autodiscover`：约定 `app/modules/*/cron.py`，启动时扫描 import。升级前要权衡上面 5 条理由是否还成立。

---

## 参考

- 操作步骤见 [development-guide.md](./development-guide.md)
- 模块/入口的约束规则见 [coding-standards.md §2](./coding-standards.md)
