---
name: auditing-internal-docs
description: Use when reviewing project-internal documentation (READMEs, coding-standards, dev-guides, ADRs, submodule READMEs, alembic/tests/.env.example) for staleness, duplication, or noise — triggers include "评估文档 / 检查文档 / 文档对齐 / 审查文档", post-refactor doc cleanup, onboarding to a new codebase, or before committing changes that touch doc-referenced code.
---

# Auditing Internal Docs

## Overview

框架/工程内部文档（README、编码规范、开发指南、决策记录、子模块 README、`.env.example`）随代码演化容易脱节。本 skill 给出**系统化审查方法**，把发现的问题归入三类——不一致 / 重复 / 噪音——并对每类给出固定修正策略。

**核心原则：** 不要随机阅读 + 口头评估。先列全清单，再按三类逐条核验，最后按类别修。

## When to Use

**用：**
- 用户说"评估/审查/检查/对齐文档"
- 大重构、依赖升级、目录搬迁后的文档收尾
- 接手新工程，建立文档信任前的清理
- 提交前 sanity check（改动是否引入新不一致）

**不用：**
- 面向用户的 API/产品文档（关注点不同）
- 用户明确指定单文件 lint（直接改即可）
- 写新文档（这是审查，不是创作）

## Step 1: 列全文档清单

不只 `docs/`。常见散落位置：

| 位置 | 易漏点 |
|---|---|
| 项目根 | `README.md`、`CLAUDE.md` / `AGENTS.md` / `GEMINI.md` |
| `docs/*` | 决策记录（`framework-decisions.md` / ADR）、编码规范、开发指南 |
| 子模块 README | `alembic/README.md`、`tests/README.md`、`scripts/README.md` |
| 子包 README | `app/integrations/<provider>/README.md`、`app/modules/<域>/README.md` |
| 配置文件 | `.env.example`（注释与字段必须与 settings 类一致）|
| 代码内文档 | docstring 里的"默认值"、目录树注释、`# 见 docs/xxx.md` 引用 |

漏掉子模块/子包 README 是最常见的盲区。先 `find . -name README.md -not -path '*/node_modules/*' -not -path '*/.venv/*'` 一遍。

**若是提交前 sanity check：** 只列 `git diff --name-only` 中出现的文档文件，跳过全库扫描。

## Step 2: 三类问题分类核验

| 类别 | 症状 | 核验方式 |
|---|---|---|
| **不一致** | 文档说 X，代码做 Y | grep / read 实际代码逐条验证：路径存在？默认值一致？函数签名匹配？目录树与 `ls` 一致？清单项与代码常量一致？|
| **重复** | 同一规则在 2+ 处复述 | 跨文档搜关键词（"挂线"、"装配"、"事务"、"默认零装配"），看是否多处定义同一规则 |
| **噪音** | 内容与本节主题不相关 | 问"读者打开这一节是要解决什么？这段文字帮他了吗？"答不上来就是噪音 |

**关键反模式：** 不要只读文档"看是否通顺"，要**主动证伪**——文档说"默认 1s"，就去 settings 文件看实际默认值；文档说目录 `xxx/`，就 `ls` 看存在；文档说"`@task_consumer(timeout=...)` 覆盖"，就去装饰器签名找 `timeout` 参数。

## Step 3: 修正策略（按类别）

**分批执行：** 先改完**不一致**，暂停让用户确认，再处理重复，再处理噪音。不要一次性改完三类。

### 不一致 → 改文档贴合代码

- 默认改文档（除非代码本身是 bug，那要回头改代码 + 文档同改）
- **代码注释也算文档**：`engine.py:90` 注释里的"默认 1s" 与 `MessagingSettings.outbox_poll_interval_seconds = 3.0` 不一致，也算
- 改完一处后问"还有哪里提到了它"——同一事实散布多处时全改

### 重复 → 一处定义 + 其他处只链接

- 选**最详细 / 最权威**的版本做"主定义"（决策类 → ADR/framework-decisions，操作类 → dev-guide）
- 其他处压成一行 + 交叉引用链接，**不要复制定义到第二处**
- 例：三入口挂线规则 → `framework-decisions` 给表，`dev-guide` 步骤加一行"机制见 framework-decisions"

### 噪音 → 删 / 外移 / 压缩

按价值选一种：
- **完全无价值** → 删（如导航段、与正文重复的"参考"段）
- **有价值但放错位置** → 外移到合适的章节/文件（如运维操作从编码规范移到 troubleshooting；依赖管理细节从 dev-guide 移到 pyproject 注释）
- **有价值但过于啰嗦** → 压成一行（保留必要提示，删次要细节）

### YAGNI 检查

无 2+ 同类落点的"预留规则"要删。例：编码规范写"跨 service 合并用 `_in` 后缀"，但 example 模块没有 `_in` 方法，违反自己定下的"无 2+ 同类项不预留目录"。

## Common Mistakes

| 错误 | 现实 |
|---|---|
| "改一处即可" | 改代码时所有提到该代码的文档/配置/注释/测试要同步改。每次改动问"还有哪里提到它" |
| 只读不验证 | 通顺 ≠ 正确。每条事实声明都要 grep/read 实际代码证伪 |
| 重复处加引语而非链接 | 复制定义到第二处只是把同步问题从 1 处变 2 处 |
| 提交前不跑测试 | 文档改动也要跑 unit tests（万一改了代码内注释带了 typo），并 `git diff --stat` 看净行数 |
| 一次性大改 | 按一致性 → 重复 → 噪音的优先级分批改，每批后让用户检查 |
| 用"是否提交"自己定 | 用户没说提交，**绝不** git commit；改完工作区留给用户决定 |

## Verification Before Reporting Done

- `git diff --stat`：净减行数（删 > 加）是健康信号；净增大概率是引入了新重复
- `uv run pytest -q`（或对应测试命令）：即便只改文档也要跑，确保没误改代码
- 重新读改后的文档：是否引入了新的不一致？（如删了某节后，别处的"见上节 X"会变死链）
- 列出三类问题各自的数量与具体修正，**不要**口头说"已修正"

## Output Shape

报告按三列表格呈现，每行一处问题：

```
### 一、与代码不一致
| 位置 | 问题 |

### 二、重复啰嗦
| 规则 | 散布位置 | 主定义建议 |

### 三、噪音
| 位置 | 类型（删/外移/压缩）| 理由 |
```

修正后给一份"改了什么"清单，按文件聚合，每行一句。
