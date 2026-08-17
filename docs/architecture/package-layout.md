# `quant_research` 包结构

## 目标

项目使用单一 `quant_research` wheel。包内按业务能力和依赖方向组织，CLI 与
Dashboard 只是输入输出适配器，真实依赖只在 `bootstrap` 组合根装配。

```text
bootstrap ──> cli / dashboard ──> application ──> capabilities
    │                                      ▲
    └────────────> infrastructure ─────────┘
```

其中 `capabilities` 包括 `data`、`factors`、`factor_studies`、`universe`、
`portfolio`、`strategies`、`backtest`、`analytics`、`experiments` 和 `tasks`。

## 目录职责

- `application/`：数据更新、实验、因子研究、任务与 Worker 用例；仅依赖业务能力
  暴露的模型和 Protocol。
- `infrastructure/persistence/`：SQLite、SQLAlchemy、Alembic、任务队列和研究结果
  仓储的具体实现。
- `infrastructure/baostock/`：BaoStock 客户端、字段映射和数据集路由。
- `cli/`：Typer 命令组与终端输出适配，不负责创建数据库或供应商连接。
- `dashboard/`：FastAPI app factory、路由、请求响应模型和展示查询。
- `bootstrap/`：CLI、Dashboard 与 Worker 的组合根；允许依赖所有层。

Vue 工程继续位于仓库根目录 `frontend/`。Dashboard 组合根显式传入
`frontend/dist`，包导入期间不会连接网络或隐式创建外部资源。

## 依赖约束

`tests/unit/test_architecture_boundaries.py` 使用 AST 检查以下约束：

- 业务能力与应用层不得导入 `cli`、`dashboard` 或 `bootstrap`。
- `application` 不得导入基础设施具体实现。
- `cli` 与 `dashboard` 不得互相导入。
- `cli` 和 Dashboard 的 HTTP 接口层不得直接导入基础设施。
- `infrastructure` 不得导入 UI 或组合根。
- 生产源码不得包含旧 `quant_core` 包引用。

公共 Python 导入统一使用 `quant_research.*`，不提供旧包转发层。
