# `quant_research` 包结构

## 目标与依赖方向

项目只发布一个 `quant_research` wheel。研究能力按稳定领域边界拆分，CLI 与 Dashboard
只做输入输出适配，真实实现只在 `bootstrap` 组合根装配：

```text
bootstrap → cli / dashboard → application → capabilities
    │                                      ▲
    └────────────→ infrastructure ─────────┘
```

`application` 和能力包依赖消费者侧 Protocol；不得导入基础设施具体实现。数据研究只通过
`CanonicalResearchRepository` 读取通过 `VALIDATE-ALL` 门禁的数据。

## 目录职责

- `application/`：数据、研究族、任务和 Worker 用例；协调事务边界，不实现数值算法。
- `domain/`：证券标识、枚举、金额、组件描述和稳定值对象。
- `data/`：Canonical 目录、Schema、流水线与研究读取契约；这是唯一行情读取入口。
- `research_protocols/`：严格 YAML、TRAIN/VALIDATION/TEST、搜索空间展开和选型规则。
- `universe/`：动态 A 股 PIT 股票池与固定证券池。
- `features/`、`factors/`：可复用批量特征与因子定义，不决定仓位。
- `signals/`：截面分数、方向和配置三类不可混用的信号产物。
- `risk/`：波动率、Beta、行业、流动性与协方差估计。
- `costs/`：事前成本曲面与实际成交成本语义。
- `portfolio/`：Alpha-Risk-Cost 优化、方向暴露映射、配置投影和目标组合。
- `execution/`：调仓、A 股规则、成交、费用和账户状态推进。
- `analytics/`：信号、风险、成本、执行、绩效、回撤和归因。
- `strategies/`：三个不可变组件组合模板；不实现信号、组合或订单算法。
- `experiments/`：研究族身份、运行状态、指标、Manifest 和不可变产物登记。
- `tasks/`：通用 `subject_kind/subject_id` 任务和 Worker 端口。
- `infrastructure/persistence/`：SQLite、SQLAlchemy、Alembic、任务队列和研究注册表。
- `cli/`：`research`、`components`、`data`、`tasks` 和 `worker` 命令适配。
- `dashboard/`：FastAPI app、`/api/v1/research/*`、可信产物分页读取和展示查询。
- `bootstrap/`：CLI、Dashboard、Worker 的唯一组合根。

Vue 工程位于 `frontend/`。研究中心包含 `/research`、`/research/new` 和
`/research/:familyId`；数据中心、市场全景和 Notebook 继续复用现有接口。

## 依赖与发布约束

- 业务能力与应用层不得导入 `cli`、`dashboard`、`bootstrap`。
- `application` 不得导入 `infrastructure`；具体注册表、仓储和队列只由组合根注入。
- `cli` 与 `dashboard` 不得互相导入，也不得直接导入基础设施。
- `execution` 只消费目标组合、市场切片、账户和规则，不依赖策略实现。
- `analytics` 只读不可变运行产物，不在同一次运行中反馈修改配置。
- 包导入不得联网、升级数据库、启动线程或扫描用户数据目录。
- 公共导入统一使用 `quant_research.*`，不提供旧实验、旧因子研究或旧包转发层。
- 架构门禁由 `tests/unit/test_architecture_boundaries.py` 执行。
