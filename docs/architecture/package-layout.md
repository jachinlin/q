# 包结构与依赖方向

文档状态：当前有效设计　·　日期：2026-08-22

本文定义 `quant_research` 的包职责和禁止依赖。数据契约以
[数据层设计](data-layer-design.md)为准；策略、回测与实验契约以
[策略、回测与实验设计](strategy-backtest-experiment-design.md)为准。

```text
src/quant_research/
├── domain/             # 稳定枚举、标识和值对象；不依赖应用或接口层
├── data/               # catalog/schema/LOCALIZE→CURATE→VALIDATE/只读 Repository
├── factors/            # 因子定义、批量计算、共享变换和统计内核
├── factor_studies/     # 仅保留因子分析算法，不拥有任务或持久化生命周期
├── universe/           # PIT 股票池和稳定排除原因
├── signals/            # 三类判别式信号值对象
├── risk/ costs/        # 截面策略 Risk/Cost 能力
├── portfolio/          # 组合构建、约束、权重到订单规划
├── strategies/         # Strategy 契约、五模块装配、三个内置策略
├── backtest/           # T/T+1 引擎、撮合、账户、规则和不可变产物
├── analytics/          # 绩效、成交质量与归因
├── experiments/        # Experiment/Run 模型、配置、阶段和统计治理
├── tasks/              # 通用任务模型、Handler 和消费者侧端口
├── application/        # 数据及实验用例，不导入基础设施实现
├── infrastructure/     # SQLite/Alembic、任务队列、Tushare 适配器
├── cli/                # Typer 输入输出适配器
├── dashboard/          # FastAPI 路由、DTO 和查询视图
└── bootstrap/          # 唯一组合根：CLI、Dashboard、Worker
```

依赖只能沿以下方向：

```text
bootstrap → cli / dashboard → application → capabilities
    │                                      ▲
    └────────────→ infrastructure ─────────┘
```

关键边界：

- `application`、业务能力包不得导入 CLI、Dashboard、bootstrap 或基础设施具体实现。
- CLI 与 Dashboard 不得相互导入，也不得直接构造 SQLite、Tushare 或文件系统适配器。
- 研究取数只经 `CanonicalResearchRepository`；策略只经绑定 `signal_date` 的 `DecisionData`。
- `factor_studies` 只提供纯分析函数；生命周期统一归 `Experiment → Run → EXPERIMENT_RUN`。
- `strategies` 生成整数股数 `OrderIntent`；回测引擎不消费目标权重，也不生成信号。
- `bootstrap` 负责把 Repository、Registry、规则簿、Worker 和接口适配器组装起来。

架构门禁由 `tests/unit/test_architecture_boundaries.py` 执行；新增包或移动职责时必须同步更新该测试。
