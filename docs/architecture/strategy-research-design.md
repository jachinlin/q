# 策略研究平台从零目标架构设计

> 本文是策略研究、自动选型、运行编排、持久化、产物、CLI、HTTP 和 Dashboard 的唯一
> 目标设计。数据层继续以 `data-layer-design.md` 为权威，不建立第二套行情读取路径。

## 1. 目标与边界

平台面向“提出假设—批量候选—验证集选型—锁定—一次 TEST—登记结论”的研究过程。
Alpha 可以是因子组合，也可以整体替换为双 MA 趋势或 ETF 配置信号；替换通过类型化组件
和能力校验完成，而不是在一个策略类中堆条件分支。

首版提供三种研究深度：

- `SIGNAL_STUDY`：停止于信号行为和预测关系分析。
- `PORTFOLIO_STUDY`：生成理论目标组合、风险和事前成本，不模拟成交。
- `BACKTEST_EXPERIMENT`：执行调仓、成交、实际成本、账户和绩效全链路。

不提供旧实验、旧因子研究 API、旧 YAML 或 Dashboard 兼容入口，不允许任意用户 Python
插件，不实现贝叶斯搜索、风险平价或最小方差。

## 2. 从零设计的目标架构

```text
ResearchProtocol + component configuration
                    │
                    ▼
       submit-time schema/capability validation
                    │
                    ▼
CanonicalResearchRepository ── catalog_hash / PIT / VALIDATE-ALL
                    │
                    ▼
Universe → Features → Typed Signal → Risk + Pre-trade Cost
                                      │
                                      ▼
                              Target Portfolio
                                      │
                  ┌───────────────────┴──────────────────┐
                  ▼                                      ▼
        signal/portfolio analytics             Rebalance → Execution
                                                         │
                                                         ▼
                                                Account + realized cost
                                                         │
                                                         ▼
                                            performance / attribution
                    │
                    ▼
        immutable artifacts → verify → register
```

横切控制包括：数据身份漂移检查、稳定排序、取消、幂等任务推进、不可变目录、审计、
结构化错误和可信路径边界。`strategies` 只声明组件组合；算法分别位于 `signals`、`risk`、
`costs`、`portfolio` 和 `execution`。

## 3. 领域契约

### 3.1 ResearchProtocol

`train`、`validation`、`test` 均为闭区间，必须按此顺序严格分离且互不重叠。协议还包含：

- `parameter_search_space`：配置字段点路径到非空离散值列表；禁止修改协议自身；
- `selection.primary_metric` 与 `direction`；
- `constraints[]`：指标、`LTE|GTE` 和有限阈值；
- `tie_breakers[]`：按声明顺序比较；
- `multiple_testing_method`：`NONE`、`BENJAMINI_HOCHBERG` 或
  `HOLM_BONFERRONI`；
- `adjusted_alpha`：启用校正时必须位于 `(0,1)`；
- `random_seed`：进入规范化配置和研究身份。

选型只读取 VALIDATION。TRAIN 仅用于候选诊断，TEST 只能在选择结果锁定后创建一次。

### 3.2 信号产物

| 类型 | 语义 | 必需行为 |
|---|---|---|
| `CrossSectionalScoreArtifact` | 同日跨证券可比较 Alpha | 稳定 rank、有效性与原因 |
| `DirectionalSignalArtifact` | 单证券 LONG/FLAT 方向 | `desired_exposure`、`state_changed` |
| `AllocationSignalArtifact` | 多资产配置建议 | 原始权重、强度、排名、有效性 |

公共身份包含 `run_id`、组件 ID/源码哈希、`catalog_hash`、`universe_hash` 和计算区间。

### 3.3 ComponentDescriptor

每个组件发布不可变描述：组件 ID/哈希、输入输出能力、必需数据集和字段、支持的信号类型、
是否批量、确定性声明及 Draft 2020-12 JSON Schema。组件哈希由描述和 Schema 的确定性
JSON 计算；源码或公开配置语义变化必须改变哈希。

提交前按上游顺序验证能力闭包，并拒绝：信号类型不匹配、要求协方差但风险组件不提供、
成本敏感优化器没有事前成本曲面、做空能力不一致、决策日语义不一致及模板槽位被替换为
不兼容组件。

## 4. 严格配置 Schema

顶层只允许以下字段，额外字段失败：

```text
name: string(1..128)
hypothesis: string(1..4000)
research_mode: SIGNAL_STUDY | PORTFOLIO_STUDY | BACKTEST_EXPERIMENT
strategy_id: stock_multifactor | dual_ma_trend | etf_rotation
benchmark: canonical instrument id
initial_cash_fen: positive integer
research_protocol: ResearchProtocol
universe, features, signal, decision_schedule, risk, pretrade_cost,
portfolio, rebalance, execution, analytics: non-empty component objects
```

首版组件字段如下；各对象均为 `additionalProperties=false`：

| 块 | 组件与字段 |
|---|---|
| Universe | `cn_stock_standard`: `exclude_st`, `exclude_suspended`, `min_listing_days`, `allowed_boards?`, `min_avg_amount_20d?`; `fixed_instruments`: `instruments[]` |
| Feature | `stock_research_features`: `components[]`; `price_trend_features`: `windows?`, `return_windows?`, `trend_window?`, `volatility_window`, `adv_window?` |
| Signal | `cross_sectional_multifactor`: `kind=CROSS_SECTIONAL_SCORE`, `factor_weights`; `dual_ma_directional`: `kind=DIRECTIONAL`, `short_window_sessions`, `long_window_sessions`; `etf_rotation_allocation`: `kind=ALLOCATION`, `return_weights`, `trend_window_sessions`, `volatility_window_sessions`, `volatility_penalty`, `top_n` |
| Schedule | `period_boundary`: `frequency=WEEKLY|MONTHLY`; `every_session`: `frequency=DAILY` |
| Risk | `fundamental_statistical`: `lookback_sessions`, `covariance_shrinkage`; `asset_volatility_and_liquidity`: `lookback_sessions` |
| Cost | `liquidity_impact_surface`: `fixed_bps`, `impact_bps`, `max_volume_participation` |
| Portfolio | `alpha_risk_cost_optimizer`: `min_positions`, `max_positions`, `max_position_weight`, `max_turnover`, `risk_aversion`, `cost_aversion`; `directional_exposure_mapper`: `long_weight`, `flat_weight`; `allocation_projector`: `max_position_weight` |
| Rebalance | `scheduled_with_drift_threshold`: `min_weight_drift`; `signal_state_change`: no extra fields |
| Execution | `a_share_daily`: `reference_price=OPEN|CLOSE`, `slippage_bps`, `max_volume_participation` |
| Analytics | `analyzers[]`，值来自后端目录 |

字段路径搜索空间先按路径排序，再做确定性笛卡尔积；候选值保持 YAML 顺序。展开总数最多
256，超限在创建任何数据库对象前失败。每个候选产生稳定 `variant_id` 和
`composition_hash`。

## 5. 首版组件与三个策略模板

### 5.1 股票多因子

```text
动态 A 股 PIT 股票池
→ 估值/动量截面信号
→ 波动率/Beta/行业/流动性/收缩协方差
→ 固定费率 + ADV + 平方根冲击
→ Alpha-Risk-Cost 投影梯度优化
→ 周频边界 + 权重漂移调仓
→ A 股日线执行
```

优化器先稳定筛选候选，再求解多头权重、单票上限、换手和行业约束，并发布 Alpha、风险、
成本和约束罚项分解。

### 5.2 双 MA 趋势

```text
固定证券池 → 调整后短/长 MA → LONG/FLAT → 现金/风险资产暴露映射
→ 信号状态变化调仓 → A 股日线执行
```

信号按每个交易日计算，成交不得使用形成该信号的同一收盘价。

### 5.3 ETF 轮动

```yaml
name: ETF 轮动自动实验族
hypothesis: 多周期动量与趋势过滤能在 ETF 之间形成稳定配置优势
research_mode: BACKTEST_EXPERIMENT
strategy_id: etf_rotation
benchmark: 000300.SH
initial_cash_fen: 100000000
research_protocol:
  train: {start: 2018-01-02, end: 2021-12-31}
  validation: {start: 2022-01-04, end: 2023-12-29}
  test: {start: 2024-01-02, end: 2025-12-31}
  parameter_search_space:
    signal.top_n: [1, 2, 3]
    signal.volatility_penalty: [0.0, 0.5, 1.0]
  selection:
    primary_metric: sharpe
    direction: MAXIMIZE
    constraints: [{metric: max_drawdown, operator: GTE, threshold: -0.30}]
    tie_breakers: [calmar]
    multiple_testing_method: HOLM_BONFERRONI
    adjusted_alpha: 0.10
  random_seed: 20260818
universe: {component: fixed_instruments, instruments: [510050.SH, 510300.SH, 513100.SH, 588000.SH]}
features: {component: price_trend_features, return_windows: [20, 60, 120], trend_window: 120, volatility_window: 60}
signal:
  component: etf_rotation_allocation
  kind: ALLOCATION
  return_weights: {'20': 0.20, '60': 0.30, '120': 0.50}
  trend_window_sessions: 120
  volatility_window_sessions: 60
  volatility_penalty: 0.50
  top_n: 3
decision_schedule: {component: period_boundary, frequency: MONTHLY}
risk: {estimator: asset_volatility_and_liquidity, lookback_sessions: 60}
pretrade_cost: {component: liquidity_impact_surface, fixed_bps: 2.0, impact_bps: 10.0, max_volume_participation: 0.10}
portfolio: {constructor: allocation_projector, max_position_weight: 1.0}
rebalance: {policy: scheduled_with_drift_threshold, min_weight_drift: 0.005}
execution: {simulator: a_share_daily, reference_price: OPEN, slippage_bps: 5.0, max_volume_participation: 0.10}
analytics: {analyzers: [allocation_signal, portfolio_risk, execution, performance]}
```

三个可提交完整示例位于 `configs/research/examples/`。

## 6. 自动实验族

```text
RESEARCH_EXPAND
  └─ RESEARCH_RUN(TRAIN_VALIDATION) × N
       └─ last completion atomically enqueues RESEARCH_SELECT once
            ├─ no eligible candidate → execution FAILED, no TEST
            └─ publish immutable selection.json + lock selected_variant_id
                 └─ RESEARCH_RUN(TEST) × 1
                      └─ RESEARCH_REGISTER
```

选择顺序为：约束过滤 → 校正后显著性过滤 → 主指标方向 → 次指标顺序 →
`variant_id` 稳定破同分。系统错误使执行失败；数据不足和指标不满足形成候选拒绝原因。
取消在阶段或候选批次边界生效。任何重试均创建新的 execution、run 和产物目录。

每个 run 固定七阶段：

```text
VALIDATE → UNIVERSE → RESEARCH_COMPUTE → SIMULATE
→ ANALYTICS → ARTIFACT_VERIFY → REGISTER
```

研究模式不适用的阶段记录 `SKIPPED:<reason>`，不静默省略。每阶段检查提交时捕获的
`catalog_hash`。

## 7. 持久化

研究表：

- `research_family`：不可变定义、配置哈希，可审计 mark/note；
- `research_family_execution`：输入身份、状态、锁定候选和选择理由；
- `research_variant`：ordinal、参数、规范化配置、`composition_hash`、拒绝原因；
- `research_run`：phase、七阶段状态、Manifest 和错误；
- `research_metric`：split、类别、名称、值、p-value 和 adjusted p-value；
- `research_artifact`：可信路径、类型、哈希、字节数和元数据；
- `research_tag`：可审计标签。

任务只用通用 `subject_kind/subject_id`。迁移删除旧 `experiment*`、`factor_study` 和
`factor_run` 研究元数据，但保留 Raw、Canonical、质量、目录和数据任务；旧磁盘研究产物
不删除，也不再登记、读取或显示。

SQLite 事务和活动幂等键保证多 Worker 下选型、TEST 和登记只推进一次。

## 8. 不可变产物

```text
artifacts/research/<family_id>/<execution_id>/
├── selection.json
├── variants/<variant_id>/
│   ├── train-validation/<run_id>/
│   │   ├── universe.parquet
│   │   ├── signals/signals.parquet
│   │   ├── target_portfolios.parquet
│   │   ├── fills.parquet
│   │   ├── analytics/nav.parquet
│   │   ├── strategy_definition.json
│   │   ├── research_protocol.json
│   │   └── manifest.json
│   └── test/<run_id>/...
```

发布使用同文件系统临时目录和原子重命名，禁止覆盖。最终目录重读验证路径、SHA-256、
字节数、Schema、行数、主键、排序和输入身份后才能登记。HTTP 明细查询只接受白名单
产物类型，并通过已登记 Manifest 解析可信相对路径和分页读取。

## 9. CLI、HTTP 与 Dashboard

CLI：

```text
quant research validate|submit|list|show|rerun
quant components list
```

HTTP 统一位于 `/api/v1/research/*`，提供组件目录、模板、YAML 解析/规范化/候选预览、
研究族创建/列表/详情/重新执行、mark/tag/note、execution/variant/run/指标/Manifest 与白名单
产物分页查询。不存在旧 `/api/v1/experiments` 和 `/api/v1/factor-studies`。

Dashboard 统一为研究中心：

- `/research`：研究族、状态、策略、模式、选中候选与 TEST 结果；
- `/research/new`：后端 JSON Schema 驱动表单，YAML 双向同步，展示字段错误、能力冲突、
  数据需求、候选数量和时间轴；
- `/research/:familyId`：协议、候选、信号、风险/组合、成本/执行、绩效、产物/审计。

候选区与 TEST 区必须视觉和数据隔离。运行中心显示通用关联对象、任务链、阶段、取消以及
“创建新 execution 重试”。

## 10. 身份、治理与禁止项

`composition_hash` 由规范化候选配置与组件哈希生成。execution 另绑定 `catalog_hash`、
源码、依赖锁、交易规则和环境哈希。研究定义不可变；标签、mark、note 和结论可审计修改。

禁止：测试集参与选择；信号扣成本或执行订单；组合构建器猜测信号类型；研究代码扫描
Canonical 文件；事前与实际成本混用；同一运行内根据分析结果改配置；重试覆盖旧任务、
运行或产物；只保存净值而不保留股票池、信号、目标、成交和身份链。

## 11. 测试与验收

- 单元：三类信号 Schema、字面量数值、MA 交叉、ETF 排名、风险矩阵、成本、优化约束、
  成交费用、能力失败、搜索展开、校正和 TEST 隔离。
- 集成：现有 Canonical Repository 上三策略 × 三模式；漂移、取消、失败、幂等、多
  Worker 竞争和不覆盖重试；Alembic 空库及现有库升级并保留数据层。
- API/CLI：严格 YAML、命令树、结构化错误、HTTP Schema、分页和可信产物读取。
- 前端：编排器双向同步、字段错误、候选预览、轮询、详情和取消/重试。
- 验收：三个模板各完成 TRAIN/VALIDATION 选型、锁定 selection 和唯一 TEST，并证明
  TEST 指标未参与选择。
- 门禁：Ruff、严格 mypy、完整 pytest、前端 test/typecheck/build 和 AST 架构检查。
