# 策略、回测与实验设计

文档状态：当前有效设计　·　日期：2026-08-22

本文是策略定义与组装、订单级回测、实验编排与追踪的权威详细设计。平台定位、整体依赖方向、
因子层、分析层、Worker 和分阶段实施见[总体设计](design.md)；数据读取和 PIT 门禁见
[数据层设计](data-layer-design.md)。本文不重复定义因子计算、分析公式或通用任务队列。

## 目录

1. 端到端边界
2. 策略层设计（A+B 扩展 / 订单驱动）
3. 回测层设计（订单级引擎 / 多空账务）
4. 实验层设计（编排 / 追踪 / 比较）
5. 接口与 Schema 契约

## 1. 端到端边界

策略、回测和实验按以下单向链路协作：

```text
CanonicalResearchRepository / 因子输出
    → Strategy（信号、目标暴露或直接订单）
    → OrderIntent
    → BacktestEngine（撮合、账务、规则、估值）
    → nav / holdings / orders / fills / costs
    → Analytics
    → ExperimentRunner（编排、登记、比较、结论）
```

边界规则：

- 策略只决定“想交易什么”，不得自行撮合、改写账户或绕过 PIT 数据视图。
- 回测只回答“订单在规则下发生了什么”，不得生成信号或承担实验选型。
- 实验只负责编排、状态、产物和比较，不实现策略逻辑、撮合规则或分析公式。
- 三层共享提交时捕获的数据目录身份；阶段边界发现漂移立即失败。
- 事前成本模型与事后成交费用必须使用同一规则来源，以便逐笔对账。

***

## 2. 策略层设计（A+B 扩展 / 订单驱动）

### 2.1 定位

策略层是工作台的**生产力核心**：让"加一个新策略"的成本压到最低——只写策略逻辑，
不改 runner / 回测引擎 / 基础设施。策略只跟稳定公开契约打交道：`DecisionContext`（绑定 signal\_date 的
只读窄视图 + 账户视图）与 `OrderIntent`（输出，只带整数股数）。

### 2.2 边界

**输入**（每个决策时点，`Strategy.on_event(ctx)` 的 `ctx: DecisionContext`）：

- `signal_date` / `execute_date`（T 决策、T+1 执行）。
- `data: DecisionData`——**绑定到本 signal\_date 的只读窄视图**：`bars/adjusted_bars/log_returns/
  daily_basics/factor_values/industry/security_status/stock_universe`，方法签名**均无 as\_of/end 参数**，
  策略在类型上无法请求 > signal\_date 的数据（PIT 物理边界，非通用 Repository）。
- `account: AccountView`——`cash_fen`、`positions`（正多；\[P3b-2] 负空）、`sellable`、`available_margin_fen`。
- 构造期输入：`StrategySpec`（数据/因子依赖声明、参数）。A 底座额外输入五模块配置
  （AlphaModel/RiskModel/CostModel/ConstructionModel/ConstraintSet 的 `{model_id, params}`）。

**输出**：

- `Sequence[OrderIntent]`（见 `§5.1`）：`instrument_id, side∈{BUY,SELL,SHORT_OPEN,SHORT_COVER},
  quantity(正整数), reason`。**只带整数股数，无权重字段**。可空（该日不交易）。
- A 底座（`CrossSectionalStrategy`）内部先产 `TargetWeights`，由 `WeightTargetStrategy` 基类经
  `RebalancePlanner` 翻译成整数股数 `OrderIntent`；**权重不进入引擎输入**。
- 缺依赖能力时抛 `STRATEGY_CAPABILITY_UNAVAILABLE`；选型缺前置（如 MVO 要非退化风险模型）抛 `PIPELINE_MODEL_UNAVAILABLE`。

**不负责**：撮合、账务、绩效——那是回测层与实验层的输出。

### 2.3 订单意图是唯一驱动接口

要支持截面选股、择时/CTA、配对、事件驱动四类范式，策略输出必须是**订单意图**层级，而非目标权重：

```text
策略在每个决策时点产出 OrderIntent（买/卖/开空/平仓 + 整数股数）
                       ↓
回测引擎撮合 + 账务（见 `§3`）
```

"目标权重"是订单的**特例**：`WeightTargetStrategy` 基类把 `target_weights` 翻译成再平衡订单，
多因子/ETF 作者仍只声明权重、无感经过基类。反之——用权重表达配对做空、事件驱动离散下单——
不成立，所以底层必须是订单。这是 backtrader/zipline/vnpy 的共同选择。

### 2.4 策略扩展模型：A + B

#### 2.4.1 B — 策略即插件（底层通用口子）

```python
class Strategy(Protocol):
    @property
    def spec(self) -> StrategySpec: ...          # id / 频率 / 数据依赖声明 / 参数
    def warmup(self, ctx: DecisionContext) -> None: ...
    def on_event(self, ctx: DecisionContext) -> Sequence[OrderIntent]: ...
```

- `on_event` 是唯一必须实现的方法：给定当前决策时点的 PIT 上下文（可见行情、因子、持仓、
  现金/保证金），返回订单意图。截面、择时、配对、事件驱动都能表达。
- 通过 `StrategyRegistry`（与 `FactorRegistry` 同一注册表模式）注册；配置按 `{strategy_id, params}` 选择。
- 数据依赖在 `spec` 声明，preflight 校验；缺能力则 `STRATEGY_CAPABILITY_UNAVAILABLE`。

#### 2.4.2 A — 策略即配置（截面范式的模块化底座）

大多数迭代是"同范式微调"（换因子/换配权/加约束）。为此提供内置 `CrossSectionalStrategy`
（`WeightTargetStrategy` 子类），由五个可插拔模块组装，配置驱动、无需写代码：

```text
FactorArtifacts ─► AlphaModel ─► expected_return / score
returns/history ─► RiskModel  ─► Σ
holdings/cash   ─► CostModel  ─► 事前成本估计
                     ▼
        PortfolioConstructionModel（优化器） + ConstraintSet
                     ▼
              target_weights ─►（基类翻译）─► OrderIntent
```

- **AlphaModel** / **RiskModel** / **TransactionCostModel** / **PortfolioConstructionModel** /
  **ConstraintSet**——五个可插拔模块，各自设计见 §2.5。

每类模块一个注册表 `model_id → 实现`。ETF 轮动、股票多因子是"底座的两个内置配置"，不是写死特例。
换任一模块即一个新策略配置，可直接跑、直接与其他 Run 结果并排比较。

#### 2.4.3 A 与 B 的关系

```text
Strategy (Protocol, B 的口子)
  └── WeightTargetStrategy (基类：target_weights → OrderIntent)
        └── CrossSectionalStrategy (A 的底座：五模块组装)
              ├── 内置配置: etf_rotation
              └── 内置配置: stock_multifactor
        └── DualMATrendStrategy (B: 时序状态 → 目标暴露 → OrderIntent)
  └── PairsStrategy / EventDrivenStrategy … (B: 直接实现 on_event)
```

A 是 B 之上的便利层，覆盖 \~80% 微调；B 覆盖异构范式。二者同一 `Strategy` 契约、同一回测引擎。

### 2.5 五模块设计（Alpha / Risk / Cost / Construction / Constraint）

A 底座（`CrossSectionalStrategy`）把"截面选股→目标权重"拆成五个**消费者侧 Protocol 端口**，
每类一个注册表 `model_id → 实现`，配置 `{model_id, params}` 选型。共同约束：

- 只接收 `DecisionContext`（绑定 signal\_date 的窄视图 `DecisionData` + 账户视图）取数，
  **不得**自取任意日期数据；估计窗口不越 signal\_date（PIT 铁律）。
- 冻结不可变配置；参数进 `config_hash`（换参即新 Run，可并排比较）。
- 纯函数、确定性：同输入同输出、稳定排序。
- 缺前置能力抛 `PIPELINE_MODEL_UNAVAILABLE`（如 MVO 要求非退化风险模型）。

优化目标（组合构建的统一形式，退化项按所选模块置零）：

```text
maximize_w   αᵀw − λ·wᵀΣw − TC(Δw)      s.t. ConstraintSet(w)
             └AlphaModel  └RiskModel └CostModel   └ConstraintSet
```

#### 2.5.1 AlphaModel — 预期收益 / 打分

- **职责**：把因子/信号转成每证券的截面预期收益或可比打分（越大越优）。
- **契约**：`expected_returns(ctx, universe) -> DataFrame[instrument_id, score, is_valid, reason_code]`。
  输入 `ctx.data.factor_values(...)`（已绑定 signal\_date）；无效样本保留行 + 原因码，不静默丢。
- **内置谱系**：
  - `single_factor`：单因子方向调整后直接作分。
  - `multi_factor_composite`：固定 `MAD 去极值 → 截面 zscore → 方向调整 → 类别聚合`，**复用因子层
    `transforms`，禁止另写近似**；有效因子数不足则该证券排除。
  - （后续）`ml_forecast`：模型打分，输入仍走 PIT 窄视图。
- **不变量**：只用信号日可见因子；方向与类别权重进 `config_hash`；输出可比、可 rank。

#### 2.5.2 RiskModel — 协方差 / 风险结构

- **职责**：提供组合优化所需的风险度量 Σ（或退化占位）。
- **契约**：`covariance(ctx, universe) -> CovarianceEstimate`。Σ **只由** **`available_at ≤ signal_date`
  的** **`ctx.data.log_returns(...)`** **估计**，估计窗口是参数、不得越界。
- **内置谱系**：
  - `none`（退化）：返回对角/单位占位，优化目标退化为纯打分（首版默认，配 TopN 用）。
  - `sample_cov`：样本协方差（窗口可配）。
  - `shrinkage`：Ledoit-Wolf 收缩，改善病态与小样本。
  - `factor_risk`：`Σ = BFBᵀ + D`（因子暴露 B、因子协方差 F、特异风险 D）。
- **不变量**：PIT 估计窗口；非退化模型才允许 MVO/MinVariance/RiskParity，否则 `PIPELINE_MODEL_UNAVAILABLE`。

#### 2.5.3 TransactionCostModel — 事前成本估计

- **职责**：给优化器/权重翻译提供**事前**成交成本估计，用于抑制过度换手、决定交易量。
- **契约**：`estimate(trades, ctx) -> DataFrame[instrument_id, est_cost_fen]`（整数分）。
- **内置谱系**：`fixed_bps`（规则簿佣金率及单笔最低佣金）/ `linear_impact`（叠加线性冲击 × 参与率）/
  `sqrt_impact`（Almgren 型 √参与率冲击）。
- **双角色一致性（硬约束）**：事前 CostModel 与回测撮合的**事后**费用（`MarketRuleBook.fees`）
  **必须由 `configs/rules/a_share.yaml` 同一实例构造**。实验 YAML 不能覆盖佣金率或最低佣金，
  只允许为冲击模型配置 `impact_bps` 与 `max_participation`；冲击和撮合滑点独立列示。

#### 2.5.4 PortfolioConstructionModel — 组合构建 / 优化器

- **职责**：给定 alpha、Σ、成本、约束、当前持仓与现金，求目标权重。
- **契约**：`construct(alpha, risk, cost, constraints, ctx, account) -> Mapping[InstrumentId, float]`
  （权重和 ≤ 1，余额为现金）；结果由 `CrossSectionalStrategy` 包成 `TargetWeights`，
  经 `RebalancePlanner` 翻译为整数股数 `OrderIntent`——**权重不进引擎**。
- **内置谱系**：
  - `top_n_equal_weight`（首版默认）：分数前 N 等权 + 上限/换手/流动性约束；Σ/成本退化不参与。
  - `mean_variance`：最大化 `αᵀw − λ·wᵀΣw − TC`，带约束；要求非退化 RiskModel；先闭式/轻量 QP，
    不引重型求解器依赖。
  - （按需）`risk_parity` / `min_variance`。
- **不变量**：确定性解；持仓数不足抛约束违例；初次建仓豁免换手约束。

#### 2.5.5 ConstraintSet — 声明式约束

- **职责**：声明并强制组合约束，被优化器消费、构建后二次校验（防优化器实现违反）。
- **契约**：`apply(weights, ctx) -> weights`（裁剪到可行域）+ `validate(weights)`（越界即报错）。
- **覆盖**：个股权重上限、持仓数区间、换手上限、行业中性/暴露边界（用信号日 as-of `industry_code`，
  PIT，单成员组失效）、流动性（最小 ADV）、多空/gross/net 敞口（\[P3b-2]）。
- **不变量**：约束内容进 `config_hash`；行业约束不回填未来状态。

#### 2.5.6 装配（StrategyPipeline）

```text
StrategyPipeline = AlphaModel + RiskModel + CostModel + ConstructionModel + ConstraintSet
CrossSectionalStrategy(pipeline).target_weights(ctx):
    universe = ctx.data.stock_universe().filter(eligible)
    alpha = pipeline.alpha.expected_returns(ctx, universe)
    risk  = pipeline.risk.covariance(ctx, universe)
    w = pipeline.construction.construct(alpha, risk, pipeline.cost, pipeline.constraints, ctx, account)
    w = pipeline.constraints.apply(w, ctx); pipeline.constraints.validate(w)
    return TargetWeights(signal_date, execute_date, w)
```

内置策略即两份装配：`etf_rotation`（composite 动量/趋势/波动 + none + fixed\_bps + top\_n）、
`stock_multifactor`（七因子 composite + none|shrinkage + fixed\_bps + top\_n|mvo）。换任一模块即新策略配置。
各模块的精确口径、内置实现与 oracle 见 [实现级细化](implemention.md) `§4.5`。

### 2.6 必须支持的四类范式

- 截面选股 → 目标权重 → 调仓（多因子、ETF 轮动）：走 A 底座。首版即可跑（多头）。
- 择时 / CTA（单标的仓位随时间变化）：走 B 插件。首个内置实现是 `dual_ma_trend`；多头择时
  首版即可，**空头腿随 P3b-2**。
- 配对交易（成对相对头寸，需做空）：走 B 插件。**纯多空对冲随 P3b-2**（依赖做空账务）。
- 事件驱动（稀疏、按事件触发的离散订单）：走 B 插件。多头版首版即可。

> 契约层面四范式都可表达；**依赖做空的腿在 P3b 解锁**（见 `§3.5`）。
> P3 阶段策略产出的 `SHORT_OPEN/SHORT_COVER` 会被引擎拒绝 `SHORT_NOT_SUPPORTED`。

### 2.7 内置双均线趋势策略（`dual_ma_trend`）

#### 2.7.1 定位与组装

双均线是独立的时序方向模型，不进入 `CrossSectionalStrategy` 的 Alpha 五模块。它实现为
`DualMATrendStrategy(WeightTargetStrategy)`：策略只决定单标的目标暴露，基类继续复用
`RebalancePlanner` 将目标权重转换为整数股数订单，回测引擎继续统一负责 T+1、涨跌停、停牌、
费用、滑点、容量和账务。

```text
fixed instrument
  → adjusted close history
  → short/long moving average
  → LONG|FLAT state
  → target exposure
  → RebalancePlanner
  → BUY|SELL OrderIntent
  → common execution/account/analytics
```

首版只支持 LONG/FLAT：`LONG` 映射到 `long_weight`，`FLAT` 映射到 `flat_weight=0`。P3b-2
完成后可新增 `SHORT` 状态及 `short_weight<0`，不得在首版用负权重绕过做空账务门禁。

#### 2.7.2 信号和时间语义

对交易日 `T`，使用截至 T 日决策截止时点可见的**前复权收盘价**计算：

```text
MA_n(T) = mean(adjusted_close[T-n+1 : T])
state(T) = LONG  if MA_short(T) > MA_long(T)
           FLAT  otherwise
```

- `short_window_sessions`、`long_window_sessions` 都按交易日计数，且必须满足
  `2 ≤ short_window_sessions < long_window_sessions`；相等时明确为 FLAT。
- 必须有连续 `long_window_sessions` 个有效价格才产生首个状态；停牌日是否有有效收盘价由
  Canonical 行情契约决定，策略不得自行前向填充。窗口不足或价格无效时输出 `INVALID` 原因并且不下单。
- 信号用 **as-of T 的前复权价**消除拆分、分红等机械跳变：调整因子只能消费截至 T 已可见的
  公司行为，禁止使用“以回测结束日为基准”的整段前复权序列回写历史。成交与账户估值仍使用
  T+1 的未复权市场价格。
- T 日收盘数据形成状态后，最早在 `execute_date=T+1` 撮合，禁止按 T 日收盘价成交。
- `state_changed` 与上一个**有效决策日状态**比较。首个有效状态为 LONG 时视为变化并建仓；
  首个有效状态为 FLAT 时不产生空操作。无效日不更新前态。

#### 2.7.3 调仓与失败恢复

正常情况下只在 `state_changed=true` 时建立新的目标暴露，避免每天因价格漂移重复调仓。若订单因
T+1 可卖、停牌、涨跌停、容量或部分成交未完成，策略保存本次目标状态，并在后续决策日仅对剩余差额
续单，直到达到 `target_tolerance`、状态再次变化或运行结束。已达到目标后不做日常漂移再平衡。
该待完成目标属于单次 Run 的确定性状态，可由此前信号和执行结果重建；Worker 重试不得依赖进程内
未持久化对象而产生不同订单。

状态改变与订单结果是两个不同事实：`state_changed` 只描述信号，拒单、部分成交和续单原因进入执行
产物，不能回写或篡改均线状态。

#### 2.7.4 严格配置

```yaml
strategy:
  strategy_id: dual_ma_trend
  params:
    instrument_id: 510300.SH
    price_field: adjusted_close       # 固定字面量，不允许改为未复权价
    short_window_sessions: 20
    long_window_sessions: 120
    long_weight: 1.0                  # (0, 1]
    flat_weight: 0.0                  # 首版固定为 0
    target_tolerance: 0.005           # [0, 0.1]
    retry_unfilled: true
```

配置对象拒绝额外字段。`instrument_id` 必须是 Canonical 证券标识；策略依赖声明至少包含
`adjusted_bars`、`bars`、`security_status` 和 `trading_calendar`。窗口、权重和标的共同进入 Run 的
冻结配置；参数搜索只能在 TRAIN/VALIDATION 中比较，TEST 不参与均线窗口或权重选择。

#### 2.7.5 产物、分析与验收

运行除通用订单、成交、账户和绩效产物外，还输出按决策日稳定排序的信号明细：

```text
signal_date, execute_date, instrument_id,
short_ma, long_ma, state, previous_state, state_changed,
target_weight, is_valid, invalid_reason
```

分析至少覆盖：LONG/FLAT 分状态收益、状态持续期、交叉次数、持仓率、换手、未成交恢复、费用拖累、
参数稳定性和相对基准表现。验收使用字面量价格序列锁定首个有效日、金叉/死叉日、相等为 FLAT、
无效窗口、T/T+1 分离、首次 LONG 建仓、死叉清仓、部分成交续单，以及 TEST 未参与参数选择。

### 2.8 错误码

```text
STRATEGY_CAPABILITY_UNAVAILABLE   策略声明的数据依赖不满足
PIPELINE_MODEL_UNAVAILABLE        选定模型缺依赖（如 MVO 要求非退化风险模型）
```

（成本一致性错误码 `COST_MODEL_INCONSISTENT` 见 `§3`。）

### 2.9 包结构

```text
src/quant_research/
├── strategies/
│   ├── base.py           # Strategy 协议 + StrategySpec + DecisionContext + OrderIntent
│   ├── registry.py       # StrategyRegistry
│   ├── weight_target.py  # WeightTargetStrategy 基类（权重→订单）
│   ├── cross_sectional.py# CrossSectionalStrategy（A 的五模块底座）
│   └── builtins/         # etf_rotation / stock_multifactor / dual_ma_trend / pairs / event_driven
├── alpha/ risk/ costs/   # 五模块能力包之三 + 各自注册表
└── portfolio/            # ConstructionModel + ConstraintSet + 注册表
```

### 2.10 依赖方向

```text
experiments → strategies → {alpha,risk,costs,portfolio} → {data,factors,backtest}
```

策略与模块只经 `ResearchDataRepository` 等只读端口取数；不导入接口层或组合根。

### 2.11 测试契约

- **扩展性（核心诉求）**：新增一个 `Strategy` 插件无需改 runner/引擎即可跑通（最小 stub 策略测）。
- **PIT**：`DecisionContext` 物理只暴露 ≤ 决策时点数据；RiskModel/CostModel 估计窗口不越界。
- **五模块**：MultiFactorComposite 固定 MAD→zscore→方向→类别聚合、复用因子层 transform；oracle。
- **权重翻译**：差额订单、整手取整、负权重→空头、清仓路径。
- **双均线**：字面量价格 oracle 锁定窗口、金叉/死叉、相等为 FLAT、`state_changed`、首次有效状态、
  无效数据不推进状态、T+1 成交和部分成交续单。
- **回归黄金结果**：etf\_rotation / stock\_multifactor / dual\_ma\_trend 固定小样本锁定输出。

### 2.12 完成定义

> 写一个新策略只需实现 `Strategy.on_event` 并注册（或对截面范式写一份组合五模块的配置），
> 不改 runner / 回测引擎 / 基础设施即可跑通、出绩效、并排比较；四类范式可表达（依赖做空的腿随 P3b-2）；
> 事前成本与回测实际成本可对账（见 `§3`）。

***

## 3. 回测层设计（订单级引擎 / 多空账务）

### 3.1 定位

回测层按交易日推进时间轴，消费策略产出的 `OrderIntent`，负责**撮合 + 账务 + A 股规则 + 估值**。
它不产生信号（策略层）、不算因子（因子层）。设计优先级：回测可信 > 确定性 > 性能。

### 3.2 边界

**输入**：

- `BacktestRequest`：`start_date, end_date, benchmark, initial_cash_fen, execution_config`
  （`reference_price, slippage_bps, max_volume_participation, limit_fill_policy`）。
- `Strategy` 实例（引擎逐日回调 `on_event` 取 `OrderIntent`）。
- `ResearchDataRepository`：逐日 `MarketSlice`（未复权 OHLCV/preclose、is\_suspended、instrument\_type、
  board、security\_status）、交易日历、`corporate_actions`。
- `MarketRuleBook`（从 `configs/rules/a_share.yaml` 加载：涨跌幅/费率/融券费/保证金比例）。

**输出**：

- 逐日产物表：`nav`（cash/多头市值/equity\_fen/benchmark\_close；\[P3b-2] 增空头市值/保证金占用列）、
  `holdings`（逐标的头寸/可卖/成本/市值）、`fills`（成交与拒绝 + reason\_code）、
  `costs`（佣金/印花税/过户费；\[P3b-2] 增融券费）。
- `AccountSnapshot` 序列 + ledger 事件流（P3 `OPENING_CASH/BUY/SELL`；\[P3b-1] 增 `DIVIDEND`；
  \[P3b-2] 增 `SHORT_OPEN/SHORT_COVER/BORROW_FEE/MARGIN_*`）。
- 撮合失败原因码；账务不变量违背或成本不一致时抛 `COST_MODEL_INCONSISTENT` 等结构化错误。

**不输出**：绩效指标（分析层由本层产物计算）、信号/因子。

### 3.3 订单级驱动

引擎的驱动输入是 `OrderIntent`——**只带整数** **`quantity`，不含权重**（理由见 `§2.3`）。
目标权重经 `RebalancePlanner` 在策略基类翻译成整数股数订单，权重不进入引擎输入。

### 3.4 时间语义与频率

- **日频驱动**：引擎按交易日推进，T 日决策、T+1 起可成交（T/T+1 严格分离）。使用 T 日收盘信息时，
  不得默认按 T 日收盘价成交。
- 订单级接口不绑定日频：未来要日内（分钟/Tick）只需引擎支持 session 内多时点驱动，
  策略 `on_event` 契约不变。本期日频。

### 3.5 分阶段：首版纯多头，公司行为与做空后置

首版不一次吃下全部复杂度。三段推进（详见[总体设计](design.md) `§12`）：

- **P3 纯多头无公司行为**：BUY/SELL、T+1、涨跌停、停牌、费用、滑点、容量、整手/碎股、未复权撮合、
  基础 cash/position/equity。空头订单被引擎拒绝（`SHORT_NOT_SUPPORTED`）。
- **P3b-1 公司行为**：`corporate_action` ledger——现金分红入账、送转调股。
- **P3b-2 做空**：负头寸、保证金/维持保证金、融券费、空头逐日 mark、多空分腿归因。

接口一次性预留（`OrderSide.SHORT_*`、`AccountView.available_margin_fen`），按阶段补实现，上层契约不变。

### 3.6 账务：ledger 为事实来源，equity 统一公式（无双算）

**统一 equity 公式**（贯穿各阶段，避免"空头负债已 mark 又加浮盈亏"的双算）：

```text
equity = cash + long_market_value − short_market_value − accrued_fees
```

- `long_market_value` = Σ 多头数量 × 当日 close。
- `short_market_value` = Σ |空头数量| × 当日 close（欠券方负债，**\[P3b-2]**；无空头时为 0）。
- `accrued_fees` = 已计提未结算费用（含 **\[P3b-2]** 融券费）。
- **保证金是 cash 的占用/约束，不计入 equity**；空头盈亏已隐含在 `cash`（含开仓所得）与
  `short_market_value`（按现价）的差额中，**不再单列浮盈亏**。

**P3（多头）**：`positions ≥ 0`，`short_market_value = 0`；ledger 只用 `OPENING_CASH / BUY / SELL`；
多头按 lot（T+1 可卖、FIFO 成本）；整数分；每日 ledger-vs-头寸双向对账。

**\[P3b-1] 公司行为**：`DIVIDEND` ledger 按 `ex_date` 派发现金红利；送转按 `share_ratio` 调整 lot 股数
（成本不变、摊薄单位成本）。使 NAV 在除权除息日不因未复权价跳水而失真。

**\[P3b-2] 做空**：净头寸可正可负；空头按开仓均价 + 借券成本计提；可用资金/保证金占用/维持保证金；
开空占用保证金、逐日 mark；ledger 增 `SHORT_OPEN / SHORT_COVER / BORROW_FEE / MARGIN_*`；
equity 起 `− short_market_value` 项。

### 3.7 A 股撮合约束（配置化）

从 `configs/rules/a_share.yaml` 加载 `MarketRuleBook`，撮合覆盖：

- T+1 可卖；按证券/板块/日期的涨跌幅（主板 ±10%、创业板/科创板 ±20%、ST ±5%、新股无限制）。
- 停牌不可成交；涨停买入失败、跌停卖出失败（口径见下方决策点）。
- 整手买入、碎股卖出；成交量参与率容量限制；部分成交与完全无法成交。
- 上市/退市/风险状态；佣金（含最低佣金）/印花税/过户费；固定或比例滑点。（做空保证金与融券费为 P3b-2。）

**决策点（涨跌停口径）**：`ExecutionConfig.limit_fill_policy ∈ {WHOLE_DAY_SEALED, REFERENCE_AT_LIMIT}`，
**默认** **`REFERENCE_AT_LIMIT`（保守，符合"回测可信/避免过度乐观"）**；`WHOLE_DAY_SEALED` 更宽松
（仅全天封死才拒绝）。

### 3.8 撮合价与复权口径

- 撮合用**未复权价**（真实成交价）+ 公司行为事件补偿账务。
- 因子/信号侧用**前复权序列**。二者口径分离，不可混用。

### 3.9 交易成本双角色一致性（硬约束）

- **事前成本**（策略层 CostModel）：优化器/权重翻译时决定交易量。
- **事后成本**（本层撮合的费用 + 滑点 + 借券费）：实际扣减账户。
- 二者**必须共享费率参数**，否则策略对着与实际脱节的成本模型下单。一致性测试：同一笔成交，
  事前估计与事后实际在同参数下逐项对账；不一致抛 `COST_MODEL_INCONSISTENT`。

### 3.10 输出产物

逐日：`nav`（cash/多头市值/空头负债/保证金占用/nav/benchmark\_close）、`holdings`（逐标的净头寸/
可卖/成本/市值）、`fills`（成交与拒绝 + reason\_code）、`costs`（佣金/印花税/过户费/融券费）。
供分析层消费。

### 3.11 包结构

```text
src/quant_research/backtest/
├── engine.py       # 逐日主循环
├── execution.py    # ExecutionModel 撮合
├── accounting.py   # PortfolioAccount 多空账务 + ledger
├── rulebook.py     # MarketRuleBook 规则/费率
├── calendar.py     # 交易日历
└── models.py       # OrderIntent/AccountView/AccountSnapshot 等 DTO
```

### 3.12 依赖方向

回测层被 `strategies` 与 `experiments` 依赖；自身只经 `ResearchDataRepository` 取行情/状态/公司行为，
不导入接口层或组合根。

### 3.13 测试契约

- **账务不变量**：P3 `equity = cash + long_market_value − accrued_fees`；ledger-vs-头寸双向对账。
  (\[P3b-2] `equity = cash + long_market_value − short_market_value − accrued_fees`，验证无双算、保证金不进 equity。)
- **A 股约束**：T+1、涨跌停（两种 policy）、停牌、整手/碎股、容量、部分成交、费用，各字面量 oracle。
- **口径分离**：撮合用未复权价、信号用前复权价。
- **订单契约**：`OrderIntent` 无权重字段；权重经 `RebalancePlanner` 翻译成整数股数。
- **做空拒绝（P3b-2 前）**：`SHORT_OPEN/SHORT_COVER` 被拒 `SHORT_NOT_SUPPORTED`。
- **\[P3b-1]**：现金分红入账、送转调股，除权日 NAV 不跳水，各字面量 oracle。
- **\[P3b-2]**：做空开平、保证金占用/释放、借券费按天计提、空头逐日 mark、多空分腿 各字面量 oracle。
- **成本一致性**：事前/事后同参数可对账。
- **PIT**：策略 `DecisionData` 窄视图物理只暴露 ≤ 决策时点数据（无 as\_of/end 参数）。
- **确定性**：相同输入相同成交/净值序列。

### 3.14 完成定义

> 首版（P3，纯多头无公司行为）：引擎按 `OrderIntent`（整数股数）驱动，正确处理 T+1、涨跌停、停牌、
> 费用、滑点、容量、整手/碎股；`equity = cash + long_market_value − accrued_fees` 恒等式与双向对账成立；
> 撮合用未复权价、信号用前复权价；事前成本与事后成本可对账。
> P3b-1：分红送转除权除息正确（除权日 NAV 不失真）。
> P3b-2：做空/保证金/融券费/空头逐日 mark 与多空分腿在负头寸场景成立，equity 增 `− short_market_value` 项且无双算。

***

## 4. 实验层设计（编排 / 追踪 / 比较）

### 4.1 定位与范围

实验层是工作台的**编排核心**：编排"数据只读读取 → 策略产生订单 → 回测撮合账务 → 绩效分析 →
结果落盘"，并统一追踪、比较、结论标记。它调度策略层与回测层，自身不产订单、不撮合。

**本文明确不做**：

- 不保存可选择的历史 Canonical 快照，也不承诺从旧 `catalog_hash` 回放数据。
- Manifest 负责产物完整性和可信读取：逐文件记录并复核 SHA-256、字节数、行数、Schema 与排序；
  它不承担历史数据回放。
- 重跑始终复制冻结配置并创建新 Run、新任务和新产物目录，禁止覆盖历史产物。
- 不做分布式 / 多用户 / 远程 registry / model serving。

设计优先级：PIT 正确 > 回测可信 > 策略迭代低摩擦 > 结果可比较 > 性能。

### 4.2 边界

**输入**：

- 实验配置（YAML，Pydantic 严格校验）：`kind∈{STRATEGY_BACKTEST, FACTOR_STUDY}`、`name`、
  策略/因子配置、`start_date/end_date`、`benchmark`、`initial_cash_fen`、`execution`、`sample_windows`。
- 装配依赖（组合根注入）：`CanonicalResearchRepository`、`FactorEngine`、`StrategyRegistry`、
  `BacktestEngine`、`ExperimentRunRegistry`、`TaskQueue`。
- 当前 `catalog_hash`（运行开始捕获，用于运行内一致性门）。

**输出**：

- **元数据**（SQLite）：`Experiment / Run`（`config_json` 冻结快照、`status`、`catalog_hash`、
  `uses_test_region`、`research_mark`、`artifact_dir`、`error_json`）、`run_metric`、`audit_event`、
  `task/task_attempt`。
- **产物目录** `artifacts/experiments/<experiment_id>/<run_id>/`：回测 kind 输出
  `signals/orders/nav/holdings/fills/costs/performance/attribution`；因子 kind 输出
  `summary/coverage/ic/quantile_returns/long_short_returns/correlation`；+ `config.json/metrics.json/manifest.json`。
- **读侧视图**：排行榜、配置/指标 diff、血缘、结论标记（供 Dashboard/CLI）。
- 数据版本漂移/阶段失败/取消/状态冲突时抛 `EXPERIMENT_DATA_DRIFT / EXPERIMENT_STAGE_FAILED /
  EXPERIMENT_CANCELLED / EXPERIMENT_STATE_CONFLICT`。

### 4.3 追踪实体

```text
Experiment（研究命名空间 / 意图 / 标签 / 结论 / baseline 指针）
   └── Run（一次执行；记录配置快照、状态、指标、产物指针）
```

- Run 以 ULID `run_id` 为记录键；冻结 `config_json/config_hash`，产物身份由 Manifest 管理。
- **Run 记录**：`config_json`(冻结快照，便于比较)、`status`、`catalog_hash`(记录提交时数据版本，
  **用于展示与运行内一致性，不用于复现回放**)、`metrics`、`artifact_dir`、`error_json`。
- 重跑只允许生成新 Run、新任务和新目录；旧 Run、指标和产物不可覆盖。
- 统一因子研究（`FACTOR_STUDY`）与策略回测（`STRATEGY_BACKTEST`）到同一追踪主脊与比较视图。

### 4.4 状态机

```text
CREATED → QUEUED → RUNNING → SUCCEEDED
                        ├→ FAILED
                        └→ CANCELLED
```

乐观并发迁移（携期望前态，CAS）；`SUCCEEDED` 前产物必须已落地；`FAILED` 只存结构化错误码 +
安全上下文；一个 Run 只绑一个任务，重试建新 Run + 新任务。

### 4.5 编排：kind 无关的阶段执行器

```text
STRATEGY_BACKTEST: VALIDATE → PREPARE_INPUTS → STRATEGY_RUN(交织 BACKTEST) → ANALYTICS → PERSIST
FACTOR_STUDY:      VALIDATE → PREPARE_INPUTS → ANALYZE_FACTORS → PERSIST
```

- 截面策略在 `PREPARE_INPUTS` 内先算股票池/因子/管线信号；择时/事件驱动此阶段可能只 warmup。
  差异由 `Strategy.spec` 声明的数据依赖驱动，runner 本身 kind 无关。
- **STRATEGY\_RUN 与 BACKTEST 交织**：引擎按交易日推进，每个决策时点调 `strategy.on_event(ctx)`
  取订单；`ctx` 只暴露 PIT 可见信息（防未来函数的物理边界，由回测层保证）。
- 每阶段边界检查协作取消；失败/取消不留半成品目录。

### 4.6 运行内一致性门（非复现）

运行开始记录当前 `catalog_hash`；每阶段前后校验未被并发更新，变则 `EXPERIMENT_DATA_DRIFT` 失败。
作用是"这次运行不混用两批数据"，不是复现回放（不存历史版本、不回放）。

### 4.7 产物与登记

产物目录 `artifacts/experiments/<experiment_id>/<run_id>/`：同文件系统 staging → 写固定产物 →
生成 Manifest → 原子 `rename` → 从最终目录复核路径、SHA-256、字节数、行数、Schema、排序和输入身份。
任何失败都清理未登记目录；`PERSIST` 成功后才 `RUNNING→SUCCEEDED` 并写 `artifact_dir`。

### 4.8 分析层（详见[总体设计](design.md) `§8`）

`ANALYTICS` 阶段调用分析层，从回测产物（nav/holdings/fills/costs）计算绩效（累计/年化收益、波动、
Sharpe/Sortino/Calmar、最大回撤与恢复、IR、beta/alpha）、交易质量、风险与暴露、归因（期间/风格/个股；
多空分腿与 gross/net 敞口为 P3b-2）。因子研究则计算覆盖率/IC/分层/多空/相关/显著性
（见[总体设计](design.md) `§5.7-5.8`）。全部统计公式字面量 oracle；首日 0 收益口径明确
（见[总体设计](design.md) `§8.4`、[实现级细化](implemention.md) `§5.8`）。

### 4.9 防过拟合治理（保留）

- 样本区间锁定：配置声明 `train/validation/test`；Run 提交计算 `uses_test_region`；
  Experiment 累计 test 预算消耗并在 Dashboard 显式展示（样本外偷看可见可审计）。
- 多重检验记账：Experiment 记录尝试 Run 数、参数组合数、校正方法（Bonferroni/BH-FDR）；
  显著性报告据此校正。
- 不硬阻断超预算 Run（保留研究灵活性），只做可见可审计。

### 4.10 比较 / 血缘

- 排行榜：同 Experiment 按指定 metric 排序 Run。
- 配置 diff：两 Run 的 `config_json` 结构化差异。
- 血缘：Run → `catalog_hash`（数据版本，展示用）→ 产物目录，追溯到唯一 Run。
- 结论标记：`research_mark` = UNREVIEWED/BASELINE/CANDIDATE/DISCARDED；`baseline_run_id` 指精确 Run，不回退所属实验最新 Run。

### 4.11 错误码

```text
EXPERIMENT_DATA_DRIFT     运行内数据版本被并发改动
EXPERIMENT_STAGE_FAILED   阶段执行失败（含内层原因码）
EXPERIMENT_CANCELLED      协作取消
EXPERIMENT_STATE_CONFLICT 乐观并发状态冲突
```

### 4.12 包结构

```text
src/quant_research/experiments/
├── models.py      # Experiment/Run、治理、两种判别式 RunConfig、阶段图
├── config.py      # 严格实验与 Run YAML 解析、规范化和配置哈希
├── runner.py      # 唯一 EXPERIMENT_RUN handler 与 CAS 生命周期
└── statistics.py  # Bonferroni / BH-FDR 多重检验校正

application/experiments.py                           # 创建、派生、重跑、标记和查询用例
infrastructure/persistence/experiment_runs.py        # SQLite Registry
bootstrap/worker.py                                  # 策略/因子执行器组合根
```

### 4.13 依赖方向

```text
bootstrap → application → experiments → strategies → {alpha,risk,costs,portfolio} → {data,factors,backtest}
```

实验层经只读端口取数与调用引擎；不导入接口层或组合根。

### 4.14 决策记录

| # | 决策                       | 结论                        |
| - | ------------------------ | ------------------------- |
| A | 防过拟合护栏（test 预算 + 多重检验记账） | **保留**，服务"结论可信"，非复现       |
| B | 频率                       | 仅日频，订单接口已为日内预留            |
| C | 模块建设节奏                   | 渐进：先端口+退化实现打通，再补 MVO/风险模型 |
| D | 重跑语义                     | 创建新 Run、新任务和新目录，禁止覆盖历史       |

### 4.15 测试契约

- **状态机**：合法迁移通过、非法迁移 `EXPERIMENT_STATE_CONFLICT`、并发 CAS 不覆盖。
- **一致性门**：运行中 `catalog_hash` 变化 → `EXPERIMENT_DATA_DRIFT`；前后双校验。
- **阶段**：失败/取消不留半成品；`SUCCEEDED` 前产物已落地。
- **指标 oracle**：Sharpe/Sortino/Calmar/回撤/IR/beta；首日 0 口径；undefined 显式记录。
- **治理**：`uses_test_region` 标记、test 预算计数、多重检验记账。
- **kind 复用**：FACTOR\_STUDY 与 STRATEGY\_BACKTEST 共用同一 runner。

### 4.16 完成定义

> 因子研究与策略回测共享同一 `Experiment→Run` 追踪主脊与比较视图；每阶段前后校验数据版本
> （运行内一致性）；产物原子发布、`SUCCEEDED` 前落地；绩效指标字面量 oracle；样本外使用与
> 试验次数可审计。

***

## 5. 接口与 Schema 契约

本章给出三层跨包公开契约。数据接口见[总体设计](design.md) `§11.2`，因子接口见
[总体设计](design.md) `§11.3`；实现可以调整内部结构，但不得改变下列输入输出语义。

### 5.1 策略层（B：统一契约）

> 契约预留做空：`OrderSide` 含 `SHORT_*`、`AccountView` 含 `available_margin_fen`、`LedgerEventType`
> 含 `SHORT_*/BORROW_FEE/MARGIN_*`。首版（P3）引擎拒绝空头订单（`SHORT_NOT_SUPPORTED`），
> 做空账务在 **P3b-2** 实现——契约不变，只补实现（见[总体设计](design.md) `§12`）。

```python
class OrderSide(StrEnum): BUY=...; SELL=...; SHORT_OPEN=...; SHORT_COVER=...   # SHORT_* 首版拒绝，P3b 启用

@dataclass(frozen=True, slots=True)
class OrderIntent:
    """回测引擎唯一消费的订单意图——只带整数股数，不含权重。"""
    instrument_id: InstrumentId; side: OrderSide
    quantity: int                        # 正整数股数（唯一表达）
    reason: str = ""

@dataclass(frozen=True, slots=True)
class TargetWeights:
    """截面/组合类策略的输出：目标权重（和 ≤ 1，余额为现金）。不是引擎输入。"""
    signal_date: date; execute_date: date
    weights: Mapping[InstrumentId, float]     # 正=多；[P3b-2] 负=空

class RebalancePlanner(Protocol):
    """把 TargetWeights 翻译成整数股数 OrderIntent（差额、整手、负权重→空头[P3b]）。"""
    def plan(self, targets: TargetWeights, account: "AccountView",
             ref_prices: Mapping[InstrumentId, float]) -> Sequence[OrderIntent]: ...

class DecisionData(Protocol):
    """绑定到 signal_date 的只读窄视图：方法签名不含 as_of/end，未来数据在类型上取不到。"""
    def bars(self, instruments: Sequence[InstrumentId], lookback_sessions: int) -> pl.LazyFrame: ...
    def adjusted_bars(self, instruments: Sequence[InstrumentId], lookback_sessions: int) -> pl.LazyFrame: ...
    def log_returns(self, instruments: Sequence[InstrumentId], lookback_sessions: int) -> pl.LazyFrame: ...
    def daily_basics(self, instruments: Sequence[InstrumentId], lookback_sessions: int) -> pl.LazyFrame: ...
    def factor_values(self, factor_ids: Sequence[str], instruments: Sequence[InstrumentId]) -> pl.LazyFrame: ...
    def industry(self, instruments: Sequence[InstrumentId]) -> pl.LazyFrame: ...
    def security_status(self, instruments: Sequence[InstrumentId]) -> pl.LazyFrame: ...
    def stock_universe(self) -> pl.LazyFrame: ...
    # 全部返回截止到本视图 signal_date 且 pit_usable 的数据；签名无任何参数可指定更晚日期。

@dataclass(frozen=True, slots=True)
class DecisionContext:
    """物理 PIT 边界：data 是绑定 signal_date 的窄视图，策略无法请求未来数据。"""
    signal_date: date; execute_date: date
    data: DecisionData                          # signal_date 绑定的只读窄视图（非通用 Repository）
    account: "AccountView"                      # 现金/持仓/可用保证金

@dataclass(frozen=True, slots=True)
class StrategySpec:
    strategy_id: str; frequency: str
    data_dependencies: tuple[DatasetKind, ...]
    factor_dependencies: tuple[str, ...]
    parameters: Mapping[str, JsonValue]

class Strategy(Protocol):
    @property
    def spec(self) -> StrategySpec: ...
    def warmup(self, ctx: DecisionContext) -> None: ...
    def on_event(self, ctx: DecisionContext) -> Sequence[OrderIntent]: ...

class WeightTargetStrategy(Strategy):
    """基类：子类实现 target_weights；基类经 RebalancePlanner 翻译成 OrderIntent。"""
    def target_weights(self, ctx: DecisionContext) -> TargetWeights: ...   # 子类实现
    # on_event = planner.plan(target_weights(ctx), ctx.account, ref_prices)

class StrategyRegistry:
    def register(self, factory: Callable[[Mapping[str, JsonValue]], Strategy], *, strategy_id: str) -> None: ...
    def build(self, strategy_id: str, params: Mapping[str, JsonValue]) -> Strategy: ...
```

基类：`WeightTargetStrategy(Strategy)` 的 `on_event` = `target_weights(ctx) -> TargetWeights`
→ `RebalancePlanner.plan(...)` 翻译成整数股数 `OrderIntent`。子类只实现 `target_weights`；
**权重不进入引擎输入**（引擎只消费 `OrderIntent.quantity`）。

### 5.2 策略层（A：截面五模块）

```python
class AlphaModel(Protocol):
    def expected_returns(self, ctx: DecisionContext, universe: Sequence[InstrumentId]) -> pl.DataFrame: ...
    # 列: instrument_id, expected_return|score, is_valid, reason_code

class RiskModel(Protocol):
    def covariance(self, ctx: DecisionContext, universe: Sequence[InstrumentId]) -> "CovarianceEstimate": ...
    # NoRisk 退化: 返回单位/对角占位，优化器据此退化为纯打分

class TransactionCostModel(Protocol):
    def estimate(self, trades: pl.DataFrame, ctx: DecisionContext) -> pl.DataFrame: ...
    # 事前成本; 费率参数必须与回测 rulebook 同源（见 §5.4 一致性）

class ConstraintSet(Protocol):
    def apply(self, weights: pl.DataFrame, ctx: DecisionContext) -> pl.DataFrame: ...
    def validate(self, weights: pl.DataFrame) -> None: ...   # 构建后二次校验

class PortfolioConstructionModel(Protocol):
    def construct(self, alpha, risk, cost, constraints, ctx: DecisionContext,
                  current: "AccountView") -> Mapping[InstrumentId, float]: ...  # target_weights

@dataclass(frozen=True, slots=True)
class StrategyPipeline:
    alpha: AlphaModel; risk: RiskModel; cost: TransactionCostModel
    construction: PortfolioConstructionModel; constraints: ConstraintSet
```

每类模块一注册表 `model_id → 实现`。`CrossSectionalStrategy(WeightTargetStrategy)` 持有
`StrategyPipeline`，`target_weights = construction.construct(...)`。

内置实现（首批）：AlphaModel: `single_factor`/`multi_factor_composite`；RiskModel: `none`/`sample_cov`/`shrinkage`；
CostModel: `fixed_bps`/`linear_impact`；Construction: `top_n_equal_weight`/`mean_variance`；
ConstraintSet: 由 YAML 声明的通用约束集合。

### 5.3 回测引擎与账务

> 分阶段（见[总体设计](design.md) `§12`）：
>
> - **P3 纯多头无公司行为**：`positions` 恒 ≥0；ledger 只用 `OPENING_CASH/BUY/SELL`；
>   `equity = cash + long_market_value − accrued_fees`。
> - **\[P3b-1] 公司行为**：`DIVIDEND` ledger（现金分红）+ 送转股数调整。
> - **\[P3b-2] 做空**：负头寸、`available_margin_fen`、`SHORT_*/BORROW_FEE/MARGIN_*`、`borrow_fee`；
>   `equity` 增 `− short_market_value` 项。
>   接口一次性预留全部字段，标注 \[P3b-1]/\[P3b-2] 的按阶段实现，上层契约不变。

```python
@dataclass(frozen=True, slots=True)
class AccountView:
    cash_fen: int
    positions: Mapping[InstrumentId, int]        # 正=多；[P3b-2] 负=空
    sellable: Mapping[InstrumentId, int]
    available_margin_fen: int                     # [P3b-2]

class LedgerEventType(StrEnum):
    OPENING_CASH=...; BUY=...; SELL=...
    DIVIDEND=...                                                              # [P3b-1]
    SHORT_OPEN=...; SHORT_COVER=...; BORROW_FEE=...; MARGIN_POST=...; MARGIN_RELEASE=...  # [P3b-2]

@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    trade_date: date; cash_fen: int
    positions: tuple["PositionSnapshot", ...]
    long_market_value_fen: int
    short_market_value_fen: int                   # [P3b-2]，空头按当日 close 计的市值（负债）
    accrued_fees_fen: int                          # 已计提未结算费用（含 [P3b-2] 融券费）
    margin_used_fen: int                           # [P3b-2]，保证金占用（约束，不计入 equity）
    equity_fen: int
    # 不变量（统一，无双算）：
    #   P3   : equity_fen == cash_fen + long_market_value_fen − accrued_fees_fen
    #   P3b-2: equity_fen == cash_fen + long_market_value_fen − short_market_value_fen − accrued_fees_fen
    # 说明：空头盈亏已隐含在 (cash 含开仓所得) 与 (short_market_value 按现价) 的差额，不再单列浮盈亏；
    #      保证金是 cash 的占用/约束，不直接增加 equity。

class MarketRuleBook(Protocol):
    def price_limits(self, profile, trade_date, preclose, status) -> "PriceBand | None": ...
    def fees(self, fill, profile) -> "FeeBreakdown": ...
    def borrow_fee(self, short_position, days) -> int: ...      # [P3b-2] 融券成本(分)
    @property
    def content_hash(self) -> str: ...

class ExecutionModel(Protocol):
    def execute(self, intents: Sequence[OrderIntent], market, account: AccountView,
                rulebook: MarketRuleBook, config) -> "ExecutionBatch": ...

class BacktestEngine:
    def run(self, request: "BacktestRequest", strategy: Strategy,
            progress, cancellation) -> "BacktestResult": ...
    # 内部: 逐交易日 → strategy.on_event(DecisionContext) → execute(只认 OrderIntent.quantity)
    #      → account.apply → [P3b-1] 按 corporate_action 派发 DIVIDEND/调整股数 → mark_to_market
```

引擎逐日：先按当日 `corporate_action` 处理持仓公司行为（现金红利入账、送转调股），再撮合当日待执行
订单，再 mark-to-market。撮合价用未复权价；因子/信号侧用前复权序列。

### 5.4 成本双角色一致性

`TransactionCostModel`（事前）与 `MarketRuleBook.fees/borrow_fee`（事后）**必须由同一费率配置构造**。
一致性测试：同一笔成交，事前估计与事后实际的费用项在同参数下逐项对账；不一致抛
`COST_MODEL_INCONSISTENT`。

### 5.5 实验层

```python
class ExperimentKind(StrEnum): FACTOR_STUDY=...; STRATEGY_BACKTEST=...
class RunStatus(StrEnum): CREATED=...; QUEUED=...; RUNNING=...; SUCCEEDED=...; FAILED=...; CANCELLED=...

class ExperimentRunRegistry(Protocol):   # 消费者侧持久化端口
    def create_experiment(self, name: str, kind: ExperimentKind, config: Mapping) -> str: ...
    def create_run(self, experiment_id: str, config_snapshot: Mapping, catalog_hash: str) -> str: ...
    def transition(self, run_id: str, expected: RunStatus, target: RunStatus, **terminal) -> None: ...
    def record_metrics(self, run_id: str, metrics: Mapping[str, float]) -> None: ...
    def get_run(self, run_id: str) -> Mapping[str, object]: ...
    def list_runs(self, experiment_id: str) -> Sequence[Mapping[str, object]]: ...

class StageGraph(Protocol):
    def stages(self, kind: ExperimentKind) -> tuple["Stage", ...]: ...

class ExperimentRunner:
    def run(self, run_id: str, progress, cancellation) -> "RunResult": ...
    # 策略阶段: VALIDATE → PREPARE_INPUTS → STRATEGY_RUN → ANALYTICS → PERSIST
    # 因子阶段: VALIDATE → PREPARE_INPUTS → ANALYZE_FACTORS → PERSIST
    # 每阶段前后校验 catalog_hash 未变(运行内一致性)，变则 EXPERIMENT_DATA_DRIFT
```

#### 5.5.1 SQLite 表

```text
experiment(id PK, name, kind, description, definition_json, definition_hash,
           baseline_run_id NULL, created_at)
experiment_tag(experiment_id FK, tag)
run(id PK, experiment_id FK, status, config_json, catalog_hash,
    config_hash, task_id, stage, uses_test_region, research_mark,
    artifact_dir NULL, manifest_hash NULL,
    created_at, queued_at, started_at, completed_at, error_json NULL)
run_metric(id PK, run_id FK, name, value, unit, created_at)
run_tag(run_id FK, tag)
run_artifact(id PK, run_id FK, artifact_type, relative_path, content_hash,
             byte_count, row_count, schema_json, created_at)
audit_event(id PK, run_id NULL, subject_kind, subject_id, task_id NULL,
            event_type, details_json, created_at)
task(id PK, subject_kind, subject_id, task_type, payload_json, status, priority,
     idempotency_key, worker_id, heartbeat_at, created_at, ...)
task_attempt(id PK, task_id FK, attempt_no, status, started_at, completed_at, error_json NULL)
```

产物目录 `artifacts/experiments/<experiment_id>/<run_id>/`：含 kind 对应 Parquet、
`config.json`、`metrics.json` 和可信 `manifest.json`；发布后不可覆盖。

### 5.6 严格 YAML Schema

实验定义顶层只允许：

```yaml
name: 非空名称
description: 可空说明
kind: STRATEGY_BACKTEST             # 或 FACTOR_STUDY
tags: [按字典序、无重复]
sample_windows:
  train:      {start: YYYY-MM-DD, end: YYYY-MM-DD}
  validation: {start: YYYY-MM-DD, end: YYYY-MM-DD}
  test:       {start: YYYY-MM-DD, end: YYYY-MM-DD}
governance:
  test_budget: 2
  correction: BONFERRONI             # 或 BH_FDR
initial_run: <与 kind 对应的 RunConfig>
```

三个窗口必须严格有序且互不重叠。策略 Run 只允许 `kind/start_date/end_date/strategy/benchmark/
initial_cash_fen/execution`；因子 Run 只允许 `kind/start_date/end_date/factor_study`。Run 日期必须位于协议
总区间内。所有 Pydantic 模型均 `extra=forbid, strict, frozen`，日期只接受明确 `YYYY-MM-DD`。

截面策略参数是唯一五模块流水线：

```yaml
strategy:
  strategy_id: stock_multifactor
  parameters:
    pipeline:
      frequency: WEEKLY
      target_tolerance: 0.001
      alpha: {model_id: multi_factor_composite, params: {factor_weights: {book_to_price_mrq: 0.5, momentum_120_20: 0.5}, min_valid_factors: 2}}
      risk: {model_id: none}
      cost: {model_id: fixed_bps}
      construction: {model_id: top_n_equal_weight, params: {top_n: 20}}
      constraints: {model_id: long_only, params: {min_positions: 10, max_positions: 20, max_position_weight: 0.05, max_turnover: 0.4, max_industry_weight: 0.3, min_adv_amount: 50000000.0, long_exposure: 1.0}}
```

`mean_variance` 要求 `sample_cov|shrinkage` 风险模型；`none + mean_variance` 在提交前以
`PIPELINE_MODEL_UNAVAILABLE` 拒绝。双均线是独立插件参数：`instrument_id/short_window/long_window/
long_weight/target_tolerance`，不伪装成 Alpha 因子组合。

可直接提交的完整配置：

- [股票多因子](../../configs/experiments/examples/multifactor.yaml)
- [ETF 轮动](../../configs/experiments/examples/etf_rotation.yaml)
- [双均线趋势](../../configs/experiments/examples/dual_ma_trend.yaml)
- [因子研究](../../configs/experiments/examples/factor_study.yaml)

### 5.7 CLI 与 HTTP

CLI：

```text
quant experiments validate <yaml>
quant experiments submit <yaml>
quant experiments run <experiment_id> <run-yaml>
quant experiments rerun <run_id>
quant experiments list
quant experiments show <experiment_id>
quant strategies list
```

HTTP：

```text
GET  /api/v1/strategies
POST /api/v1/experiments/validate
POST /api/v1/experiments
GET  /api/v1/experiments
GET  /api/v1/experiments/{experiment_id}
POST /api/v1/experiments/{experiment_id}/runs
POST /api/v1/runs/{run_id}/rerun
PATCH /api/v1/runs/{run_id}/research
POST /api/v1/experiments/compare
GET  /api/v1/runs/{run_id}/artifacts/{artifact_type}?page=1&page_size=100
```

产物查询只接受白名单类型，从登记 Run 的可信 Manifest 解析相对路径，并重新校验 Manifest 与文件哈希、
字节数、行数、Schema、主键和排序；接口不接受任意文件路径。

### 5.8 Dashboard

- `/experiments`：统一列表，展示 kind、最新 Run 状态、Run/TEST 使用次数和 baseline。
- `/experiments/new`：三个策略模板和因子模板；后端组件 JSON Schema 是字段与能力规则的唯一来源，
  组件选择与 YAML 双向同步，提交前必须由后端规范化校验。
- `/experiments/:experimentId`：协议、Run 时间线、派生 Run、比较、baseline/mark、取消、重试以及
  信号/持仓/成交/成本/绩效或覆盖率/IC/相关性/显著性和 Manifest。
- `/tasks`：通用运行中心，以 `subject_kind=EXPERIMENT_RUN, subject_id=run_id` 关联实验任务。

旧 `/research`、`/factors`、`/api/v1/research/*`、`quant research` 和 `quant components` 不存在。

### 5.9 迁移与硬切

Alembic `0007_experiment_runs` 删除旧 `research_family*`、`research_variant`、旧 research/factor study
元数据，创建 `experiment/experiment_tag/run/run_tag/run_metric/run_artifact/audit_event` 并把任务关联统一为
`subject_kind/subject_id`。Raw、Canonical、质量、数据目录和数据任务表及数据不变。旧研究产物文件不由
迁移删除，但不再登记、读取或展示。
