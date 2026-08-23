# A 股量化策略研究工作台

文档状态：当前有效设计　·　日期：2026-08-22

本文是 A 股个人量化策略研究工作台的**总体设计入口**，按阅读顺序汇集项目诉求、需求规格、
整体架构、跨层边界与实现计划。数据层的权威详细设计见[数据层设计](data-layer-design.md)，
策略、回测与实验的权威详细设计见[策略、回测与实验设计](strategy-backtest-experiment-design.md)，
实现级细化见 `implemention.md`。未附文档链接的 `第 N 章`、`§M` 引用均指向本文。

## 目录

1. 项目主诉求与定位（对齐基准）
2. 需求规格
3. 整体架构
4. [数据层设计（独立文档）](data-layer-design.md)
5. 因子层设计
6. [策略层设计（独立文档）](strategy-backtest-experiment-design.md) `§2`
7. [回测层设计（独立文档）](strategy-backtest-experiment-design.md) `§3`
8. 分析层设计（绩效 / 风险 / 归因）
9. [实验层设计（独立文档）](strategy-backtest-experiment-design.md) `§4`
10. Worker 与任务队列设计
11. 接口与 Schema 契约（落地基准）
12. 分阶段实现计划

***

## 1. 项目主诉求与定位（对齐基准）

### 1.1 一句话定位

个人自有资金的 A 股低频量化**策略研究工作台**：核心价值是**低摩擦地快速迭代、试错、比较各种策略**，
在保证"结果当下可信"的前提下尽快看到结果。不追求特定收益率，也不提供历史 Canonical 回放；
每次 Run 的冻结配置、指标和已发布产物仍保持不可变，便于同一实验内比较。

### 1.2 诉求优先级（高 → 低）

1. **PIT 正确 / 无未来函数**：只用信号产生时真实可得的数据。用了未来信息，结论**当下就是错的**——与复现无关，第一优先级，不可让步。
2. **回测可信**：真实模拟 A 股交易制度（T+1、涨跌停、停牌、费用、滑点、容量；做空保证金），避免过度乐观。
3. **快速迭代任意策略**：加一个新策略应"只写策略逻辑本身"，不改 runner、不改回测引擎、不碰基础设施。这是决定架构形态的核心生产力诉求。
4. **数据源隔离 + 可演进**：BaoStock 可换 TuShare，不动上层；研究平台可平滑长到模拟/实盘。
5. **单人可维护**：Notebook + 核心包 + 本地 Dashboard 共享同一套逻辑；单体、单用户、本机。

### 1.3 明确放弃的诉求（不为其增加复杂度）

放弃的是**跨运行复现回放**，不是所有哈希（哈希用于去重/增量/完整性/运行内一致性仍保留；
技术边界见 `§3.4.2「哈希边界」`）：

- **跨运行复现回放**：不保留可选择的历史 catalog、不承诺旧 Run 可重新读取当时的 Canonical 数据、
  不做 source/env 指纹链或 `run_identity` 内容身份哈希。Manifest 仍逐文件记录并复核 SHA-256、字节数、
  行数、Schema、主键和排序，重跑始终创建新 Run、新任务和新目录，禁止覆盖历史产物。
- **数据层复现**：数据层重性能与去重（"只考虑不重算，不考虑复现"），不为跨运行复现付费。
- 高频 / Tick / 盘口 / 实盘下单 / 多用户 / 公网 / 分布式 / 参数寻优后直接发布 / 收益承诺。

### 1.4 关键边界与再定位

- **PIT 一致性门**：作用是"防止运行中途数据变了、导致这次结果混用两批数据而错误"，即
  **运行内一致性**，不是跨运行复现回放。
- **数据版本号（`catalog_hash`）轻量化**：只用于运行内一致性判定，不做历史版本存储与回放。
- **防过拟合治理（train/valid/test 隔离、多重检验记账）：保留**。它服务于"结论统计可信"，
  不属于复现机制，故在放弃复现的同时仍然保留。

### 1.5 策略扩展模型：A+B

平台支持"各种策略"，含**同范式微调**和**完全不同范式**两类：

- **A（策略即配置）**：一组通用可组合模块（Alpha / 风险 / 成本 / 组合构建 / 约束）+ 策略配置 schema。
  覆盖大多数迭代（换因子、换权重、加约束）。ETF 轮动、股票多因子是"用底座拼出的内置策略"，
  不是写死的特例。
- **B（策略即插件）**：`Strategy` 协议 + 注册表。给异构策略留口子，新策略 = 实现协议并注册。
- **双均线趋势属于 B**：它不是截面 Alpha 的一种因子组合，而是独立的时序状态策略；内置
  `dual_ma_trend` 插件以复权收盘价生成 LONG/FLAT 状态，并复用权重翻译、撮合、成本和分析能力。
- 二者并存：底座覆盖 \~80% 微调（配置驱动），插件口子覆盖异构范式。

**核心设计目标**：策略作者只跟稳定公开契约打交道（数据只读接口、因子、模型端口、回测引擎），
永不改 runner / 引擎 / 基础设施。

### 1.6 必须支持的策略范式（含完全不同范式）

- 截面选股 → 目标权重 → 调仓（多因子、ETF 轮动）。
- 择时 / CTA（单标的仓位方向随时间变化）。
- 配对交易（成对相对头寸，需做空）。
- 事件驱动（稀疏、按事件触发的离散订单）。

**架构含义（已定）**：回测引擎驱动接口是\*\*订单意图（order intent）\*\*层级——
策略直接产出买/卖/开平仓订单，引擎负责撮合+账务+规则。目标权重范式经"权重→订单"的策略基类
表达，多因子/ETF 作者仍可只声明目标权重、无感经过该基类。
理由：权重是订单的特例，反之不成立；这是 backtrader/zipline/vnpy 的共同选择。

### 1.7 回测分阶段：首版纯多头，公司行为与做空后置

首版不一次吃下全部复杂度。做空（负头寸+保证金+维持保证金+融券费+空头逐日 mark）和公司行为
（分红送转除权除息）都会显著放大最难的账务模块，分别后置，均**不阻塞首个可跑通闭环**：

- **架构与接口预留**：`OrderIntent.side` 含 `SHORT_OPEN/SHORT_COVER`、`AccountView` 含
  `available_margin_fen`；订单级驱动本就为四范式泛化，不因是否做空而变。
- **首版（P3）纯多头、无公司行为**：多头 lot + T+1 + FIFO + 撮合规则；空头订单被引擎显式拒绝
  （`SHORT_NOT_SUPPORTED`）；除权日 NAV 因未复权价会暂时失真（已知偏差，P3b-1 修正）。
- **P3b-1 公司行为**：现金分红入账、送转调股，使除权日 NAV 不失真。
- **P3b-2 做空**：负头寸、保证金/维持保证金、融券费、空头逐日 mark、gross/net 敞口与多空分腿归因；
  依赖 P3b-1 先就位。解锁配对纯对冲、CTA 空头腿（P3 阶段先做其多头版/观察版）。

### 1.8 已确认决策

- 防过拟合护栏（样本外 test 预算 + 多重检验记账）：**保留**（服务"结论可信"，非复现）。
- 频率：**仅日频**，订单级接口为日内预留（未来加 session 内多时点驱动，契约不变）。
- 公司行为（分红/送转/除权除息）：作为数据层一等事件，进入复权与回测账务。

***

## 2. 需求规格

### 2.1 定位

个人自有资金的 A 股低频量化**策略研究工作台**。核心价值是**低摩擦地快速迭代、试错、
比较各种策略**，并在"结果当下可信"的前提下尽快看到结果。不追求特定收益率，也不提供历史
Canonical 回放；已发布 Run 仍不可变并可审计比较。

单用户、Windows 本机、单体、本地 Dashboard。频率以日频为主（接口为日内预留但本期不实现）。

### 2.2 诉求优先级（贯穿全文的取舍依据）

1. **PIT 正确 / 无未来函数** —— 只用信号时点真实可得的数据。用未来信息，结论当下就是错的。第一位，不让步。
2. **回测可信** —— 真实模拟 A 股交易制度（T+1、涨跌停、停牌、费用、滑点、容量；做空保证金 P3b-2）。
3. **快速迭代任意策略** —— 加新策略只写策略逻辑，不改 runner / 引擎 / 基础设施。
4. **数据源隔离 + 可演进** —— BaoStock 可换 TuShare 不动上层。
5. **单人可维护** —— Notebook + 核心包 + Dashboard 共享同一核心逻辑。

### 2.3 明确放弃 / 明确纳入

#### 2.3.1 放弃（不为其增加复杂度）

放弃的是**跨运行复现回放**，不是所有哈希（哈希边界见 `§3.4.2「哈希边界」`）：

- **跨运行复现回放**：不保留可选择历史 catalog、不承诺旧 Run 可重新读取当时的 Canonical 数据、
  不做 source/env 指纹链或 `run_identity` 内容身份哈希。Run 产物使用可信 Manifest 并不可覆盖；
  重跑复制冻结配置创建新 Run。
- **数据层复现**：数据层重性能与去重，不为跨运行复现付费（"只考虑不重算，不考虑复现"）。
- 高频/Tick、实盘下单、多用户、公网、分布式、参数寻优后自动发布、收益承诺。

> 仍保留完整性/增量/运行内一致性所需的哈希：Raw `request_hash`/`content_hash`、
> Canonical partition `content_hash`、`schema_fingerprint`、`catalog_hash`、curate `input_hash`。

#### 2.3.2 纳入（按业界通用回测框架的合理做法）

- **做空 / 融券 / 多空对冲**：架构与接口预留，**首版多头、做空账务 P3b-2 实现**（不阻塞首个闭环）。
- **四类策略范式**：截面选股、择时/CTA、配对、事件驱动（配对纯对冲/CTA 空头腿随 P3b-2 解锁）。
- **订单级回测**：策略产出订单意图（只带整数股数），引擎撮合。
- **公司行为**：分红/送转/除权除息作为一等事件进入回测账务与复权，**P3b-1 实现**（首版 P3 不含，
  除权日 NAV 会暂时失真，P3b-1 修正）。

### 2.4 系统总览

```text
数据源(BaoStock/TuShare) → 数据层(Raw→Canonical→Feature, PIT+质量门)
   → 因子层(定义/计算/统计) → 策略层(Strategy 协议 + A+B 扩展)
   → 回测引擎(订单级/多空/A股约束) → 分析层(绩效/风险/归因)
   → 实验层(编排/追踪/比较) → 本地 Dashboard / Notebook
```

依赖方向单向向下；策略作者只接触稳定公开契约（数据只读接口、因子、模型端口、回测引擎）。

### 2.5 数据层需求（详见[数据层设计](data-layer-design.md)）

- **数据源隔离**：`SourceClient` 采集 + `CanonicalMapper` 规范化；上层不触供应商专有字段。
- **三层**：Raw（原样、内容寻址、不可变）→ Canonical（统一代码/日历/字段/复权入口/PIT 时点）→ Feature（因子结果）。
- **PIT 审计列**：每个 Canonical 数据集含 `source / available_at / availability_source / pit_usable / ingested_at`；财务/行业按公告与可用时间时点化。
- **公司行为**：分红送转除权除息作为 Canonical 事件数据集，服务复权与回测账务。
- **质量门**：交易日缺重、主键重复、价量缺失、OHLC 逻辑、复权跳变、状态冲突、财务时间倒置、全市场记录数异常等；严重/致命失败**关闭研究读取门**。
- **不做复现**：无数据快照版本回放；数据身份仅用于"运行内一致性"（见 §2.10）。

### 2.6 因子层需求（详见 `第 5 章`）

- 因子唯一 `factor_id`；统一输出契约 `trade_date, instrument_id, factor_id, value, available_at, is_valid`。
- 每次运行重算，不持久化跨运行缓存（与"不复现"一致，缓存只为性能且运行内有效）。
- PIT 绑定：因子值只用信号日可见输入；`available_at ≤ 信号日` 才有效。
- 统计内核（IC/RankIC/分层/多空/相关）用**字面量 oracle** 校验；**全向量化，禁止 per-(date,instrument) Python 行循环**。
- 因子研究（覆盖率、IC、分层收益、相关、稳定性）作为一种实验 kind。

### 2.7 策略层需求（A + B）

#### 2.7.1 统一契约 B（策略即插件）

```python
class Strategy(Protocol):
    @property
    def spec(self) -> StrategySpec: ...            # id/频率/数据依赖/参数
    def warmup(self, ctx) -> None: ...
    def on_event(self, ctx: DecisionContext) -> Sequence[OrderIntent]: ...
```

`on_event` 是唯一必需方法；`DecisionContext` 物理上只暴露 `≤ 决策时点` 的 PIT 数据。
经 `StrategyRegistry` 注册。四范式均可表达。

#### 2.7.2 模块化底座 A（策略即配置，截面范式）

`CrossSectionalStrategy` 由五可插拔模块组装，配置驱动、无需写代码：
AlphaModel / RiskModel / TransactionCostModel / PortfolioConstructionModel / ConstraintSet。
ETF 轮动、股票多因子退化为"底座的两个内置配置"。

#### 2.7.3 层次

```text
Strategy(Protocol)
 └ WeightTargetStrategy(权重→订单基类)
    └ CrossSectionalStrategy(五模块底座) → etf_rotation / stock_multifactor
    └ DualMATrendStrategy(时序目标暴露) → dual_ma_trend
 └ PairsStrategy / EventDrivenStrategy(直接实现 on_event)
```

### 2.8 回测引擎需求

- **订单级驱动**：策略产出 `OrderIntent`（只带整数股数），引擎撮合。目标权重经 `RebalancePlanner` 翻译，非引擎输入。
- **时间语义**：T 日决策，最早 T+1 成交；不允许用 T 日收盘信息按 T 日收盘价成交。
- **A 股约束（配置化）**：T+1 可卖、按证券/板块/日期的涨跌幅、停牌不可成交、涨停买入/跌停卖出失败、
  整手与碎股、上市退市风险状态、佣金/最低佣金/印花税/过户费、滑点模型、
  成交量容量限制、部分成交与无法成交。（分红送转除权除息见下条 P3b-1。）
- **公司行为（P3b-1）**：分红送转除权除息账务作为 P3b-1 实现；首版 P3 不含（除权日 NAV 暂失真）。
- **做空/保证金（P3b-2）**：账务模型预留净头寸可正可负；保证金占用与维持保证金、空头逐日
  mark-to-market、融券成本作为 P3b-2 实现。**首版（P3）多头**，空头订单被引擎拒绝（`SHORT_NOT_SUPPORTED`）。
- **账务（equity 统一公式，无双算）**：整数分；
  `equity = cash + long_market_value − short_market_value − accrued_fees`
  （P3 无 short\_market\_value 项）；保证金是占用不计入 equity；每日 ledger-vs-头寸双向对账。
- **输出**：净值/收益/基准/超额、回撤与恢复、持仓与权重、成交与失败原因、换手/费用/滑点、
  年月滚动绩效、风险与归因、gross/net 敞口与多空分腿。

### 2.9 分析层需求（详见 `第 8 章`）

- 绩效：累计/年化收益、波动、Sharpe/Sortino/Calmar、最大回撤与恢复、信息比率、beta/alpha。
- 成交质量：成交/失败率、换手、费用拖累、按原因码汇总。
- 归因：风格/个股/期间；多空分腿。
- 因子分析：RankIC、滚动 RankIC、分层收益、覆盖率、相关、稳定性；重叠持有期做序列相关修正（HAC/bootstrap），多重检验校正。
- 全部统计公式字面量 oracle 覆盖。

### 2.10 策略 / 回测 / 实验层需求

详细设计见[策略、回测与实验设计](strategy-backtest-experiment-design.md) `§2-§4`。

- 追踪实体：`Experiment → Run`（轻量，Run 无 run\_identity 复现哈希，但存 catalog\_version）。Run 记录冻结配置快照、状态、指标、产物指针。
- 编排：`VALIDATE → PREPARE_INPUTS → STRATEGY_RUN → ANALYTICS → PERSIST`；每个任务使用一个短生命周期执行会话。`STRATEGY_RUN` 只生成内存回测表，`PERSIST` 才一次性发布。
- **PIT 一致性门（保留、重新定位）**：运行开始记录数据版本，运行中数据被并发改动则失败——作用是"这次运行不混用两批数据"，非复现回放。
- 成本双角色一致性：事前 CostModel 与回测事后费用共享费率、可对账。
- 比较：同 Experiment 下排行榜、配置/指标 diff、结论标记（baseline/candidate/discarded）。
- 防过拟合护栏（用户确认保留）：train/valid/test 区间声明、test 预算计数、多重检验记账——服务"结论可信"，非复现。

### 2.11 Dashboard 需求

本地只读研究运营台，前后端分离：**FastAPI 后端**暴露只读 REST/JSON API（默认仅监听 127.0.0.1，
不公网），**Vue 单页前端**消费该 API 渲染；页面不执行 Python、不直连 SQLite/文件，写操作
（数据更新、跑实验、重试）经后端用例触发。功能覆盖：

- 研究就绪状态：研究门开关、阻断原因、最新交易日、数据新鲜度。
- 数据中心：Canonical 数据集覆盖与质量运行结果、更新任务。
- 策略/实验中心：按策略与标签筛选、并排比较、结论标记、从已有配置派生新 Run；
  仅终态 Run 可删除，实验包含活动 Run 时禁止删除。删除聚合时同步清理可信产物目录，
  独立任务和审计记录继续保留，任务解除已删除 Run 的主体关联。
- 回测分析：净值/回撤/年月收益/风险指标/归因/成交失败统计/多空敞口。
- 因子分析：RankIC、分层、覆盖、相关、稳定性。
- 运行中心：任务队列状态、日志下钻、幂等重试。
- Notebook 入口内嵌本机 JupyterLab。

### 2.12 非功能需求

- **性能**：20 年全市场多因子完整回测在验收机 60 分钟内；ETF 轮动 5 分钟内；Dashboard 常规查询 3 秒内。
- **PIT 与防泄漏**：`DecisionContext` 物理边界 + 专门未来函数测试。
- **可靠性**：任务幂等；关键失败不发布不完整数据；SQLite 事务；长任务后台化。
- **安全**：Dashboard 仅本机；凭据走环境变量、不入代码/日志/Git。
- **可维护**：核心模块类型标注 + 稳定公开接口；交易规则/参数配置化不散落。

### 2.13 技术栈

Python 3.12；Polars（主）+ DuckDB + Parquet；SQLite（元数据）；Pydantic（配置校验）；
Dashboard 前后端分离 —— 后端 FastAPI（REST/JSON，只读研究 API），前端 Vue（+ 图表库如
ECharts/Plotly）；JupyterLab；pytest + Ruff + mypy；前端 Vitest + ESLint。

### 2.14 验收标准

1. BaoStock 初次全量 + 增量更新，形成 Raw/Canonical/Feature 与研究门。
2. 关键质量失败阻止研究门开启。
3. 四范式各有一个可运行示例（截面：多因子/ETF；择时：`dual_ma_trend`；配对；事件驱动），
   从 Notebook/CLI 跑通并在 Dashboard 展示（双均线与事件驱动首版为多头版，配对纯对冲随 P3b-2）。
4. 回测正确处理 T+1/涨跌停/停牌/费用/滑点/容量/整手碎股（首版 P3 纯多头）；**分红送转除权除息为 P3b-1、做空/保证金/融券费为 P3b-2 验收项**。
5. **加一个新策略只需实现** **`Strategy`** **并注册（或写一份五模块配置），不改 runner/引擎/基础设施**。
6. 事前成本与回测实际成本可对账。
7. 模拟 TuShare 适配器证明上层不依赖 BaoStock 专有模型。
8. 单元（含字面量 oracle）、PIT 时点、集成、回归、性能测试通过。
9. 无未解释的严重/致命数据质量问题。
10. 20 年全市场多因子完整回测 ≤ 60 分钟。
    11.（P3b-1）公司行为账务：现金分红入账、送转调股，除权日 NAV 不失真。
    12.（P3b-2）做空账务：负头寸、保证金/维持保证金、融券费、空头逐日 mark-to-market、多空分腿归因正确，equity 无双算。

### 2.15 完成定义

> 写一个新策略只需实现 `Strategy.on_event` 并注册（或对截面范式写一份组合五模块的配置），
> 不改 runner/回测引擎/基础设施即可跑通、出绩效、在 Dashboard 与其他策略并排比较；
> 四范式可表达（多空对冲随 P3b-2）；PIT 与 A 股交易约束在引擎层强制；事前成本与回测实际成本可对账；
> 数据质量门失败时不开放研究读取。

***

## 3. 整体架构

### 3.1 分层与依赖方向

```text
                bootstrap（唯一组合根）
                     │ 装配真实依赖
        ┌────────────┼───────────────┐
        ▼            ▼                ▼
      cli        dashboard        (worker)
        └────────────┼───────────────┘
                     ▼
               application（用例编排）
                     ▼
   experiments ─► strategies ─► {alpha, risk, costs, portfolio}
        │              │                  │
        │              └────────► backtest（订单级引擎/账务/撮合）
        ▼                                 │
   analytics ◄────────────────────────────┘
        │
   factors ─► universe
        │
       data（Raw/Canonical/Feature + 只读研究仓库）
        ▲
   infrastructure（SQLite/SQLAlchemy/Parquet/供应商适配器/文件系统）
```

**铁律**（AST 门禁强制）：

- `bootstrap` 是唯一可依赖所有层的组合根。
- `cli`/`dashboard` 不导入基础设施具体实现、不互相导入。
- `application` 与能力包不导入接口层、组合根、基础设施具体类；只依赖消费者侧 Protocol。
- 能力包之间只能上层依赖下层（策略→模块→回测→因子→数据），禁止反向。
- 导入期不连网、不建线程、不扫用户数据目录。

### 3.2 关键架构决定

| 决定          | 内容                                                                                                                                                                   | 理由                          |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------- |
| D1 订单级回测    | 策略产出 `OrderIntent`，引擎撮合；权重是特例                                                                                                                                        | 支持四范式；权重无法表达配对做空/事件驱动       |
| D2 回测分阶段    | P3 纯多头无公司行为；P3b-1 公司行为（分红送转）；P3b-2 做空（负头寸+保证金+融券费）。接口一次性预留 `SHORT_*`/`available_margin`                                                                              | 两者都放大最难的账务模块，分别后置，不阻塞首个闭环   |
| D3 策略 A+B   | 插件协议 + 截面模块化底座                                                                                                                                                       | 低摩擦迭代任意策略                   |
| D4 去跨运行复现回放 | 不做 run\_identity/source-env 指纹/产物 manifest 逐文件校验/历史 catalog；**保留** Raw/Canonical content\_hash、request\_hash、schema\_fingerprint、catalog\_version 用于去重/增量/完整性/运行内一致性 | 复现回放是用户明确放弃项；但完整性与增量所需哈希不可省 |
| D5 PIT 物理边界 | `DecisionContext` 只暴露 ≤ 决策时点数据                                                                                                                                       | 防未来函数的第一道且最强防线              |
| D6 数据源隔离    | SourceClient + CanonicalMapper                                                                                                                                       | BaoStock↔TuShare 可换         |
| D7 成本双角色一致  | 事前 CostModel 与事后撮合共享费率                                                                                                                                               | 防优化器对脱节成本下单                 |

### 3.3 包结构

```text
src/quant_research/
├── domain/            # 稳定枚举、标识（InstrumentId）、值对象、错误码
├── data/              # 数据目录/schema/流水线/只读研究仓库
│   ├── sources/       #   SourceClient 协议 + BaoStock/TuShare 适配（适配器在 infra）
│   ├── canonical/     #   CanonicalMapper、schema、审计列
│   ├── corporate/     #   分红送转除权除息事件
│   ├── pipeline/      #   localize → curate → validate
│   ├── quality/       #   质量规则 + 门禁
│   ├── storage/       #   Raw 分区、可信路径、原子文件发布与校验
│   ├── catalog.py     #   数据集目录、更新策略和供应商端点能力
│   ├── contracts.py   #   跨子包不可变 DTO 与确定性 JSON
│   └── repository.py  #   CanonicalResearchRepository（只读、PIT 截断）
├── factors/           # 因子定义/注册/计算/统计内核
├── universe/          # PIT 股票池
├── alpha/ risk/ costs/# 截面五模块之三
├── portfolio/         # ConstructionModel + ConstraintSet
├── strategies/        # Strategy 协议 + 注册表 + 基类 + 内置策略
├── backtest/          # 订单级引擎 + 多空账务 + 撮合规则
├── analytics/         # 绩效/风险/归因
├── experiments/       # 编排/追踪/比较
├── tasks/             # TaskQueue 协议 + 任务模型/状态机（Worker 队列契约）
├── application/       # 用例（数据更新、跑实验）+ Worker 主循环 + TaskHandler 分派
├── cli/               # CLI 接口适配器
├── dashboard/         # Dashboard 后端：FastAPI 应用/路由/只读 DTO（不含前端）
├── infrastructure/    # SQLite/ORM/Alembic/供应商 SDK/文件系统
└── bootstrap/         # 组合根

frontend/              # Vue Dashboard 单页应用（独立构建，消费 FastAPI 只读 API）
```

### 3.4 横切关注点

#### 3.4.1 PIT（跨层第一原则）

三道防线，从物理到逻辑：

1. **数据层**：Canonical 携 `available_at` 等审计列；`CanonicalResearchRepository` 读取时按
   `as_of / signal_date` 截断，物理上不返回未来行。
2. **回测引擎**：`DecisionContext` 只包含 ≤ 决策时点可见的行情/因子/持仓。
3. **测试**：专门未来函数测试 + 财务/行业修订不回填旧信号日。

#### 3.4.2 哈希边界：去复现 ≠ 去哈希

**放弃的是"跨运行复现回放"，不是所有哈希**。哈希不是为了"半年后复现"，而是为了
**去重、增量、完整性、运行内一致性**。二者边界：

| 保留（完整性/增量/运行内一致性）                  | 用途                     |
| ---------------------------------- | ---------------------- |
| Raw `request_hash`                 | 请求身份、目录名、断点续抓去重        |
| Raw `content_hash`                 | Raw 内容身份、去重、完整性校验      |
| Canonical partition `content_hash` | 分区内容身份、完整性校验、增量判定      |
| `schema_fingerprint`               | 端点/分区契约身份，独立于数据值       |
| `catalog_hash`                  | 当前目录版本号，运行内一致性门（非历史回放） |
| curate `input_hash`                | 增量：输入未变则复用不重算          |

| 不做（跨运行复现回放）                | 理由                |
| -------------------------- | ----------------- |
| 可选择的历史 catalog / 数据快照版本    | 不回放旧数据            |
| `run_identity` 内容身份哈希      | 不需按内容身份定位/重放旧 run |
| source/env 指纹链             | 不锁定代码+环境以复现       |
| 可重放历史 Canonical 的快照 Manifest | 不提供历史数据回放        |
| source/env 完整复现链               | 不锁定旧代码和旧依赖环境    |

Run 产物的可信 Manifest 属于完整性边界，仍须逐文件复核 SHA-256、字节数、行数、Schema、主键和排序；
重跑新建 Run，不覆盖既有产物。这不等同于保存历史 Canonical 供回放。

一句话：**内容哈希用于"这一份数据/这一次运行"的正确与高效，不用于"跨时间重放"。**

**运行内一致性门**（`catalog_hash` 的唯一用途）：实验运行开始记录当前 `catalog_hash`；
每阶段边界校验未被并发改动，改动则 `EXPERIMENT_DATA_DRIFT` 失败——作用是"一次运行不混用两批数据"，
**不用于跨运行回放**（无历史版本存储）。

#### 3.4.3 配置

- YAML + Pydantic 严格校验（`extra=forbid, strict`）；配置冻结为不可变对象。
- 交易规则、费率、策略参数全部配置化，不硬编码散落。

#### 3.4.4 错误与日志

- 统一 `ErrorDetail(code, severity, message, context, remediation, retryable)`；进程边界转结构化错误，不泄露 traceback。
- 结构化 JSON Lines 日志，写前脱敏；stdout 只出成功 JSON，错误/日志走 stderr。

#### 3.4.5 持久化

- 元数据 SQLite（事务、乐观并发迁移）；大数据 Parquet；本地分析 DuckDB。
- **实验产物**按 Run 写入不可变目录，可信 Manifest 逐文件复核 SHA-256、字节数、行数、Schema、
  主键和排序；数据层 Raw/Canonical 继续内容寻址（§3.4.2 保留侧）。

### 3.5 数据源可插拔（演进保障）

```text
SourceClient(Protocol)          # 采集：登录/拉取原始批次
   ├─ BaoStockClient (infra)
   └─ TuShareClient  (infra，后续)
CanonicalMapper(Protocol)       # 规范化：供应商字段 → Canonical schema
   ├─ BaoStockMapper
   └─ TuShareMapper
```

上层（因子/策略/回测）只见 Canonical schema 与 `CanonicalResearchRepository`，切换供应商零改动。
验收用"模拟 TuShare 适配器"证明该隔离。

### 3.6 运行形态

- **CLI**：数据更新、跑实验、Worker。
- **Worker**：后台执行长任务（回测/因子研究），幂等、可取消、可重试。
- **Dashboard**：本机只读研究台，前后端分离 —— FastAPI 后端（`dashboard/` 包，暴露只读 REST/JSON API，
  默认仅监听 127.0.0.1）+ Vue 单页前端（`frontend/`，独立构建，通过 API 取数，不直连 SQLite/文件）。
- **Notebook**：内嵌 JupyterLab，只经公开 API 调用核心包，不复制引擎。

### 3.7 演进路径（不锁死）

- 频率：日频驱动，`on_event` 接口已为日内预留（未来加 session 内多时点驱动，契约不变）。
- 实盘：策略/回测/组合边界稳定，未来可加模拟盘→券商适配器→实盘风控，不推翻研究层。
- 复现：当前不做；若未来需要，可在实验层加"配置+数据版本快照"层，不影响其他层。

***

## 4. 数据层设计

详细设计已经拆分到[数据层设计](data-layer-design.md)，该文档是数据目录、Schema、
`LOCALIZE → CURATE → VALIDATE`、PIT 研究读取、质量门和数据身份的权威来源。

本总体设计只保留三个跨层边界：

- 研究代码只能通过 `CanonicalResearchRepository` 读取，不得扫描 Raw 或 Canonical 文件。
- Canonical 行必须遵守 PIT 审计列和可见性约束；严重或致命质量问题关闭研究读取门。
- 实验捕获当前目录身份并执行运行内漂移检查，但不提供历史数据快照回放。

接口落地基准仍见 `§11.2`，阶段性交付范围见 `§12.3`。

***

## 5. 因子层设计

### 5.1 设计取向

- 因子层是策略的**共享输入生产线**：定义、计算、统计诊断因子，供 AlphaModel、因子研究、
  归因共用。
- **PIT 与统计正确性保持全部严格性**（第一/第二优先级）。
- **去复现**：因子不绑定内容身份哈希用于复现；每次运行重算，缓存仅运行内、只为性能。
- **两条硬性工程约束**：① 标签契约必须含 `invalid_reason`/`label_kind`；② 全向量化，
  禁止 per-(date,instrument) Python 行循环。

### 5.2 边界

**输入**：

- `ResearchDataRepository`（只读、PIT 已截断）：行情/前复权收益/估值/状态/财务/行业等。
- `FactorSpec`（factor\_id、lookback\_sessions、依赖、方向、参数、`data_dependencies`）+ `FactorContext(start, end)`（`end` = PIT 截止）。
- 因子研究额外输入：PIT 股票池（`signal_date, instrument_id, eligible`）、持有期集合、可选行业策略和
  成本情景；理论与可执行 `label_kind` 固定同时生成，不提供开关。

**输出**：

- **因子值**：`pl.LazyFrame`，精确 schema `trade_date, instrument_id, factor_id, value, available_at, is_valid`
  （见 `§11.3`，满足 7 条不变量）。
- **未来收益标签**：主键 `signal_date, instrument_id, horizon, label_kind`，列
  `return_start, return_end, future_return, is_valid, invalid_reason`（固定优先级原因码）。
- **统计诊断**（因子研究产物）：`summary / coverage / label_quality / industry_coverage / ic /
  quantile_returns / long_short_returns / monotonicity / turnover / stability / cost_scenarios / correlation`；
  收益相关主键含 `signal_variant, label_kind, factor_ref, horizon`。
- 缺依赖能力时抛 `FACTOR_CAPABILITY_UNAVAILABLE`。

**不输出**：跨运行持久化因子缓存（每次重算；运行内缓存仅性能）。

### 5.3 因子契约

- 唯一 `factor_id`（无 `id@version`）。
- 统一输出 schema：

```text
trade_date, instrument_id, factor_id, value, available_at, is_valid
```

- `is_valid` 同时满足"因子自身有效"与"当日股票池有效"；无效值保留行 + 原因，不静默丢弃。
- `available_at` 绑定 PIT：`available_at ≤ 信号日上海日终` 才可用；越界即无效。
- 每次运行重算，**不持久化跨运行缓存**（与"不复现"一致）。运行内 `FeatureCache` 只为性能。

### 5.4 因子注册与计算

```text
Factor(Protocol): spec:FactorSpec ; compute(ctx:FactorContext) -> LazyFrame
FactorRegistry:   factor_id → (实现, 依赖声明)
FactorEngine:     解析依赖 DAG → 拓扑序计算 → 校验输出契约
```

- `FactorSpec`：`factor_id / 频率 / lookback_sessions / 依赖 / 方向 / 参数 / 数据依赖声明`。
- 依赖能力 preflight：因子声明的数据依赖不满足则拒绝（`FACTOR_CAPABILITY_UNAVAILABLE`）。
- 分区计算：按证券稳定分区，分区大小是性能参数，**不改变最终排序、内容或统计结果**。

### 5.5 内置因子（示例，非穷举）

价值：`earnings_yield_ttm`、`book_to_price_mrq`；质量：`roe_pit`（PIT 财务，190 日计龄）；
动量：`momentum_120_20`；风险：`volatility_60d`、`downside_volatility_60d`、`max_drawdown_120d`；
辅助：`avg_amount_20d`（流动性，不入 alpha）。

计算口径全部按**交易所会话**计数；停牌占位行记零收益；真实缺行使窗口失效；历史不足直接无效，
不回退不完整窗口。

### 5.6 向量化要求

- **禁止** per-(date,instrument) Python 行循环构造生产因子/标签/分位。
- 用 Polars 窗口函数、`over(instrument_id)` 滚动、NumPy 向量化内核。
- 具体：`build_future_returns` 用 join + shift/window；`assign_quantiles` 用 `over(signal_date)` 的 rank/qcut；
  滚动统计用 `rolling_*`。
- 双路径（若保留 Python 参考实现）必须与向量化路径字面量 oracle 互验。

### 5.7 未来收益标签

持有期 `h` 基础标签：`entry = 信号日后第1会话 open`，`exit = 信号日后第h会话 close`，`return = exit/entry − 1`。

主键：`signal_date, instrument_id, horizon, label_kind`。至少含：

```text
return_start, return_end, future_return, is_valid, invalid_reason
```

- **`label_kind`**：`THEORETICAL_FORWARD_RETURN`（纯预测关系）/ `EXECUTABLE_FORWARD_RETURN`
  （要求入场可成交、退出价有效）。统计按 `label_kind` 分区，不混样本。
- **`invalid_reason`（固定优先级）**：

```text
INCOMPLETE_FORWARD_WINDOW / NOT_LISTED_AT_ENTRY / ENTRY_SUSPENDED /
ENTRY_LIMIT_UP / MISSING_ENTRY_PRICE / DELISTED_WITHOUT_EXIT_PRICE /
MISSING_EXIT_PRICE / NONFINITE_RETURN
```

不得仅用 `future_return=null` 表达全部失败，不得在 join/聚合中静默删除无效样本。

### 5.8 统计内核（字面量 oracle 覆盖）

- 覆盖率：`coverage = valid_count / eligible_count`。
- IC：Pearson + Spearman RankIC（并列用平均秩）；滚动均值、累计、正值率；日度无效原因码。
- 分位：继续使用稳定排序拆分并列值，输出实际因子边界、样本数、均值收益和空组诊断。
- 多空：Q−1 毛 spread、至少三组时的单调性，以及相邻信号日高低分位成员换手。
- 相关：同日同股票池的因子相关矩阵（Pearson + Rank）。
- **显著性**：以 Bartlett kernel Newey-West/HAC 处理重叠持有期，滞后固定为
  `min(horizon-1, valid_count-1)`；分别对 Rank IC 和毛 spread family 做 Bonferroni/BH-FDR。
- **成本代理**：`net_spread = gross_spread - total_turnover × bps / 10000`；不计算净值、Sharpe
  或最大回撤。

全部统计公式用硬编码 oracle 校验，覆盖并列、常数截面、单样本、NaN/Inf、空组、零方差。

### 5.9 因子研究作为一种实验 kind

- 因子研究（质量/IC/分层/单调性/稳定性/换手/成本代理）由实验层 `FACTOR_STUDY` kind 编排，
  与策略回测共享同一 `Experiment → Run` 追踪主脊与比较视图（见
  [策略、回测与实验设计](strategy-backtest-experiment-design.md) `§4`）。
- 固定产物：summary / coverage / label\_quality / industry\_coverage / ic / quantile\_returns /
  long\_short\_returns / monotonicity / turnover / stability / cost\_scenarios / correlation。

### 5.10 与其他层边界

- 因子层只经 `CanonicalResearchRepository` 读数据；不导入策略/实验/接口层。
- AlphaModel（策略五模块之一）消费因子输出；因子层不感知策略。
- 复权：因子用前复权序列（`AdjustmentService`），与回测撮合的未复权价口径分离。

### 5.11 测试契约

- **PIT**：因子只用信号日可见输入；未来窗口测试；财务/行业修订不回填。
- **向量化**：无 per-row 循环（可用性能测试 + 代码门禁）；分区大小不改变内容哈希级结果。
- **oracle**：IC/RankIC/分位/多空/显著性/标签原因码各自字面量校验。
- **契约**：输出 schema 精确；无效样本保留行 + 原因码。

### 5.12 明确不做

- 跨运行因子缓存与复现绑定。
- 未在 factor\_id 声明依赖的隐式数据读取（尤其行业分类须显式声明）。

***

## 6. 策略层设计（独立文档）

策略层详细设计已拆分到[策略、回测与实验设计](strategy-backtest-experiment-design.md) `§2`，
该文档是 Strategy 协议、A+B 扩展模型、五模块装配、双均线趋势策略和订单意图的权威来源。

本总体设计只保留四个跨层边界：

- 策略只能通过绑定决策时点的只读数据视图取数，不能请求未来数据。
- `OrderIntent` 是回测引擎唯一消费的策略输出；目标权重必须先在策略侧转换为整数股数订单。
- A（模块化配置）与 B（Strategy 插件）共享同一引擎；双均线趋势属于独立时序策略插件。
- 策略不负责撮合、账户账务、绩效计算或实验登记。

***

## 7. 回测层设计（独立文档）

回测层详细设计已拆分到[策略、回测与实验设计](strategy-backtest-experiment-design.md) `§3`，
该文档是订单级驱动、A 股撮合规则、账务、估值、成本对账和回测产物的权威来源。

本总体设计只保留三个跨层边界：

- 引擎消费整数股数的 `OrderIntent`，按交易日推进并产出订单、成交、持仓、成本和净值事实。
- 策略信号与目标权重不进入撮合内部；分析层只读取已完成的回测产物。
- T+1、停牌、涨跌停、费用、滑点、容量、公司行为和做空能力按阶段及唯一规则文件实施。

***

## 8. 分析层设计（绩效 / 风险 / 归因）

### 8.1 定位

分析层是**只读、事后（post-hoc）的度量层：消费回测产物（nav/holdings/fills/costs）与因子研究
输入，产出可解释、可比较的绩效/风险/归因结果。它**不产生信号、不算因子、不撮合、不改账务——
只对已发生的结果做统计。设计优先级：**统计公式正确 > 口径可解释 > 性能**。

- 与回测层的边界：回测层产出"发生了什么"（净值、成交、费用、持仓），分析层产出"表现如何/为什么"。
- 与因子层的边界：策略回测的绩效/归因在本层；因子研究的 IC/分层/相关/显著性口径在因子层
  （`§5.7-5.8`），本层不重复实现。
- PIT 在此层不适用（对已完成的历史结果做度量），但**输入本身必须来自 PIT 正确的回测**。

### 8.2 边界

**输入**（回测层产物，见[策略、回测与实验设计](strategy-backtest-experiment-design.md) `§3.10`）：

- `nav`：`trade_date, cash_fen, long_market_value_fen, short_market_value_fen, accrued_fees_fen, margin_used_fen, equity_fen, benchmark_close`。
- `holdings`：逐日逐标的头寸/可卖/成本/市值。
- `fills`：成交与拒绝 + `reason_code`。
- `costs`：`rule_fees_fen + slippage_fen = total_cost_fen`；规则内费用的细分留在交易规则审计中。
- 配置：`sessions_per_year`(默认 252)、基准标识、切片规则（年/月/滚动窗口）。

**输出**（供实验层落盘、Dashboard 展示）：

- `performance`：逐日组合/基准收益、累计收益、净值与回撤时序表。
- `monthly_returns` / `annual_returns`：月度和年度收益表。
- `attribution`：归因表（期间/风格/个股；\[P3b-2] 多空分腿）。
- `execution_summary`：按方向 × 原因码汇总的成交质量。
- `exposure_summary`：可用仓位暴露；没有可靠风格输入时保持为空并披露原因。
- `metrics.json` 保存全部标量、日期、观察数与 `null`；`quality_disclosure.json` 保存未定义原因和统计口径。SQLite 只登记有限且已定义的数值。

**不负责**：信号/因子/撮合/账务；跨运行复现；实盘归因。

### 8.3 模块划分

```text
回测产物(nav/holdings/fills/costs)
   ├─► PerformanceMetrics   收益/风险标量 + 时序（净值/回撤/年月）
   ├─► ExecutionQuality     换手/费用拖累/成交失败统计
   ├─► RiskAnalytics        波动/beta/暴露/敞口（[P3b-2] gross/net、多空分腿）
   └─► Attribution          期间/风格/个股收益归因
```

分析入口都是纯计算：`f(内存回测表, 配置) -> 结果表`，确定性、稳定排序、可字面量 oracle 校验；分析阶段不读写最终产物目录。

### 8.4 PerformanceMetrics — 绩效

- **收益**：累计收益、年化收益、基准/超额收益（几何超额）。
- **风险调整**：年化波动、Sharpe、Sortino、Calmar、信息比率(IR)、beta、Jensen alpha、tracking error。
- **回撤**：回撤序列、最大回撤及其峰/谷/恢复日、水下时长、水下占比。
- **成本视角**：gross vs net 收益、费用拖累（累计/年化）。
- **时序表**：逐日净值/回撤、月度收益、年度收益。

**口径铁律**（易错点，实现见[实现级细化](implemention.md) `§5.8`）：

- 净值序列用 `equity_fen`（P3 = `cash + long_mv − accrued_fees`；\[P3b-2] 再减 `short_mv`）。
- **首日 0 收益**：波动率/Sharpe/Sortino/IR/beta 用 `daily_return[1:]`（剔首日）；drawdown/水下时长
  用含首日 0 的完整序列。二者不可混用。
- 未定义指标（`std=0`、`N=1`、`max_dd=0` 等）记入 `undefined_metrics`，**禁止填 0/NaN**。
- 年化统一用可配 `sessions_per_year`（默认 252）。

### 8.5 ExecutionQuality — 交易质量

- 成交/失败率、部分成交率；按 `reason_code`（涨跌停/停牌/容量/资金不足/\[P3b-2] 保证金不足）汇总。
- 换手率（单边/双边）、按日与年化。
- 费用拖累分项（佣金/印花税/过户费/\[P3b-2] 融券费）对净值的贡献。
- 输出 `execution_summary`：`side × reason_code` 的订单数/请求量/成交量/名义额。

### 8.6 RiskAnalytics — 风险与暴露

- 已实现波动、下行波动；滚动窗口版本。
- 相对基准：beta、tracking error、active risk。
- 敞口：现金占比、单标的最大权重、行业/风格暴露（用回测期实际持仓，非 PIT 约束层）。
- **\[P3b-2]**：gross/net exposure、多空分腿（多头腿、空头腿分别的收益与风险贡献）。

### 8.7 Attribution — 归因

- **期间归因**：年度/季度/月度/市场状态切片的收益分解，每切片绑定同一 Run。
- **个股归因**：各标的对组合收益的贡献（持仓权重 × 区间收益）。
- **风格归因**：对数流通市值、Beta、波动、流动性等风格因子的暴露与收益贡献；风格暴露输入应满足与
  信号相同的口径披露，非 PIT 输入只进显式披露的敏感性分析。
- **\[P3b-2] 多空分腿**：多头/空头各自的 gross 贡献与净贡献。

归因是**诊断**，不替代回测的实际成交结果；所有归因表按稳定主键排序。

### 8.8 因子研究分析（归属因子层，此处只引用）

因子研究（覆盖率、IC、RankIC、分层收益、多空、相关、稳定性、显著性）的口径与产物由
`§5.7-5.8` 定义，实验层 `FACTOR_STUDY` kind 调用。本层不重复实现；
两者共用同一"字面量 oracle 覆盖统计公式"的纪律。

### 8.9 依赖方向

```text
bootstrap 执行会话 → backtest（内存结果）
                  └→ analytics（只消费规范化 DataFrame）
```

`backtest` 不依赖 `analytics`。分析层不导入接口层、组合根或回测引擎运行态；组合根在
`ANALYTICS` 阶段把规范化 DataFrame 交给分析层，并在 `PERSIST` 统一发布。

### 8.10 包结构

```text
src/quant_research/analytics/
├── performance.py   # PerformanceMetrics：收益/风险标量 + 净值/回撤/年月时序
└── attribution.py   # Attribution：期间/风格/个股归因
```

### 8.11 测试契约

- **绩效 oracle**：Sharpe/Sortino/Calmar/IR/beta/alpha/最大回撤 各硬编码字面量校验。
- **首日 0 口径**：剔首日样本 vs 完整序列两套用法各自锁定；undefined 指标显式记录不填 0/NaN。
- **成本视角**：gross − net 与 costs 累计对账。
- **交易质量**：`reason_code` 汇总与 fills 逐笔一致；换手率正例。
- **归因**：个股贡献之和与组合收益一致（在无交易摩擦口径下）；切片绑定同一 Run。
- **\[P3b-2]**：多空分腿贡献之和 = 组合收益；gross/net 敞口正确。
- **确定性**：相同产物输入 → 相同指标与排序。

### 8.12 完成定义

> 分析层从回测产物计算出可解释的绩效/风险/归因，全部统计公式有字面量 oracle；首日 0 口径与
> undefined 记录严格；gross/net 与成本、个股/多空分腿贡献可对账；只读事后度量，不触信号/因子/账务。

***

## 9. 实验层设计（独立文档）

实验层详细设计已拆分到[策略、回测与实验设计](strategy-backtest-experiment-design.md) `§4`，
该文档是实验配置、Run 状态机、阶段编排、运行内数据身份门、不可变产物和比较追踪的权威来源。

本总体设计只保留三个跨层边界：

- 实验层调度策略、回测和分析能力，自身不产订单、不撮合、不重复实现统计公式。
- 因子研究与策略回测共享追踪主脊，但按实验 kind 生成各自的阶段与产物。
- 失败、取消和重试不得覆盖既有 Run；每阶段前后检查提交时捕获的数据目录身份。

***

## 10. Worker 与任务队列设计

### 10.1 定位与边界

Worker 是**后台长任务执行器**：把数据更新、数据校验、因子研究、策略回测这类耗时操作从
CLI/Dashboard 请求线程剥离，异步执行，关闭浏览器不影响任务。它只做"排队 → 领取 → 执行 → 落状态"
的编排与生命周期管理，**不含业务逻辑**——具体做什么由各 `TaskHandler` 委派给数据层流水线、
因子引擎、实验 runner。

单用户、本机、单体：**默认单 Worker 进程**；队列用 SQLite（非消息中间件）。设计支持多 Worker
并发领取（乐观并发 CAS），但一期不追求多机分布式。

#### 10.1.1 边界

**输入**：

- 任务提交：`TaskQueue.submit(task_type, payload, idempotency_key, priority, available_at)`（来自 CLI/Dashboard 用例）。
- 组合根注入的能力：数据流水线、`FactorEngine`、`ExperimentRunner`、各 `Store`、时钟。

**输出**：

- SQLite `task` / `task_attempt` 状态流转（QUEUED→RUNNING→SUCCEEDED/FAILED/CANCELLED）。
- 结构化进度事件与任务日志（供 Dashboard 运行中心下钻）。
- 业务副产物由 handler 产生（Canonical 数据、质量运行、实验产物），Worker 只记录 outcome 指针。
- 失败时结构化错误码 + 安全上下文，不泄露 traceback/敏感路径。

**不负责**：业务计算本身（委派 handler）、跨运行复现、分布式调度、定时触发（由外部 Windows 任务计划或 CLI 触发提交）。

### 10.2 任务模型（通用队列）

Worker 服务全平台后台任务，不只实验。`task_type` 枚举：

| task\_type          | 触发       | handler 委派                                  | run\_id |
| ------------------- | -------- | ------------------------------------------- | ------- |
| `DATA_UPDATE`       | 数据中心/CLI | 数据流水线 LOCALIZE→CURATE→VALIDATE（按固化计划）       | NULL    |
| `DATA_VALIDATION`   | 数据中心/CLI | 质量校验（全目录或单数据集诊断）                            | NULL    |
| `EXPERIMENT_RUN`    | 实验中心     | 按 Experiment kind 选择策略或因子阶段图         | 绑定 Run  |

要点：`run_id` **可空**——数据类任务无实验 Run（这修正了"task.run\_id NOT NULL"的过窄约束）。
payload 是该任务类型的冻结参数（如 DATA\_UPDATE 的固化计划 `plan_hash` + 窗口）。

### 10.3 生命周期与状态机

```text
QUEUED → RUNNING → SUCCEEDED
              ├→ CANCEL_REQUESTED → CANCELLED
              ├→ FAILED
              └→ ORPHANED
```

- 提交即 `QUEUED`（可带 `available_at` 延迟可见）。
- 领取（claim）以 CAS 置 `RUNNING` + `worker_id` + `locked_at`，并开一条 `task_attempt`。
- 终态写 `completed_at`；失败写 `error_json`；重试创建**新 attempt 或新 task**（见 §10.6）。
- 每次状态迁移携期望前态，乐观并发；影响行数为 0 即冲突失败，绝不 last-writer-wins。

### 10.4 领取与租约（claim / heartbeat / lease）

- **领取**：`SELECT ... WHERE status='QUEUED' AND (available_at IS NULL OR available_at<=now)
  ORDER BY priority DESC, created_at ASC LIMIT 1`，随即 CAS 抢占。
- **心跳**：RUNNING 或 CANCEL_REQUESTED 期间周期更新 `heartbeat_at`
  和 `updated_at`（租约续期）。
- **陈旧回收**：Worker 首次轮询必执行扫描，之后最多每 30 秒扫描一次。
  RUNNING/CANCEL_REQUESTED 任务超过 60 秒没有任务或活动 attempt 心跳时，
  任务与所有活动 attempt 一并进入终态 `ORPHANED`。历史异常数据即使缺少
  attempt 也能收敛，而存在新鲜 attempt 心跳时不会误回收。
- **Run 联动**：`EXPERIMENT_RUN` 任务进入 `ORPHANED` 后，仍处于
  CREATED/QUEUED/RUNNING 的 Run 同步进入 `FAILED`，错误码为 `TASK_ORPHANED`。
  回收不自动重跑；需按任务类型走显式重试契约。

### 10.5 幂等

- `idempotency_key` UNIQUE：相同键的重复提交收敛为同一 task（如 `run-<run_id>`、
  `data-update-<plan_hash>`）。提交竞态由唯一约束兜底。
- 所有任务**必须幂等**：handler 重跑不产生重复数据或冲突结果（数据层按内容寻址去重、
  实验任务只恢复同一未完成 Run；用户发起的重试始终创建新 Run）。这是"进程重启后识别未完成任务、
  避免重复写入"的前提。

### 10.6 取消与重试

- **取消**：置取消标志（或状态请求）；Worker 在**阶段边界**检查协作取消，安全退出并清理 staging 临时目录，
  置 `CANCELLED`。不做强杀。
- **重试**：只对幂等任务开放。
  - 数据类任务：重试复用原固化计划（不重新解析窗口）；旧版未固化计划的历史任务只能查看不能重试。
  - 实验类任务：重试建**新 Run + 新 task**，不覆盖旧 Run（实验产物不可覆盖历史）。

### 10.7 Handler 分派

Worker 主循环 kind 无关；`task_type → TaskHandler` 分派表由组合根装配。`TaskHandler.execute(task, ctx)`
接收进度/取消端口，返回 `TaskOutcome`。新增任务类型 = 注册一个 handler，不改 Worker 主循环。

### 10.8 依赖方向

Worker 属 `application` 层用例编排 + `bootstrap` 组合。`TaskQueue` 是消费者侧 Protocol（定义于
`tasks/`，被 `experiments`/`application` 依赖），SQLite 实现由基础设施提供、组合根注入。
Worker 主循环与 `TaskHandler` 分派在 `application`；Worker 不导入接口层（cli/dashboard）。

### 10.9 与 Dashboard 运行中心的契约

- 运行中心只读展示任务队列：排队/运行/成功/失败/取消、运行时长、提交参数、错误摘要与日志下钻。
- 写操作（重试、取消）经 application 用例，不在页面直接改库。
- 只对终态任务允许删除运行记录与 attempt 索引；实验/产物/审计/日志继续保留。
- 数据更新任务按业务语义展示更新模式、汇总范围、所选数据集逐项执行窗口。

### 10.10 测试契约

- **CAS 并发**：两 Worker 抢同一 QUEUED，仅一个成功领取；状态迁移冲突失败不覆盖。
- **租约回收**：无心跳的 RUNNING/CANCEL_REQUESTED 和缺少 attempt 的历史活动任务
  被置为 ORPHANED；有新鲜 attempt 心跳时不误判；对应活动 Run 置为 FAILED。
- **幂等**：相同 idempotency\_key 收敛为一 task；handler 重跑不产生重复副产物。
- **取消**：阶段边界协作退出，不留 staging 半成品目录。
- **重试**：数据类复用固化计划；实验类建新 Run 不覆盖旧 Run。
- **run\_id 可空**：DATA\_UPDATE/DATA\_VALIDATION 任务无 Run 也能全生命周期流转。
- **崩溃恢复**：进程重启首次轮询即识别超时活动任务，终结为 ORPHANED；
  重跑由显式重试创建，不覆盖原任务或 Run。

### 10.11 完成定义

> 长任务后台执行、关闭浏览器不影响；四类任务共用同一队列与生命周期（数据类 run\_id 为空）；
> 幂等、可协作取消、可安全重试；崩溃 Worker 的任务经租约超时被回收；状态迁移乐观并发不覆盖；
> 新增任务类型只注册 handler、不改主循环。

***

## 11. 接口与 Schema 契约（落地基准）

### 11.1 domain（稳定标识与错误）

```python
@dataclass(frozen=True, slots=True)
class InstrumentId:
    """规范证券标识，如 600000.SH / 000300.SH。"""
    symbol: str; exchange: str
    def canonical(self) -> str: ...
    @classmethod
    def parse(cls, value: str) -> "InstrumentId": ...

class Severity(StrEnum): INFO=...; WARNING=...; SEVERE=...; FATAL=...

@dataclass(frozen=True, slots=True)
class ErrorDetail:
    code: str; severity: Severity; message: str
    context: Mapping[str, JsonValue]; remediation: str; retryable: bool

class QuantError(Exception):
    def __init__(self, detail: ErrorDetail) -> None: ...
```

### 11.2 数据层

#### 11.2.1 数据源隔离（consumer-side Protocol）

```python
class SourceClient(Protocol):
    def login(self) -> None: ...
    def close(self) -> None: ...
    def fetch(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]: ...

class CanonicalMapper(Protocol):
    def accepts(self, endpoint: str, schema_fingerprint: str) -> bool: ...
    def normalize(self, raw: RawPartition) -> Iterable[CanonicalBatch]: ...

@dataclass(frozen=True, slots=True)
class RawBatch:
    source: str; endpoint: str; request: Mapping[str, JsonValue]
    retrieved_at: datetime; schema: tuple[str, ...]
    rows: Sequence[Mapping[str, JsonValue]]
```

#### 11.2.2 Canonical schema（列 + 主键；每表尾部含审计列）

审计列（所有数据集）：`source:str, available_at:datetime(UTC), availability_source:str,
pit_usable:bool, ingested_at:datetime(UTC)`。

| 数据集                       | 业务列                                                                                                            | 主键                                               |
| ------------------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `instrument`              | instrument\_id, exchange, board, name, instrument\_type, listing\_status, list\_date, delist\_date             | instrument\_id                                   |
| `trade_calendar`          | trade\_date, is\_trading\_day                                                                                  | trade\_date                                      |
| `daily_bar`               | instrument\_id, trade\_date, open, high, low, close, preclose, volume, amount, pct\_change                     | instrument\_id, trade\_date                      |
| `daily_basic`             | instrument\_id, trade\_date, pe\_ttm, pb\_mrq, ps\_ttm, turnover                                               | instrument\_id, trade\_date                      |
| `security_status`         | instrument\_id, trade\_date, is\_listed, is\_suspended, is\_st, board, price\_limit\_rule\_id                  | instrument\_id, trade\_date                      |
| `corporate_action`        | instrument\_id, ex\_date, action\_type, cash\_per\_share, share\_ratio, announced\_at                          | instrument\_id, ex\_date, action\_type           |
| `financial_observation`   | instrument\_id, report\_period, metric, value, revision, announced\_at                                         | instrument\_id, report\_period, metric, revision |
| `industry_classification` | as\_of\_date, supplier\_update\_date, instrument\_id, taxonomy, industry\_code, industry\_name, is\_classified | as\_of\_date, instrument\_id, taxonomy           |
| `index_bar`               | index\_id, trade\_date, open, high, low, close, preclose, volume, amount, pct\_change                          | index\_id, trade\_date                           |

价格类型：未复权 Float64；数量 Int64；金额 Float64。`corporate_action.action_type ∈
{CASH_DIVIDEND, STOCK_DIVIDEND, SPLIT, RIGHTS_ISSUE}`。

#### 11.2.3 只读研究仓库（上层唯一数据入口）

```python
class ResearchDataRepository(Protocol):
    def instruments(self) -> pl.LazyFrame: ...
    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame: ...
    def bars(self, instruments, start: date, end: date) -> pl.LazyFrame: ...           # 未复权
    def adjusted_bars(self, instruments, start: date, end: date) -> pl.LazyFrame: ...  # 前复权(end 为 PIT 截止)
    def log_returns(self, instruments, start, end, *, lookback_sessions: int) -> pl.LazyFrame: ...
    def daily_basics(self, instruments, start, end) -> pl.LazyFrame: ...
    def security_status_range(self, start, end, instruments=None) -> pl.LazyFrame: ...
    def corporate_actions(self, instruments, start, end) -> pl.LazyFrame: ...
    def financials_as_of(self, field_ids, as_of: date, instruments=None) -> pl.LazyFrame: ...
    def financial_history(self, field_ids, as_of: date, instruments=None) -> pl.LazyFrame: ...
    def industry_as_of(self, instruments, as_of: date) -> pl.LazyFrame: ...
    def catalog_hash(self) -> str: ...   # 运行内一致性用，非复现
```

铁律：任何返回都物理截断到 `available_at ≤ 截止 & pit_usable`，且只从当前已通过质量门的目录读取。

### 11.3 因子层

```python
FACTOR_OUTPUT_SCHEMA = {trade_date:Date, instrument_id:str, factor_id:str,
                        value:Float64, available_at:datetime(UTC), is_valid:bool}

@dataclass(frozen=True, slots=True)
class FactorSpec:
    factor_id: str; frequency: str; lookback_sessions: int
    dependencies: tuple[str, ...]; direction: int          # ±1
    parameters: Mapping[str, JsonValue]
    data_dependencies: tuple[DatasetKind, ...]

@dataclass(frozen=True, slots=True)
class FactorContext:
    start: date; end: date                                 # end = PIT 截止

class Factor(Protocol):
    @property
    def spec(self) -> FactorSpec: ...
    def compute(self, ctx: FactorContext) -> pl.LazyFrame: ...   # 精确 FACTOR_OUTPUT_SCHEMA

class FactorRegistry:
    def register(self, factor: Factor) -> None: ...
    def resolve(self, factor_id: str) -> Factor: ...
    def topological_order(self, refs: tuple[str, ...]) -> tuple[str, ...]: ...

class FactorEngine:
    def compute(self, factor_ids, ctx: FactorContext) -> Mapping[str, pl.LazyFrame]: ...
```

未来收益标签（因子研究/AlphaModel 复用）：主键 `signal_date, instrument_id, horizon, label_kind`；
列 `return_start, return_end, future_return, is_valid, invalid_reason`；`label_kind ∈
{THEORETICAL_FORWARD_RETURN, EXECUTABLE_FORWARD_RETURN}`。

### 11.4 策略、回测与实验契约（独立文档）

`Strategy`、五模块、`OrderIntent`、回测账务、成本一致性、实验模型及 SQLite 表定义已迁至
[策略、回测与实验设计](strategy-backtest-experiment-design.md) `§5`。本章不再保留重复契约；
数据层与因子层分别只依赖自身公开接口，策略、回测和实验按总体依赖方向组装。

### 11.5 防过拟合治理

```python
@dataclass(frozen=True, slots=True)
class SampleWindows:
    train: tuple[date, date]; validation: tuple[date, date]; test: tuple[date, date]

# Run 提交时计算 uses_test_region（是否覆盖 test 区间）→ Experiment 累计 test 预算计数
# 多重检验记账: Experiment 记录尝试 Run 数/参数组合数/校正方法(Bonferroni|BH_FDR)
```

### 11.6 错误码族（进程边界统一）

```text
DATA_QUALITY_GATE_CLOSED / DATA_SOURCE_CONTRACT
FACTOR_CAPABILITY_UNAVAILABLE
STRATEGY_CAPABILITY_UNAVAILABLE / PIPELINE_MODEL_UNAVAILABLE / COST_MODEL_INCONSISTENT
SHORT_NOT_SUPPORTED                         # 首版(P3)引擎拒绝空头订单；P3b-2 启用做空后不再抛
EXPERIMENT_DATA_DRIFT / EXPERIMENT_STAGE_FAILED / EXPERIMENT_CANCELLED / EXPERIMENT_STATE_CONFLICT
```

### 11.7 依赖方向（AST 门禁强制）

```text
bootstrap → application → experiments → strategies → {alpha,risk,costs,portfolio} → {data,factors,backtest}
infrastructure 只被 bootstrap 装配；能力包只经 ResearchDataRepository 等 Protocol 取数。
```

***

## 12. 分阶段实现计划

### 12.1 全局工程约定

- Python 3.12 + uv；Ruff + 严格 mypy；pytest（unit/integration/pit/regression/performance/acceptance）。
- 每层公开 API 中文 docstring；跨边界 DTO 冻结 dataclass；依赖注入用消费者侧 Protocol。
- 架构边界 AST 门禁测试从 P0 起建立，随层扩展。
- 每阶段末：Ruff + mypy + 该阶段测试全绿，才进下一阶段。

### 12.2 阶段 P0 — 地基（domain + 存储 + 架构门禁）

**交付**：`domain/`（InstrumentId/Severity/ErrorDetail/QuantError/DatasetKind 枚举）；
`infrastructure/` SQLite + SQLAlchemy + Alembic 初始迁移（
[策略、回测与实验设计](strategy-backtest-experiment-design.md) `§5.5.1` 全部表；
`task`/`task_attempt` 以[实现级细化](implemention.md) `§6.3` 为准，`run_id` 可空）；
`bootstrap/` 骨架 + Engine 生命周期；`cli/` 入口骨架；架构边界 AST 门禁测试。

**验收**：空库建库 + 迁移升级测试通过；AST 门禁能拦住非法 import；`quant --help` 可跑。

### 12.3 阶段 P1 — 数据层

**交付**：

- `SourceClient` / `CanonicalMapper` Protocol + BaoStock 实现（infra）。
- Raw 存储（内容寻址 Parquet + request 去重 + 断点续抓）。
- Canonical 九数据集 schema + LOCALIZE→CURATE 流水线（year 分区增量）。
- **公司行为** **`corporate_action`** **数据集**（含解析与 available\_at 时点化）。
- 复权：`AdjustmentService`（前复权因子由公司行为推导）。
- 质量门 VALIDATE（规则 × 数据集，严重/致命关研究门）。
- `CanonicalResearchRepository`（PIT 物理截断 + `catalog_hash`）。
- **通用 Worker 队列**（首次落地）：`TaskQueue` + CAS 领取/心跳/租约回收/幂等 + kind 无关主循环 +
  `DATA_UPDATE`/`DATA_VALIDATION` handler（详见 `第 10 章` / [实现级细化](implemention.md)第 6 章）。数据更新/校验
  作为后台任务运行；后续阶段只注册新 handler、不改主循环。

**验收（PIT 为重）**：

- 财务/行业修订不回填旧信号日；仓库返回物理截断到 `available_at ≤ 截止 & pit_usable`。
- 公司行为可见性时点正确；除权除息日行情跳变与事件对账。
- 每条质量规则字面量 oracle；合法市场异常不误报。
- 模拟 TuShare mapper 产出与 BaoStock 一致的 Canonical schema（隔离验证）。
- 确定性排序测试。
- Worker：CAS 并发领取仅一个成功、心跳超租约回收、幂等键收敛、`DATA_UPDATE` 复用固化计划；`run_id` 可空任务全生命周期流转。

### 12.4 阶段 P2 — 因子层

**交付**：`Factor`/`FactorSpec`/`FactorContext` 契约 + `FactorRegistry` + `FactorEngine`；
首批内置因子（价值/质量 roe\_pit/动量/风险/流动性）；统计内核（IC/RankIC/分位/多空/相关/显著性）；
未来收益标签（含 `label_kind` + `invalid_reason` 固定优先级）。

**验收**：

- 统计公式全部字面量 oracle（含并列、常数截面、单样本、NaN/Inf、空组、零方差）。
- **无 per-(date,instrument) Python 行循环**（性能测试 + 代码审查门禁）。
- 因子 PIT：只用信号日可见输入；未来窗口测试。
- 标签无效样本保留行 + 原因码，不静默丢弃。

### 12.5 阶段 P3 — 回测引擎与账务（纯多头，无公司行为）★ 最难主干

首版只吃"多头可信回测"的复杂度，不含公司行为与做空。

**交付**：

- `OrderIntent`（**只带整数** **`quantity`**）/ `DecisionContext`（`data` 为绑定 signal\_date 的窄视图
  `DecisionData`）/ `AccountView` 契约。接口一次性预留 `SHORT_*`/`available_margin`，P3 引擎拒绝空头
  `SHORT_NOT_SUPPORTED`。
- 撮合：`MarketRuleBook`（涨跌停/费用，配置化，内容哈希）+ `ExecutionModel`——BUY/SELL、T+1 可卖、
  涨跌停、停牌、费用、滑点、成交量容量、整手/碎股、**未复权价撮合**、部分成交/拒绝原因码。
- 账务：多头 FIFO lot（T+1）+ `OPENING_CASH/BUY/SELL` ledger；逐日 mark-to-market；整数分；
  **`equity = cash + long_market_value − accrued_fees`** + ledger-vs-头寸双向对账。
- `RebalancePlanner`（`TargetWeights` → 整数股数 `OrderIntent`，供 P4 基类用）。
- `BacktestEngine`：逐日 = on\_event 取单 → 撮合 → mark-to-market；T/T+1 分离。
- **已知偏差（显式记录）**：无公司行为，NAV 在除权除息日会因未复权价跳水而暂时失真，P3b-1 修正。

**验收（回测可信为重）**：

- 单测字面量 oracle：T+1 可卖、涨停买/跌停卖阻断、停牌、整手/碎股、费用、滑点、容量、部分成交。
- `equity = cash + long_market_value − accrued_fees` 不变量与双向对账成立。
- 撮合用未复权价、信号用前复权价的口径分离测试。
- 空头订单被拒 `SHORT_NOT_SUPPORTED`；`OrderIntent` 无权重字段（权重经 `RebalancePlanner` 翻译）。
- 无未来函数：`DecisionData` 窄视图物理边界测试（无 as\_of/end 参数可越界）。

### 12.6 阶段 P4 — 策略层 A+B（最小闭环）

**交付**：

- `Strategy` 协议 + `StrategyRegistry` + `WeightTargetStrategy` 基类（`target_weights → TargetWeights`
  → `RebalancePlanner` → `OrderIntent`）。
- 五模块 Protocol + **退化实现**：AlphaModel `multi_factor_composite`、RiskModel `none`、
  CostModel `fixed_bps`、Construction `top_n_equal_weight`、ConstraintSet 通用集合（多头约束）。
- `CrossSectionalStrategy` 装配五模块；内置 `etf_rotation` / `stock_multifactor` 配置。
- **成本双角色一致性**：CostModel 与 rulebook.fees 同源，一致性测试。

**验收**：

- etf\_rotation / stock\_multifactor 端到端跑通，锁定回归黄金结果（股票池/因子/权重/成交/净值/绩效）。
- **加一个 stub 插件策略无需改 runner/引擎即跑通**（扩展性验收，核心诉求）。
- 成本一致性测试触发 `COST_MODEL_INCONSISTENT` 的负向用例。

> P0–P5 到此构成**首个可跑通闭环**（纯多头、无公司行为、无做空），验证核心诉求"快速迭代策略"。
> 下列 P3b-1/P3b-2 是独立增量，不在首个闭环关键路径上。

### 12.7 阶段 P3b-1 — 公司行为账务（首个闭环跑通后）

**交付**：

- 消费数据层 `corporate_action`：`DIVIDEND` ledger 事件按 `ex_date` 派发现金红利；送转按
  `share_ratio` 调整 lot 股数（成本不变、摊薄单位成本）。
- 引擎逐日循环插入"撮合前处理当日公司行为"。

**验收**：

- 现金分红入账、送转调股各字面量 oracle；除权除息日 NAV 不再跳水（对照 P3 已知偏差）。
- `equity` 不变量在含分红场景仍成立。

### 12.8 阶段 P3b-2 — 做空账务（在 P3b-1 之后）

**交付**：

- 账务扩展负头寸：`ShortPosition` + 可用资金/保证金占用/维持保证金；`SHORT_OPEN/SHORT_COVER/
  BORROW_FEE/MARGIN_*` ledger；空头按当日 close 计 `short_market_value`（负债）；
  **`equity = cash + long_market_value − short_market_value − accrued_fees`**（保证金是占用，不加 equity）。
- `MarketRuleBook` 增融券费/保证金比例；`ExecutionModel` 放开空头撮合。
- CostModel 增融券成本项（沿用成本双角色一致性）。
- 分析层多空分腿归因、gross/net 敞口。
- 解锁配对纯对冲、CTA 空头腿策略（`PairsStrategy` 在此交付）。

**验收**：

- 做空开平、保证金占用/释放、借券费按天计提、空头逐日 mark 各字面量 oracle。
- 多空混合下 `equity = cash + long_market_value − short_market_value − accrued_fees` 与双向对账成立（无双算）。
- 配对纯对冲策略端到端跑通。

### 12.9 阶段 P5 — 实验层编排 + 分析

**交付**：

- `Experiment/Run` 模型 + CAS 状态机 + `ExperimentRunRegistry`（SQLite）。
- 唯一 `EXPERIMENT_RUN` handler：策略阶段图
  `VALIDATE→PREPARE_INPUTS→STRATEGY_RUN→ANALYTICS→PERSIST`，因子阶段图
  `VALIDATE→PREPARE_INPUTS→ANALYZE_FACTORS→PERSIST`。
- 运行内一致性门（`catalog_hash` 在阶段和长回测交易日边界校验，漂移即失败）。
- `analytics/`：绩效（Sharpe/Sortino/Calmar/回撤/IR/beta/alpha）、成交质量、归因（多空分腿随 P3b）。
- Worker：唯一实验任务 handler（`EXPERIMENT_RUN`）接入**通用 Worker 队列**——队列本身
  （`task`/`task_attempt` 表、CAS 领取、心跳/租约、幂等、主循环）在 P0/P1 已建（数据类任务先用），
  P5 只注册实验 handler（详见 `第 10 章` / [实现级细化](implemention.md)第 6 章）。CLI `quant worker once|run` + 提交实验。
- 防过拟合治理：SampleWindows + test 预算计数 + 多重检验记账。

**验收**：

- 绩效指标字面量 oracle；首日 0 收益口径锁定。
- 阶段失败/取消不留半成品目录；状态迁移冲突失败不覆盖。
- 因子研究 kind 与策略回测 kind 共享同一 runner 与追踪主脊。
- 数据漂移触发 `EXPERIMENT_DATA_DRIFT`。

### 12.10 阶段 P6 — 四范式策略 + 异构验收

**交付**：`DualMATrendStrategy`（`strategy_id=dual_ma_trend`，CTA 多头版）复用
`WeightTargetStrategy`，`EventDrivenStrategy`（稀疏订单，多头版）直接实现 `on_event`；
`PairsStrategy`（做空对冲）**依赖 P3b-2**，在 P3b-2 完成后交付。

**验收**：多头范式（截面/双均线择时/事件驱动）各跑通一个示例并在实验层登记；双均线满足
[策略、回测与实验设计](strategy-backtest-experiment-design.md) `§2.7.5` 的信号、时序和续单 oracle；
证明订单级接口对异构范式充分；配对纯对冲随 P3b-2
验收（做空账务正确）。

### 12.11 阶段 P7 — Dashboard（FastAPI + Vue）

**交付**：

- 后端 `dashboard/`：FastAPI 只读 REST/JSON（研究就绪/数据中心/实验比较/回测分析/因子分析/运行中心），仅监听 127.0.0.1。
- 前端 `frontend/`：Vue SPA + 图表；通过 API 取数，不直连 SQLite/文件。
- Notebook 入口内嵌 JupyterLab。

**验收**：六页面可用；写操作经后端用例；前端 typecheck/build/lint 通过；后端 API 单测。

### 12.12 阶段 P8 — 全市场性能与端到端验收

**交付**：20 年全市场数据导入；性能剖析（阶段耗时/峰值内存）；回归/集成/验收测试套件。

**验收**：`§2.14` 全部通过——含"加新策略只写策略逻辑"、多头四范式、分红除权、
20 年全市场多因子回测 ≤ 60 分钟、TuShare 隔离、无未解释严重/致命质量问题。（做空为 P3b 验收项。）

### 12.13 依赖关系与里程碑

```text
P0 ─► P1 ─► P2 ─┐
      └──► P3 ──┴─► P4 ─► P5 ─► P6 ─► P7 ─► P8
                    └► P3b-1（公司行为）─► P3b-2（做空）─► (配对纯对冲)
```

P3b-1（公司行为）与 P3b-2（做空）挂在 P4 之后、与 P5/P6 并行，均不在首个可跑通闭环
（P0–P5，纯多头无公司行为）的关键路径上；P3b-2 依赖 P3b-1。

- **里程碑 M1（P0–P2）**：数据可信 + 因子可算（PIT + 质量门 + oracle）。
- **里程碑 M2（P3–P4）**：一个截面策略端到端可回测（纯多头、无公司行为），扩展性验收通过。
- **里程碑 M3（P5–P6）**：实验编排 + 多头四范式（截面/择时/事件驱动）+ 比较。
- **里程碑 M3b-1（P3b-1）**：公司行为账务（分红送转），除权日 NAV 不失真。
- **里程碑 M3b-2（P3b-2）**：做空账务 + 配对纯对冲。
- **里程碑 M4（P7–P8）**：Dashboard + 全市场性能验收。

### 12.14 建设顺序理由

- **P3（回测引擎/账务）最难改、身份最底层**，与 P1/P2 并行度低，故排在因子之后、策略之前独立成阶段——
  它定义 `OrderIntent`/`DecisionData`/账务不变量，上层全依赖它。
- **首版只吃多头可信回测**：公司行为（P3b-1）与做空（P3b-2）都会放大最难模块的复杂度，分别延后。
  纯多头即可跑通完整闭环并验证核心诉求"快速迭代策略"；两个增量不阻塞首个闭环，接口一次性预留、
  只补实现、不改上层契约。做空依赖公司行为先就位（P3b-2 依赖 P3b-1）。
- 五模块 P4 **先退化实现打通闭环**再补 MVO/风险模型（渐进），避免一上来引入二次规划求解器依赖。
- Dashboard 排在功能内核之后，因为它只是只读展示层，不阻塞核心诉求"快速迭代策略"。

### 12.15 完成定义

见 `§2.15`：写新策略只需实现 `Strategy.on_event` 并注册（或截面写五模块配置），
不改 runner/引擎/基础设施即可跑通、出绩效、Dashboard 并排比较；四范式可表达（公司行为随 P3b-1、
多空对冲随 P3b-2）；PIT 与 A 股约束在引擎层强制；事前/事后成本可对账；质量门失败不开放研究读取。
