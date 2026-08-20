# A 股量化策略研究工作台

文档状态：设计草案，待用户书面审阅　·　日期：2026-08-20

本文是 A 股个人量化策略研究工作台的**唯一权威设计文档**，按阅读顺序汇集项目诉求、需求规格、整体架构、各层设计、接口契约与实现计划。实现级细化见 `implementation.md`。文中形如 `第 N 章 §M` 的引用指向本文对应章节。

## 目录

1. 项目主诉求与定位（对齐基准）
2. 需求规格
3. 整体架构
4. 数据层设计
5. 因子层设计
6. 策略层设计（A+B 扩展 / 订单驱动）
7. 回测层设计（订单级引擎 / 多空账务）
8. 分析层设计（绩效 / 风险 / 归因）
9. 实验层设计（编排 / 追踪 / 比较）
10. Worker 与任务队列设计
11. 接口与 Schema 契约（落地基准）
12. 分阶段实现计划

***

## 1. 项目主诉求与定位（对齐基准）

### 1.1 一句话定位

个人自有资金的 A 股低频量化**策略研究工作台**：核心价值是**低摩擦地快速迭代、试错、比较各种策略**，
在保证"结果当下可信"的前提下尽快看到结果。不追求特定收益率，不做实验归档系统。

### 1.2 诉求优先级（高 → 低）

1. **PIT 正确 / 无未来函数**：只用信号产生时真实可得的数据。用了未来信息，结论**当下就是错的**——与复现无关，第一优先级，不可让步。
2. **回测可信**：真实模拟 A 股交易制度（T+1、涨跌停、停牌、费用、滑点、容量；做空保证金），避免过度乐观。
3. **快速迭代任意策略**：加一个新策略应"只写策略逻辑本身"，不改 runner、不改回测引擎、不碰基础设施。这是决定架构形态的核心生产力诉求。
4. **数据源隔离 + 可演进**：BaoStock 可换 TuShare，不动上层；研究平台可平滑长到模拟/实盘。
5. **单人可维护**：Notebook + 核心包 + 本地 Dashboard 共享同一套逻辑；单体、单用户、本机。

### 1.3 明确放弃的诉求（不为其增加复杂度）

放弃的是**跨运行复现回放**，不是所有哈希（哈希用于去重/增量/完整性/运行内一致性仍保留；
技术边界见 `§3.4.2「哈希边界」`）：

- **跨运行复现回放**：不保留可选择的历史 catalog、不要求旧 run 可完全重放、不做 source/env 指纹链、
  不做 `run_identity` 内容身份哈希、不做产物 manifest 逐文件 SHA-256 校验、不强制"结果不可覆盖"。
  重跑允许覆盖。
- **数据层复现**：数据层重性能与去重（"只考虑不重算，不考虑复现"），不为跨运行复现付费。
- 高频 / Tick / 盘口 / 实盘下单 / 多用户 / 公网 / 分布式 / 参数寻优后直接发布 / 收益承诺。

### 1.4 关键边界与再定位

- **PIT 一致性门**：作用是"防止运行中途数据变了、导致这次结果混用两批数据而错误"，即
  **运行内一致性**，不是跨运行复现回放。
- **数据版本号（`catalog_version`）轻量化**：只用于运行内一致性判定，不做历史版本存储与回放。
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
比较各种策略**，并在"结果当下可信"的前提下尽快看到结果。不追求特定收益率，不做实验归档系统。

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

- **跨运行复现回放**：不保留可选择历史 catalog、不要求旧 run 可完全重放、不做 source/env 指纹链、
  不做 `run_identity` 内容身份哈希、不做产物 manifest 逐文件 SHA-256 校验、不强制"结果不可覆盖"。重跑允许覆盖。
- **数据层复现**：数据层重性能与去重，不为跨运行复现付费（"只考虑不重算，不考虑复现"）。
- 高频/Tick、实盘下单、多用户、公网、分布式、参数寻优后自动发布、收益承诺。

> 仍保留完整性/增量/运行内一致性所需的哈希：Raw `request_hash`/`content_hash`、
> Canonical partition `content_hash`、`schema_fingerprint`、`catalog_version`、curate `input_hash`。

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

### 2.5 数据层需求（详见 `第 4 章`）

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

### 2.10 策略 / 回测 / 实验层需求（详见 `第 6 章`、`第 7 章`、`第 9 章`）

- 追踪实体：`Experiment → Run`（轻量，Run 无 run\_identity 复现哈希，但存 catalog\_version）。Run 记录冻结配置快照、状态、指标、产物指针。
- 编排：`VALIDATE → PREPARE_INPUTS → STRATEGY_RUN(逐时点产订单) → BACKTEST → ANALYTICS → PERSIST`；kind 无关执行器。
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
- 策略/实验中心：按策略与标签筛选、并排比较、结论标记、从已有配置派生新 Run。
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
| `catalog_version`                  | 当前目录版本号，运行内一致性门（非历史回放） |
| curate `input_hash`                | 增量：输入未变则复用不重算          |

| 不做（跨运行复现回放）                | 理由                |
| -------------------------- | ----------------- |
| 可选择的历史 catalog / 数据快照版本    | 不回放旧数据            |
| `run_identity` 内容身份哈希      | 不需按内容身份定位/重放旧 run |
| source/env 指纹链             | 不锁定代码+环境以复现       |
| 产物 manifest 逐文件 SHA-256 校验 | 产物普通目录写盘即可        |
| 强制"结果不可覆盖"                 | 重跑允许覆盖            |

一句话：**内容哈希用于"这一份数据/这一次运行"的正确与高效，不用于"跨时间重放"。**

**运行内一致性门**（`catalog_version` 的唯一用途）：实验运行开始记录当前 `catalog_version`；
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
- **实验产物**普通目录写盘（不做逐文件 manifest SHA-256 校验，仅基本完整性 + 孤儿清理）；
  数据层 Raw/Canonical 仍内容寻址（§3.4.2 保留侧）。

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

### 4.1 设计取向

- **不为跨运行复现付费**：无数据快照版本、无历史 catalog 回放。数据层只维护"当前一份"
  Canonical，重性能与去重（不重算），不为跨运行复现付费。注意：Raw/Canonical 仍**内容寻址**
  （content\_hash/request\_hash/schema\_fingerprint），那是为去重、增量、完整性服务，**不是**为回放旧数据——
  边界见 `§3.4.2`。
- **PIT 是唯一不让步的红线**：时点正确与防未来函数保持全部严格性。
- **公司行为一等公民**：分红/送转/除权除息作为 Canonical 事件数据集，服务复权与回测账务。
- **数据源可插拔**：SourceClient + CanonicalMapper 隔离，上层零改动切 TuShare。

### 4.2 边界

**输入**：

- 供应商原始响应（BaoStock/TuShare），经 `SourceClient.fetch(request)` 返回 `RawBatch`
  （`source, endpoint, request:Mapping, retrieved_at, schema:tuple[str], rows:list[dict[str,str]]`，全 String）。
- 采集窗口 `(start, end)` 与 `--full` 标志（由 CLI/Worker 传入）。
- 交易规则/费率无关；数据层不消费策略配置。

**输出**（下游唯一契约是 `ResearchDataRepository`，见 `§11.2.3`）：

- 九个 Canonical 数据集（Parquet，强类型 + 五审计列），schema 见 `§11.2.2`：
  `instrument / trade_calendar / daily_bar / daily_basic / security_status / corporate_action /
  financial_observation / industry_classification / index_bar`。
- 只读 PIT 接口返回 `pl.LazyFrame`，物理截断到 `available_at ≤ 截止 & pit_usable`：
  `bars / adjusted_bars / log_returns / daily_basics / security_status_range / corporate_actions /
  financials_as_of / financial_history / industry_as_of / trade_calendar / instruments`。
- `catalog_version() -> str`（运行内一致性用）；质量门状态（关门则 Repository 抛
  `DATA_QUALITY_GATE_CLOSED`）。
- 副产物：SQLite 元数据（raw\_request/raw\_object/canonical\_dataset/canonical\_partition/
  data\_catalog\_state/quality\_run…）、质量运行报告。

**不输出**：复权后的持久化价（复权因子在读取侧算）、任何跨运行数据快照。

### 4.3 三层与流水线

```text
供应商 → SourceClient → Raw(原样,内容寻址,不可变)
      → CanonicalMapper → Canonical(统一/PIT/复权入口/公司行为)
      → FeatureBuilder → Feature(因子结果,运行内缓存)
```

流水线阶段：`LOCALIZE → CURATE → VALIDATE`。

- **LOCALIZE**：只做供应商 I/O，写 Raw；按 request 去重、断点续抓（性能，不为复现）。
- **CURATE**：Raw → Canonical，规范化 + PIT 时点化 + 复权计算入口。
- **VALIDATE**：质量规则；严重/致命失败关闭"研究读取门"。

### 4.4 Canonical 数据集

| 数据集                       | 主键                                               | 说明                   |
| ------------------------- | ------------------------------------------------ | -------------------- |
| `instrument`              | `instrument_id`                                  | 证券主数据、板块、上市生命周期      |
| `trade_calendar`          | `trade_date`                                     | 开市/休市                |
| `daily_bar`               | `instrument_id, trade_date`                      | 未复权 OHLCV + 成交额      |
| `daily_basic`             | `instrument_id, trade_date`                      | 估值/换手                |
| `security_status`         | `instrument_id, trade_date`                      | 上市/停牌/ST/可交易         |
| `corporate_action`        | `instrument_id, ex_date, action_type`            | **分红/送转/除权除息事件（新增）** |
| `financial_observation`   | `instrument_id, report_period, metric, revision` | PIT 财务 + 供应商重述       |
| `industry_classification` | `as_of_date, instrument_id, taxonomy`            | 按请求日期重建的行业状态         |
| `index_bar`               | `index_id, trade_date`                           | 指数行情（基准/全收益基准）       |

#### 4.4.1 通用 PIT 审计列（每个数据集尾部）

```text
source                # 供应商
available_at          # 业务上最早可用于决策的时间（PIT 截断依据）
availability_source   # available_at 的确定依据
pit_usable            # 是否足以支持 PIT（false 保留供审计但不参与 PIT）
ingested_at           # 本地抓取时间（仅血缘，不替代 available_at）
```

#### 4.4.2 公司行为（新增，服务复权与回测账务）

`corporate_action` 至少含：

```text
instrument_id, ex_date(除权除息日), action_type(CASH_DIVIDEND|STOCK_DIVIDEND|SPLIT|...),
cash_per_share(每股现金红利, 税前), share_ratio(每股送转比例), announced_at, available_at, pit_usable
```

两个消费方向：

1. **复权计算**：Canonical 提供统一前/后/不复权入口，前复权因子由公司行为事件推导。
2. **回测账务（回测层 P3b-1 消费）**：回测引擎按 `ex_date` 给持仓派发现金红利（`DIVIDEND` ledger 事件）、
   调整送转股数，使 NAV 在除权除息日不因未复权价跳水而失真。数据集本身在数据层首版即产出，
   回测侧的消费是 P3b-1。

### 4.5 复权语义

- Canonical `daily_bar` 存**未复权**价（撮合真实性要求真实成交价）。
- 提供 `AdjustmentService`：由 `corporate_action` 推导前复权因子，供因子/信号计算使用前复权序列。
- **口径分离铁律**：回测撮合用未复权价 + 公司行为账务；因子信号用前复权价。二者不可混用。

### 4.6 PIT 与研究读取

- 研究代码只经 `CanonicalResearchRepository` 读取，禁止旁路扫描 Raw/Canonical 路径。
- 读取按 `as_of / signal_date` 截断：物理上不返回 `available_at > 截断` 或 `pit_usable=false` 的行。
- 财务：`financials_as_of` 选信号日已知的最新 revision；`financial_history` 保留截止时点全部 revision，
  不用最终修订回填早期信号日。
- 行业：按请求日期重建 as-of 状态，首次出现该状态的 `as_of_date` 起可见，不回写更早的 `supplier_update_date`。

### 4.7 质量门（VALIDATE）

阻断规则（严重/致命关闭研究门）：

```text
FATAL : 必需数据集缺失/为空、交易日历无开市日、Canonical schema 不符、跨分区 schema 不一致、主键重复
SEVERE: 必填值空、OHLC 关系错误、成交量为负、交易日覆盖缺口、证券未知、财务可用时间缺失/倒置、
        公司行为除权日与行情跳变不一致（新增校验）
```

- 质量规则语义要匹配业务真实：合法市场异常（如停牌日 turnover 为空）不误报。
- 结果按"规则 × 数据集"记 PASS/FAIL/SKIPPED；只有全绿才开研究门。

### 4.8 数据身份（轻量，去复现）

- 维护 `catalog_version`（单调递增或当前内容摘要），仅表示"当前这一份 Canonical 的版本号"。
- 用途：实验运行的**运行内一致性门**（运行中数据被更新则本次运行失败），**不做历史版本存储与回放**。
- 无 `data_hash` 进实验身份、无快照复现（对齐 project-intent）。

### 4.9 存储与性能

- Raw/Canonical 为 Parquet；按 `year=` 分区（日更只重建当年分区，成本 O(当年)，见下）。
- 去重：LOCALIZE 按 request 去重、断点续抓；CURATE 按分区输入变化增量重建。
- DuckDB/Polars 向量化；投影下推、尽早过滤。
- **性能取舍**：year 分区使日更成本随年内进度增长、跨年清零；若年底日更成为瓶颈可降级为月分区
  （最小改动），append-only 追加因复杂度暂不做（对齐"不过度设计"）。

### 4.10 数据源隔离

```text
SourceClient(Protocol): login/logout/fetch_* → RawBatch
CanonicalMapper(Protocol): RawBatch → CanonicalBatch（供应商字段 → Canonical schema）
```

BaoStock 一期实现；TuShare 后续加适配器不动上层。ETF 行情、指数、行业、财务各端点的
供应商差异全部吸收在 mapper 层。

### 4.11 测试契约

- **PIT**：财务/行业修订不回填旧信号日；`CanonicalResearchRepository` 截断物理有效；公司行为可见性时点正确。
- **质量**：每条规则字面量 oracle；合法异常不误报。
- **复权/公司行为**：前复权因子由事件推导正确；除权日行情跳变与事件对账。
- **确定性**：排序稳定，不依赖 set/文件系统/DB 未声明顺序。
- **隔离**：模拟 TuShare mapper 产出与 BaoStock 一致的 Canonical schema。

### 4.12 明确不做

- 数据快照版本与历史 catalog 回放。
- 跨运行数据复现（source/env 指纹、旧数据回放）。注意：Raw/Canonical 的内容寻址哈希**保留**（去重/增量/完整性），不在此列。
- 分钟/Tick、盘口、实时行情。
- 北交所以外范围限制（范围按业界方式放开，不再人为设限）。

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
- 因子研究额外输入：PIT 股票池（`signal_date, instrument_id, eligible`）、持有期集合、`label_kind` 选择。

**输出**：

- **因子值**：`pl.LazyFrame`，精确 schema `trade_date, instrument_id, factor_id, value, available_at, is_valid`
  （见 `§11.3`，满足 7 条不变量）。
- **未来收益标签**：主键 `signal_date, instrument_id, horizon, label_kind`，列
  `return_start, return_end, future_return, is_valid, invalid_reason`（固定优先级原因码）。
- **统计诊断**（因子研究产物）：`coverage / ic / quantile_returns / long_short_returns /
  correlation`（+ 可选 `significance / stability`），主键含 `signal_variant, factor_ref, horizon, signal_date`。
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
ENTRY_LIMIT_UP / ENTRY_LIMIT_DOWN / MISSING_ENTRY_PRICE / MISSING_EXIT_PRICE /
DELISTED_WITHOUT_EXIT_PRICE / NONFINITE_RETURN
```

不得仅用 `future_return=null` 表达全部失败，不得在 join/聚合中静默删除无效样本。

### 5.8 统计内核（字面量 oracle 覆盖）

- 覆盖率：`coverage = valid_count / eligible_count`。
- IC：Pearson + Spearman RankIC（并列用平均秩）；滚动均值、累计、正值率；日度无效原因码。
- 分位：可配并列策略（`STABLE_SPLIT` / `KEEP_TIES` / `PERCENTILE_BOUNDARY`），输出实际边界、样本数、均值收益、空组诊断。
- 多空：Q−1 组合收益、单调性、胜率、年化、Sharpe、最大回撤。
- 相关：同日同股票池的因子相关矩阵（Pearson + Rank）。
- **显著性（补齐）**：5/20 日重叠持有期用 Newey-West/HAC 或 block bootstrap 处理序列相关，
  发布 t-stat/CI/p-value；多因子并检记多重检验校正（Bonferroni/BH-FDR）。

全部统计公式用硬编码 oracle 校验，覆盖并列、常数截面、单样本、NaN/Inf、空组、零方差。

### 5.9 因子研究作为一种实验 kind

- 因子研究（覆盖率/IC/分层/相关/稳定性）由实验层 `FACTOR_STUDY` kind 编排，
  与策略回测共享同一 `Experiment → Run` 追踪主脊与比较视图（见 `第 9 章`）。
- 产物：summary / coverage / ic / quantile\_returns / long\_short\_returns / correlation（+ 可选 significance/stability）。

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

## 6. 策略层设计（A+B 扩展 / 订单驱动）

### 6.1 定位

策略层是工作台的**生产力核心**：让"加一个新策略"的成本压到最低——只写策略逻辑，
不改 runner / 回测引擎 / 基础设施。策略只跟稳定公开契约打交道：`DecisionContext`（绑定 signal\_date 的
只读窄视图 + 账户视图）与 `OrderIntent`（输出，只带整数股数）。

### 6.2 边界

**输入**（每个决策时点，`Strategy.on_event(ctx)` 的 `ctx: DecisionContext`）：

- `signal_date` / `execute_date`（T 决策、T+1 执行）。
- `data: DecisionData`——**绑定到本 signal\_date 的只读窄视图**：`bars/adjusted_bars/log_returns/
  daily_basics/factor_values/industry/security_status/stock_universe`，方法签名**均无 as\_of/end 参数**，
  策略在类型上无法请求 > signal\_date 的数据（PIT 物理边界，非通用 Repository）。
- `account: AccountView`——`cash_fen`、`positions`（正多；\[P3b-2] 负空）、`sellable`、`available_margin_fen`。
- 构造期输入：`StrategySpec`（数据/因子依赖声明、参数）。A 底座额外输入五模块配置
  （AlphaModel/RiskModel/CostModel/ConstructionModel/ConstraintSet 的 `{model_id, params}`）。

**输出**：

- `Sequence[OrderIntent]`（见 `§11.4`）：`instrument_id, side∈{BUY,SELL,SHORT_OPEN,SHORT_COVER},
  quantity(正整数), reason`。**只带整数股数，无权重字段**。可空（该日不交易）。
- A 底座（`CrossSectionalStrategy`）内部先产 `TargetWeights`，由 `WeightTargetStrategy` 基类经
  `RebalancePlanner` 翻译成整数股数 `OrderIntent`；**权重不进入引擎输入**。
- 缺依赖能力时抛 `STRATEGY_CAPABILITY_UNAVAILABLE`；选型缺前置（如 MVO 要非退化风险模型）抛 `PIPELINE_MODEL_UNAVAILABLE`。

**不负责**：撮合、账务、绩效——那是回测层与实验层的输出。

### 6.3 订单意图是唯一驱动接口

要支持截面选股、择时/CTA、配对、事件驱动四类范式，策略输出必须是**订单意图**层级，而非目标权重：

```text
策略在每个决策时点产出 OrderIntent（买/卖/开空/平仓 + 整数股数）
                       ↓
回测引擎撮合 + 账务（见 backtest-design）
```

"目标权重"是订单的**特例**：`WeightTargetStrategy` 基类把 `target_weights` 翻译成再平衡订单，
多因子/ETF 作者仍只声明权重、无感经过基类。反之——用权重表达配对做空、事件驱动离散下单——
不成立，所以底层必须是订单。这是 backtrader/zipline/vnpy 的共同选择。

### 6.4 策略扩展模型：A + B

#### 6.4.1 B — 策略即插件（底层通用口子）

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

#### 6.4.2 A — 策略即配置（截面范式的模块化底座）

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
  **ConstraintSet**——五个可插拔模块，各自设计见 §6.5。

每类模块一个注册表 `model_id → 实现`。ETF 轮动、股票多因子是"底座的两个内置配置"，不是写死特例。
换任一模块即一个新策略配置，可直接跑、直接与其他 Run 结果并排比较。

#### 6.4.3 A 与 B 的关系

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

### 6.5 五模块设计（Alpha / Risk / Cost / Construction / Constraint）

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

#### 6.5.1 AlphaModel — 预期收益 / 打分

- **职责**：把因子/信号转成每证券的截面预期收益或可比打分（越大越优）。
- **契约**：`expected_returns(ctx, universe) -> DataFrame[instrument_id, score, is_valid, reason_code]`。
  输入 `ctx.data.factor_values(...)`（已绑定 signal\_date）；无效样本保留行 + 原因码，不静默丢。
- **内置谱系**：
  - `single_factor`：单因子方向调整后直接作分。
  - `multi_factor_composite`：固定 `MAD 去极值 → 截面 zscore → 方向调整 → 类别聚合`，**复用因子层
    `transforms`，禁止另写近似**；有效因子数不足则该证券排除。
  - （后续）`ml_forecast`：模型打分，输入仍走 PIT 窄视图。
- **不变量**：只用信号日可见因子；方向与类别权重进 `config_hash`；输出可比、可 rank。

#### 6.5.2 RiskModel — 协方差 / 风险结构

- **职责**：提供组合优化所需的风险度量 Σ（或退化占位）。
- **契约**：`covariance(ctx, universe) -> CovarianceEstimate`。Σ **只由** **`available_at ≤ signal_date`
  的** **`ctx.data.log_returns(...)`** **估计**，估计窗口是参数、不得越界。
- **内置谱系**：
  - `none`（退化）：返回对角/单位占位，优化目标退化为纯打分（首版默认，配 TopN 用）。
  - `sample_cov`：样本协方差（窗口可配）。
  - `shrinkage`：Ledoit-Wolf 收缩，改善病态与小样本。
  - `factor_risk`：`Σ = BFBᵀ + D`（因子暴露 B、因子协方差 F、特异风险 D）。
- **不变量**：PIT 估计窗口；非退化模型才允许 MVO/MinVariance/RiskParity，否则 `PIPELINE_MODEL_UNAVAILABLE`。

#### 6.5.3 TransactionCostModel — 事前成本估计

- **职责**：给优化器/权重翻译提供**事前**成交成本估计，用于抑制过度换手、决定交易量。
- **契约**：`estimate(trades, ctx) -> DataFrame[instrument_id, est_cost_fen]`（整数分）。
- **内置谱系**：`fixed_bps`（按成交额 bps）/ `linear_impact`（叠加线性冲击 × 参与率）/
  `sqrt_impact`（Almgren 型 √参与率冲击）。
- **双角色一致性（硬约束）**：事前 CostModel 与回测撮合的**事后**费用（`MarketRuleBook.fees`）
  **必须由同一费率配置构造**并可对账，否则优化器对着脱节成本下单——不一致抛
  `COST_MODEL_INCONSISTENT`（详见 `§7.9`）。

#### 6.5.4 PortfolioConstructionModel — 组合构建 / 优化器

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

#### 6.5.5 ConstraintSet — 声明式约束

- **职责**：声明并强制组合约束，被优化器消费、构建后二次校验（防优化器实现违反）。
- **契约**：`apply(weights, ctx) -> weights`（裁剪到可行域）+ `validate(weights)`（越界即报错）。
- **覆盖**：个股权重上限、持仓数区间、换手上限、行业中性/暴露边界（用信号日 as-of `industry_code`，
  PIT，单成员组失效）、流动性（最小 ADV）、多空/gross/net 敞口（\[P3b-2]）。
- **不变量**：约束内容进 `config_hash`；行业约束不回填未来状态。

#### 6.5.6 装配（StrategyPipeline）

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
各模块的精确口径、内置实现与 oracle 见 `implementation.md` §4.5。

### 6.6 必须支持的四类范式

- 截面选股 → 目标权重 → 调仓（多因子、ETF 轮动）：走 A 底座。首版即可跑（多头）。
- 择时 / CTA（单标的仓位随时间变化）：走 B 插件。首个内置实现是 `dual_ma_trend`；多头择时
  首版即可，**空头腿随 P3b-2**。
- 配对交易（成对相对头寸，需做空）：走 B 插件。**纯多空对冲随 P3b-2**（依赖做空账务）。
- 事件驱动（稀疏、按事件触发的离散订单）：走 B 插件。多头版首版即可。

> 契约层面四范式都可表达；**依赖做空的腿在 P3b 解锁**（见 `§7.5`）。
> P3 阶段策略产出的 `SHORT_OPEN/SHORT_COVER` 会被引擎拒绝 `SHORT_NOT_SUPPORTED`。

### 6.7 内置双均线趋势策略（`dual_ma_trend`）

#### 6.7.1 定位与组装

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

#### 6.7.2 信号和时间语义

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

#### 6.7.3 调仓与失败恢复

正常情况下只在 `state_changed=true` 时建立新的目标暴露，避免每天因价格漂移重复调仓。若订单因
T+1 可卖、停牌、涨跌停、容量或部分成交未完成，策略保存本次目标状态，并在后续决策日仅对剩余差额
续单，直到达到 `target_tolerance`、状态再次变化或运行结束。已达到目标后不做日常漂移再平衡。
该待完成目标属于单次 Run 的确定性状态，可由此前信号和执行结果重建；Worker 重试不得依赖进程内
未持久化对象而产生不同订单。

状态改变与订单结果是两个不同事实：`state_changed` 只描述信号，拒单、部分成交和续单原因进入执行
产物，不能回写或篡改均线状态。

#### 6.7.4 严格配置

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

#### 6.7.5 产物、分析与验收

运行除通用订单、成交、账户和绩效产物外，还输出按决策日稳定排序的信号明细：

```text
signal_date, execute_date, instrument_id,
short_ma, long_ma, state, previous_state, state_changed,
target_weight, is_valid, invalid_reason
```

分析至少覆盖：LONG/FLAT 分状态收益、状态持续期、交叉次数、持仓率、换手、未成交恢复、费用拖累、
参数稳定性和相对基准表现。验收使用字面量价格序列锁定首个有效日、金叉/死叉日、相等为 FLAT、
无效窗口、T/T+1 分离、首次 LONG 建仓、死叉清仓、部分成交续单，以及 TEST 未参与参数选择。

### 6.8 错误码

```text
STRATEGY_CAPABILITY_UNAVAILABLE   策略声明的数据依赖不满足
PIPELINE_MODEL_UNAVAILABLE        选定模型缺依赖（如 MVO 要求非退化风险模型）
```

（成本一致性错误码 `COST_MODEL_INCONSISTENT` 见 `第 7 章`。）

### 6.9 包结构

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

### 6.10 依赖方向

```text
experiments → strategies → {alpha,risk,costs,portfolio} → {data,factors,backtest}
```

策略与模块只经 `ResearchDataRepository` 等只读端口取数；不导入接口层或组合根。

### 6.11 测试契约

- **扩展性（核心诉求）**：新增一个 `Strategy` 插件无需改 runner/引擎即可跑通（最小 stub 策略测）。
- **PIT**：`DecisionContext` 物理只暴露 ≤ 决策时点数据；RiskModel/CostModel 估计窗口不越界。
- **五模块**：MultiFactorComposite 固定 MAD→zscore→方向→类别聚合、复用因子层 transform；oracle。
- **权重翻译**：差额订单、整手取整、负权重→空头、清仓路径。
- **双均线**：字面量价格 oracle 锁定窗口、金叉/死叉、相等为 FLAT、`state_changed`、首次有效状态、
  无效数据不推进状态、T+1 成交和部分成交续单。
- **回归黄金结果**：etf\_rotation / stock\_multifactor / dual\_ma\_trend 固定小样本锁定输出。

### 6.12 完成定义

> 写一个新策略只需实现 `Strategy.on_event` 并注册（或对截面范式写一份组合五模块的配置），
> 不改 runner / 回测引擎 / 基础设施即可跑通、出绩效、并排比较；四类范式可表达（依赖做空的腿随 P3b-2）；
> 事前成本与回测实际成本可对账（见 `第 7 章`）。

***

## 7. 回测层设计（订单级引擎 / 多空账务）

### 7.1 定位

回测层按交易日推进时间轴，消费策略产出的 `OrderIntent`，负责**撮合 + 账务 + A 股规则 + 估值**。
它不产生信号（策略层）、不算因子（因子层）。设计优先级：回测可信 > 确定性 > 性能。

### 7.2 边界

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

### 7.3 订单级驱动

引擎的驱动输入是 `OrderIntent`——**只带整数** **`quantity`，不含权重**（理由见 `§6.3`）。
目标权重经 `RebalancePlanner` 在策略基类翻译成整数股数订单，权重不进入引擎输入。

### 7.4 时间语义与频率

- **日频驱动**：引擎按交易日推进，T 日决策、T+1 起可成交（T/T+1 严格分离）。使用 T 日收盘信息时，
  不得默认按 T 日收盘价成交。
- 订单级接口不绑定日频：未来要日内（分钟/Tick）只需引擎支持 session 内多时点驱动，
  策略 `on_event` 契约不变。本期日频。

### 7.5 分阶段：首版纯多头，公司行为与做空后置

首版不一次吃下全部复杂度。三段推进（详见 `第 12 章`）：

- **P3 纯多头无公司行为**：BUY/SELL、T+1、涨跌停、停牌、费用、滑点、容量、整手/碎股、未复权撮合、
  基础 cash/position/equity。空头订单被引擎拒绝（`SHORT_NOT_SUPPORTED`）。
- **P3b-1 公司行为**：`corporate_action` ledger——现金分红入账、送转调股。
- **P3b-2 做空**：负头寸、保证金/维持保证金、融券费、空头逐日 mark、多空分腿归因。

接口一次性预留（`OrderSide.SHORT_*`、`AccountView.available_margin_fen`），按阶段补实现，上层契约不变。

### 7.6 账务：ledger 为事实来源，equity 统一公式（无双算）

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

### 7.7 A 股撮合约束（配置化）

从 `configs/rules/a_share.yaml` 加载 `MarketRuleBook`，撮合覆盖：

- T+1 可卖；按证券/板块/日期的涨跌幅（主板 ±10%、创业板/科创板 ±20%、ST ±5%、新股无限制）。
- 停牌不可成交；涨停买入失败、跌停卖出失败（口径见下方决策点）。
- 整手买入、碎股卖出；成交量参与率容量限制；部分成交与完全无法成交。
- 上市/退市/风险状态；佣金（含最低佣金）/印花税/过户费；固定或比例滑点。（做空保证金与融券费为 P3b-2。）

**决策点（涨跌停口径）**：`ExecutionConfig.limit_fill_policy ∈ {WHOLE_DAY_SEALED, REFERENCE_AT_LIMIT}`，
**默认** **`REFERENCE_AT_LIMIT`（保守，符合"回测可信/避免过度乐观"）**；`WHOLE_DAY_SEALED` 更宽松
（仅全天封死才拒绝）。

### 7.8 撮合价与复权口径

- 撮合用**未复权价**（真实成交价）+ 公司行为事件补偿账务。
- 因子/信号侧用**前复权序列**。二者口径分离，不可混用。

### 7.9 交易成本双角色一致性（硬约束）

- **事前成本**（策略层 CostModel）：优化器/权重翻译时决定交易量。
- **事后成本**（本层撮合的费用 + 滑点 + 借券费）：实际扣减账户。
- 二者**必须共享费率参数**，否则策略对着与实际脱节的成本模型下单。一致性测试：同一笔成交，
  事前估计与事后实际在同参数下逐项对账；不一致抛 `COST_MODEL_INCONSISTENT`。

### 7.10 输出产物

逐日：`nav`（cash/多头市值/空头负债/保证金占用/nav/benchmark\_close）、`holdings`（逐标的净头寸/
可卖/成本/市值）、`fills`（成交与拒绝 + reason\_code）、`costs`（佣金/印花税/过户费/融券费）。
供分析层消费。

### 7.11 包结构

```text
src/quant_research/backtest/
├── engine.py       # 逐日主循环
├── execution.py    # ExecutionModel 撮合
├── accounting.py   # PortfolioAccount 多空账务 + ledger
├── rulebook.py     # MarketRuleBook 规则/费率
├── calendar.py     # 交易日历
└── models.py       # OrderIntent/AccountView/AccountSnapshot 等 DTO
```

### 7.12 依赖方向

回测层被 `strategies` 与 `experiments` 依赖；自身只经 `ResearchDataRepository` 取行情/状态/公司行为，
不导入接口层或组合根。

### 7.13 测试契约

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

### 7.14 完成定义

> 首版（P3，纯多头无公司行为）：引擎按 `OrderIntent`（整数股数）驱动，正确处理 T+1、涨跌停、停牌、
> 费用、滑点、容量、整手/碎股；`equity = cash + long_market_value − accrued_fees` 恒等式与双向对账成立；
> 撮合用未复权价、信号用前复权价；事前成本与事后成本可对账。
> P3b-1：分红送转除权除息正确（除权日 NAV 不失真）。
> P3b-2：做空/保证金/融券费/空头逐日 mark 与多空分腿在负头寸场景成立，equity 增 `− short_market_value` 项且无双算。

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

**输入**（回测层产物，见 `§7.10`）：

- `nav`：`trade_date, cash_fen, long_mv_fen, equity_fen, benchmark_close`（\[P3b-2] 增 `short_mv_fen`、`accrued_fees_fen`）。
- `holdings`：逐日逐标的头寸/可卖/成本/市值。
- `fills`：成交与拒绝 + `reason_code`。
- `costs`：佣金/印花税/过户费（\[P3b-2] 融券费）。
- 配置：`sessions_per_year`(默认 252)、基准标识、切片规则（年/月/滚动窗口）。

**输出**（供实验层落盘、Dashboard 展示）：

- `performance.json`：标量绩效指标 + `undefined_metrics`（未定义项显式记录，不填 0/NaN）。
- `drawdown` / `monthly_returns` / `annual_returns`：时序表。
- `attribution`：归因表（期间/风格/个股；\[P3b-2] 多空分腿）。
- `execution_summary`：按方向 × 原因码汇总的成交质量。

**不负责**：信号/因子/撮合/账务；跨运行复现；实盘归因。

### 8.3 模块划分

```text
回测产物(nav/holdings/fills/costs)
   ├─► PerformanceMetrics   收益/风险标量 + 时序（净值/回撤/年月）
   ├─► ExecutionQuality     换手/费用拖累/成交失败统计
   ├─► RiskAnalytics        波动/beta/暴露/敞口（[P3b-2] gross/net、多空分腿）
   └─► Attribution          期间/风格/个股收益归因
```

四个模块都是纯函数：`f(产物表, 配置) -> 结果表`，确定性、稳定排序、可字面量 oracle 校验。

### 8.4 PerformanceMetrics — 绩效

- **收益**：累计收益、年化收益、基准/超额收益（几何超额）。
- **风险调整**：年化波动、Sharpe、Sortino、Calmar、信息比率(IR)、beta、Jensen alpha、tracking error。
- **回撤**：回撤序列、最大回撤及其峰/谷/恢复日、水下时长、水下占比。
- **成本视角**：gross vs net 收益、费用拖累（累计/年化）。
- **时序表**：逐日净值/回撤、月度收益、年度收益。

**口径铁律**（易错点，实现见 `implementation.md` §5.8）：

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
experiments → analytics → （只消费回测产物表，不反向依赖回测引擎运行态）
```

分析层只读回测产物文件/DataFrame；不导入接口层、组合根、回测引擎内部状态。被实验层
`ANALYTICS` 阶段调用，结果并入 Run 产物目录。

### 8.10 包结构

```text
src/quant_research/analytics/
├── performance.py   # PerformanceMetrics：收益/风险标量 + 净值/回撤/年月时序
├── execution.py     # ExecutionQuality：换手/费用/成交失败汇总
├── risk.py          # RiskAnalytics：波动/beta/暴露/敞口（[P3b-2] 多空分腿）
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

## 9. 实验层设计（编排 / 追踪 / 比较）

### 9.1 定位与范围

实验层是工作台的**编排核心**：编排"数据只读读取 → 策略产生订单 → 回测撮合账务 → 绩效分析 →
结果落盘"，并统一追踪、比较、结论标记。它调度策略层与回测层，自身不产订单、不撮合。

**本文明确不做**（对齐 project-intent；此处指**跨运行复现回放**，非去掉所有哈希——
数据完整性/增量/运行内一致性哈希仍保留，边界见 `§3.4.2`）：

- 不做 `run_identity` 内容身份哈希、source/env 指纹链、产物 manifest 逐文件 SHA-256 校验、
  可选择历史 catalog——复现回放的重机制一律不建。重跑允许覆盖。
- 不做分布式 / 多用户 / 远程 registry / model serving。

设计优先级：PIT 正确 > 回测可信 > 策略迭代低摩擦 > 结果可比较 > 性能。

### 9.2 边界

**输入**：

- 实验配置（YAML，Pydantic 严格校验）：`kind∈{STRATEGY_BACKTEST, FACTOR_STUDY}`、`name`、
  策略/因子配置、`start_date/end_date`、`benchmark`、`initial_cash_fen`、`execution`、`sample_windows`。
- 装配依赖（组合根注入）：`ResearchDataRepository`、`FactorEngine`、`StrategyRegistry`、
  `BacktestEngine`、`FactorStudyStore`、`TaskQueue`。
- 当前 `catalog_version`（运行开始捕获，用于运行内一致性门）。

**输出**：

- **元数据**（SQLite）：`Experiment / Run`（`config_json` 冻结快照、`status`、`catalog_version`、
  `uses_test_region`、`research_mark`、`artifact_dir`、`error_json`）、`run_metric`、`audit_event`、
  `task/task_attempt`。
- **产物目录** `<artifact_root>/<experiment_id>/<run_id>/`：回测 kind 输出
  `nav/holdings/fills/costs/performance/attribution`；因子 kind 输出
  `summary/coverage/ic/quantile_returns/long_short_returns/correlation`；+ `config.json/metrics.json`。
- **读侧视图**：排行榜、配置/指标 diff、血缘、结论标记（供 Dashboard/CLI）。
- 数据版本漂移/阶段失败/取消/状态冲突时抛 `EXPERIMENT_DATA_DRIFT / EXPERIMENT_STAGE_FAILED /
  EXPERIMENT_CANCELLED / EXPERIMENT_STATE_CONFLICT`。

### 9.3 追踪实体（轻量版，去复现）

```text
Experiment（研究命名空间 / 意图 / 标签 / 结论 / baseline 指针）
   └── Run（一次执行；记录配置快照、状态、指标、产物指针）
```

- 无 Stage 内容寻址、无 run\_identity。Run 只需 `run_id`(ULID) 作记录键。
- **Run 记录**：`config_json`(冻结快照，便于比较)、`status`、`catalog_version`(记录提交时数据版本，
  **用于展示与运行内一致性，不用于复现回放**)、`metrics`、`artifact_dir`、`error_json`。
- 重跑允许覆盖同一 Run 或生成新 Run（配置项，不强制"结果不可覆盖"）。
- 统一因子研究（`FACTOR_STUDY`）与策略回测（`STRATEGY_BACKTEST`）到同一追踪主脊与比较视图。

### 9.4 状态机

```text
CREATED → QUEUED → RUNNING → SUCCEEDED
                        ├→ FAILED
                        └→ CANCELLED
```

乐观并发迁移（携期望前态，CAS）；`SUCCEEDED` 前产物必须已落地；`FAILED` 只存结构化错误码 +
安全上下文；一个 Run 只绑一个任务，重试建新 Run + 新任务。

### 9.5 编排：kind 无关的阶段执行器

```text
STRATEGY_BACKTEST: VALIDATE → PREPARE_INPUTS → STRATEGY_RUN(交织 BACKTEST) → ANALYTICS → PERSIST
FACTOR_STUDY:      VALIDATE → PREPARE_INPUTS → ANALYZE_FACTORS → PERSIST
```

- 截面策略在 `PREPARE_INPUTS` 内先算股票池/因子/管线信号；择时/事件驱动此阶段可能只 warmup。
  差异由 `Strategy.spec` 声明的数据依赖驱动，runner 本身 kind 无关。
- **STRATEGY\_RUN 与 BACKTEST 交织**：引擎按交易日推进，每个决策时点调 `strategy.on_event(ctx)`
  取订单；`ctx` 只暴露 PIT 可见信息（防未来函数的物理边界，由回测层保证）。
- 每阶段边界检查协作取消；失败/取消不留半成品目录。

### 9.6 运行内一致性门（非复现）

运行开始记录当前 `catalog_version`；每阶段前后校验未被并发更新，变则 `EXPERIMENT_DATA_DRIFT` 失败。
作用是"这次运行不混用两批数据"，不是复现回放（不存历史版本、不回放）。

### 9.7 产物与登记

产物目录 `<artifact_root>/<experiment_id>/<run_id>/`，普通目录写盘（无逐文件内容寻址校验）：
staging 临时目录 → 原子 `rename` 到最终目录。基本完整性检查（存在、可读、行数>0）即可。
`PERSIST` 成功后才 `RUNNING→SUCCEEDED` 并写 `artifact_dir`。

### 9.8 分析层（详见 `第 8 章`）

`ANALYTICS` 阶段调用分析层，从回测产物（nav/holdings/fills/costs）计算绩效（累计/年化收益、波动、
Sharpe/Sortino/Calmar、最大回撤与恢复、IR、beta/alpha）、交易质量、风险与暴露、归因（期间/风格/个股；
多空分腿与 gross/net 敞口为 P3b-2）。因子研究则计算覆盖率/IC/分层/多空/相关/显著性（因子层 §5.7-5.8）。
全部统计公式字面量 oracle；首日 0 收益口径明确（见 `§8.4`、`implementation.md` §5.8）。

### 9.9 防过拟合治理（保留）

- 样本区间锁定：配置声明 `train/validation/test`；Run 提交计算 `uses_test_region`；
  Experiment 累计 test 预算消耗并在 Dashboard 显式展示（样本外偷看可见可审计）。
- 多重检验记账：Experiment 记录尝试 Run 数、参数组合数、校正方法（Bonferroni/BH-FDR）；
  显著性报告据此校正。
- 不硬阻断超预算 Run（保留研究灵活性），只做可见可审计。

### 9.10 比较 / 血缘

- 排行榜：同 Experiment 按指定 metric 排序 Run。
- 配置 diff：两 Run 的 `config_json` 结构化差异。
- 血缘：Run → `catalog_version`（数据版本，展示用）→ 产物目录，追溯到唯一 Run。
- 结论标记：`research_mark` = BASELINE/CANDIDATE/DISCARDED；`baseline_run_id` 指精确 Run，不回退所属实验最新 Run。

### 9.11 错误码

```text
EXPERIMENT_DATA_DRIFT     运行内数据版本被并发改动
EXPERIMENT_STAGE_FAILED   阶段执行失败（含内层原因码）
EXPERIMENT_CANCELLED      协作取消
EXPERIMENT_STATE_CONFLICT 乐观并发状态冲突
```

### 9.12 包结构

```text
src/quant_research/experiments/
├── models.py     # Experiment/Run、状态机
├── config.py     # 实验+策略 YAML 校验与冻结
├── runner.py     # kind 无关阶段执行器
├── graph.py      # 阶段图声明
├── ports/        # market/factor/universe 只读端口
├── contracts.py  # Store 端口
└── query.py      # 比较 / 排行榜 / 结论标记
```

### 9.13 依赖方向

```text
bootstrap → application → experiments → strategies → {alpha,risk,costs,portfolio} → {data,factors,backtest}
```

实验层经只读端口取数与调用引擎；不导入接口层或组合根。

### 9.14 决策记录

| # | 决策                       | 结论                        |
| - | ------------------------ | ------------------------- |
| A | 防过拟合护栏（test 预算 + 多重检验记账） | **保留**，服务"结论可信"，非复现       |
| B | 频率                       | 仅日频，订单接口已为日内预留            |
| C | 模块建设节奏                   | 渐进：先端口+退化实现打通，再补 MVO/风险模型 |
| D | 重跑语义                     | 允许覆盖（去复现后无需强制新 Run）       |

### 9.15 测试契约

- **状态机**：合法迁移通过、非法迁移 `EXPERIMENT_STATE_CONFLICT`、并发 CAS 不覆盖。
- **一致性门**：运行中 catalog\_version 变 → `EXPERIMENT_DATA_DRIFT`；前后双校验。
- **阶段**：失败/取消不留半成品；`SUCCEEDED` 前产物已落地。
- **指标 oracle**：Sharpe/Sortino/Calmar/回撤/IR/beta；首日 0 口径；undefined 显式记录。
- **治理**：`uses_test_region` 标记、test 预算计数、多重检验记账。
- **kind 复用**：FACTOR\_STUDY 与 STRATEGY\_BACKTEST 共用同一 runner。

### 9.16 完成定义

> 因子研究与策略回测共享同一 `Experiment→Run` 追踪主脊与比较视图；每阶段前后校验数据版本
> （运行内一致性）；产物原子发布、`SUCCEEDED` 前落地；绩效指标字面量 oracle；样本外使用与
> 试验次数可审计。

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
| `FACTOR_ANALYSIS`   | 实验中心     | `ExperimentRunner`（FACTOR\_STUDY kind）      | 绑定 Run  |
| `STRATEGY_BACKTEST` | 实验中心     | `ExperimentRunner`（STRATEGY\_BACKTEST kind） | 绑定 Run  |

要点：`run_id` **可空**——数据类任务无实验 Run（这修正了"task.run\_id NOT NULL"的过窄约束）。
payload 是该任务类型的冻结参数（如 DATA\_UPDATE 的固化计划 `plan_hash` + 窗口）。

### 10.3 生命周期与状态机

```text
QUEUED → RUNNING → SUCCEEDED
              ├→ FAILED
              └→ CANCELLED
```

- 提交即 `QUEUED`（可带 `available_at` 延迟可见）。
- 领取（claim）以 CAS 置 `RUNNING` + `worker_id` + `locked_at`，并开一条 `task_attempt`。
- 终态写 `completed_at`；失败写 `error_json`；重试创建**新 attempt 或新 task**（见 §10.6）。
- 每次状态迁移携期望前态，乐观并发；影响行数为 0 即冲突失败，绝不 last-writer-wins。

### 10.4 领取与租约（claim / heartbeat / lease）

- **领取**：`SELECT ... WHERE status='QUEUED' AND (available_at IS NULL OR available_at<=now)
  ORDER BY priority DESC, created_at ASC LIMIT 1`，随即 CAS 抢占。
- **心跳**：RUNNING 期间周期更新 `heartbeat_at`（租约续期）。
- **陈旧回收**：`heartbeat_at` 超过 `lease_timeout` 的 RUNNING 任务视为 Worker 崩溃，可被重新领取；
  回收时开新 attempt，旧 attempt 标 `FAILED(lease_expired)`。防止崩溃任务永久占位。

### 10.5 幂等

- `idempotency_key` UNIQUE：相同键的重复提交收敛为同一 task（如 `run-<run_id>`、
  `data-update-<plan_hash>`）。提交竞态由唯一约束兜底。
- 所有任务**必须幂等**：handler 重跑不产生重复数据或冲突结果（数据层按内容寻址去重、
  实验重跑覆盖或建新 Run）。这是"进程重启后识别未完成任务、避免重复写入"的前提。

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
- **租约回收**：心跳超时的 RUNNING 被回收重领；旧 attempt 标 lease\_expired。
- **幂等**：相同 idempotency\_key 收敛为一 task；handler 重跑不产生重复副产物。
- **取消**：阶段边界协作退出，不留 staging 半成品目录。
- **重试**：数据类复用固化计划；实验类建新 Run 不覆盖旧 Run。
- **run\_id 可空**：DATA\_UPDATE/DATA\_VALIDATION 任务无 Run 也能全生命周期流转。
- **崩溃恢复**：进程重启识别未完成 RUNNING（超租约）任务，安全重领不重复写。

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
    def catalog_version(self) -> str: ...   # 运行内一致性用，非复现
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

### 11.4 策略层（B：统一契约）

> 契约预留做空：`OrderSide` 含 `SHORT_*`、`AccountView` 含 `available_margin_fen`、`LedgerEventType`
> 含 `SHORT_*/BORROW_FEE/MARGIN_*`。首版（P3）引擎拒绝空头订单（`SHORT_NOT_SUPPORTED`），
> 做空账务在 **P3b-2** 实现——契约不变，只补实现（见 `第 12 章`）。

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

### 11.5 策略层（A：截面五模块）

```python
class AlphaModel(Protocol):
    def expected_returns(self, ctx: DecisionContext, universe: Sequence[InstrumentId]) -> pl.DataFrame: ...
    # 列: instrument_id, expected_return|score, is_valid, reason_code

class RiskModel(Protocol):
    def covariance(self, ctx: DecisionContext, universe: Sequence[InstrumentId]) -> "CovarianceEstimate": ...
    # NoRisk 退化: 返回单位/对角占位，优化器据此退化为纯打分

class TransactionCostModel(Protocol):
    def estimate(self, trades: pl.DataFrame, ctx: DecisionContext) -> pl.DataFrame: ...
    # 事前成本; 费率参数必须与回测 rulebook 同源（见 §11.7 一致性）

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

### 11.6 回测引擎与账务

> 分阶段（见 `第 12 章`）：
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

### 11.7 成本双角色一致性

`TransactionCostModel`（事前）与 `MarketRuleBook.fees/borrow_fee`（事后）**必须由同一费率配置构造**。
一致性测试：同一笔成交，事前估计与事后实际的费用项在同参数下逐项对账；不一致抛
`COST_MODEL_INCONSISTENT`。

### 11.8 实验层

```python
class ExperimentKind(StrEnum): FACTOR_STUDY=...; STRATEGY_BACKTEST=...
class RunStatus(StrEnum): CREATED=...; QUEUED=...; RUNNING=...; SUCCEEDED=...; FAILED=...; CANCELLED=...

class FactorStudyStore(Protocol):        # 消费者侧持久化端口
    def create_experiment(self, name: str, kind: ExperimentKind, config: Mapping) -> str: ...
    def create_run(self, experiment_id: str, config_snapshot: Mapping, catalog_version: str) -> str: ...
    def transition(self, run_id: str, expected: RunStatus, target: RunStatus, **terminal) -> None: ...
    def record_metrics(self, run_id: str, metrics: Mapping[str, float]) -> None: ...
    def get_run(self, run_id: str) -> Mapping[str, object]: ...
    def list_runs(self, experiment_id: str) -> Sequence[Mapping[str, object]]: ...

class StageGraph(Protocol):
    def stages(self, kind: ExperimentKind) -> tuple["Stage", ...]: ...

class ExperimentRunner:
    def run(self, run_id: str, progress, cancellation) -> "RunResult": ...
    # 阶段: VALIDATE → PREPARE_INPUTS → STRATEGY_RUN → BACKTEST → ANALYTICS → PERSIST
    # 每阶段前后校验 catalog_version 未变(运行内一致性)，变则 EXPERIMENT_DATA_DRIFT
```

#### 11.8.1 SQLite 表（Run 无 run\_identity 复现哈希；仍存 catalog\_version）

```text
experiment(id PK, name, kind, description, baseline_run_id NULL, created_at, updated_at)
experiment_tag(experiment_id FK, tag)
run(id PK, experiment_id FK, status, config_json, catalog_version,
    uses_test_region, research_mark, artifact_dir NULL,
    created_at, queued_at, started_at, completed_at, error_json NULL)
run_metric(id PK, run_id FK, name, value, unit, created_at)
run_tag(run_id FK, tag)
audit_event(id PK, experiment_id FK NULL, run_id FK NULL, event_type, details_json, created_at)
task(id PK, run_id FK, task_type, payload_json, status, priority,
     idempotency_key, worker_id, heartbeat_at, created_at, ...)
task_attempt(id PK, task_id FK, attempt_no, status, started_at, completed_at, error_json NULL)
```

产物目录 `<artifact_root>/<experiment_id>/<run_id>/`：普通目录写盘（无逐文件内容寻址校验），
含 kind 相应 parquet + `config.json` + `metrics.json`。

### 11.9 防过拟合治理

```python
@dataclass(frozen=True, slots=True)
class SampleWindows:
    train: tuple[date, date]; validation: tuple[date, date]; test: tuple[date, date]

# Run 提交时计算 uses_test_region（是否覆盖 test 区间）→ Experiment 累计 test 预算计数
# 多重检验记账: Experiment 记录尝试 Run 数/参数组合数/校正方法(Bonferroni|BH_FDR)
```

### 11.10 错误码族（进程边界统一）

```text
DATA_QUALITY_GATE_CLOSED / DATA_SOURCE_CONTRACT
FACTOR_CAPABILITY_UNAVAILABLE
STRATEGY_CAPABILITY_UNAVAILABLE / PIPELINE_MODEL_UNAVAILABLE / COST_MODEL_INCONSISTENT
SHORT_NOT_SUPPORTED                         # 首版(P3)引擎拒绝空头订单；P3b-2 启用做空后不再抛
EXPERIMENT_DATA_DRIFT / EXPERIMENT_STAGE_FAILED / EXPERIMENT_CANCELLED / EXPERIMENT_STATE_CONFLICT
```

### 11.11 依赖方向（AST 门禁强制）

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
`infrastructure/` SQLite + SQLAlchemy + Alembic 初始迁移（`§11.8.1` 全部表；
`task`/`task_attempt` 以 `implementation.md` §6.3 为准，`run_id` 可空）；
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
- `CanonicalResearchRepository`（PIT 物理截断 + `catalog_version`）。
- **通用 Worker 队列**（首次落地）：`TaskQueue` + CAS 领取/心跳/租约回收/幂等 + kind 无关主循环 +
  `DATA_UPDATE`/`DATA_VALIDATION` handler（详见 `第 10 章` / `implementation.md` 第 6 章）。数据更新/校验
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

- `Experiment/Run` 模型 + 状态机 + `FactorStudyStore`（SQLite）。
- `StageGraph` + kind 无关 `ExperimentRunner`：`VALIDATE→PREPARE_INPUTS→STRATEGY_RUN→BACKTEST→ANALYTICS→PERSIST`。
- 运行内一致性门（catalog\_version 前后校验，漂移即失败）。
- `analytics/`：绩效（Sharpe/Sortino/Calmar/回撤/IR/beta/alpha）、成交质量、归因（多空分腿随 P3b）。
- Worker：实验任务 handler（`FACTOR_ANALYSIS`/`STRATEGY_BACKTEST`）接入**通用 Worker 队列**——队列本身
  （`task`/`task_attempt` 表、CAS 领取、心跳/租约、幂等、主循环）在 P0/P1 已建（数据类任务先用），
  P5 只注册实验 handler（详见 `第 10 章` / `implementation.md` 第 6 章）。CLI `quant worker once|run` + 提交实验。
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
`§6.7.5` 的信号、时序和续单 oracle；证明订单级接口对异构范式充分；配对纯对冲随 P3b-2
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
