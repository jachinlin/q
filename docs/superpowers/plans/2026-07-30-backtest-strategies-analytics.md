# 回测引擎、策略与分析实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 实现适用于 A 股日频研究的轻量自研回测引擎、ETF 轮动与股票多因子策略，并生成可复现的标准回测和分析产物。

**架构：** 使用“交易日事件循环 + 当日横截面向量计算”。策略只生成 `TargetPortfolio`；组合构造器负责约束；成交模型执行停牌、涨跌停、滑点、容量和整手检查；会计模型维护现金、T+1、持仓和费用；分析模块只依赖标准产物。Polars、DuckDB、PyArrow、NumPy 提供计算能力，不引入 Qlib/RQAlpha 作为生产内核；后者仅可用于离线差异验证。

**技术栈：** Python 3.12、Polars、DuckDB、PyArrow、NumPy、Pydantic v2、pytest、Hypothesis；可选 Numba 只用于性能热点。

## 前置条件与约束

- 先完成数据平台、时点股票池与因子系统两份计划。
- 所有输入绑定 `SnapshotId`，所有交易规则绑定 `rulebook_version`。
- T 日收盘后生成信号，最早在 T+1 成交；`signal_date` 与 `execute_date` 不得合并。
- 现金和费用以整数“分”记账；分析价格可使用 `Float64`。
- 任何未成交、部分成交和约束失败都必须记录原因码。
- 默认采用现金精确模式；复权收益快速模式必须在结果中明确披露。

---

## 文件职责

| 路径 | 作用 |
|---|---|
| `src/quant_core/strategies/base.py` | 策略协议、上下文与校验 |
| `src/quant_core/strategies/etf_rotation.py` | ETF 排名、趋势过滤和目标权重 |
| `src/quant_core/strategies/multifactor.py` | 多因子打分、选股和权重 |
| `src/quant_core/portfolio/constraints.py` | 个股、行业、持仓数、换手、流动性约束 |
| `src/quant_core/portfolio/constructor.py` | 得分到目标组合的转换 |
| `src/quant_core/portfolio/rebalance.py` | 目标权重到订单意图的转换 |
| `src/quant_core/backtest/calendar.py` | 信号日、执行日和交易日定位 |
| `src/quant_core/backtest/rulebook.py` | 版本化 A 股交易规则 |
| `src/quant_core/backtest/execution.py` | 停牌、涨跌停、滑点、容量与整手成交 |
| `src/quant_core/backtest/accounting.py` | 现金、持仓、T+1、费用和公司行动 |
| `src/quant_core/backtest/engine.py` | 日期循环和组件编排 |
| `src/quant_core/backtest/artifacts.py` | 标准产物流式写入 |
| `src/quant_core/analytics/*.py` | 绩效、归因和 Dashboard 摘要物化 |

### 任务 1：交易日历与版本化 RuleBook

**文件：**
- 新建：`src/quant_core/backtest/__init__.py`
- 新建：`src/quant_core/backtest/calendar.py`
- 新建：`src/quant_core/backtest/rulebook.py`
- 新建：`configs/rules/a_share_v1.yaml`
- 新建：`tests/unit/backtest/test_calendar.py`
- 新建：`tests/unit/backtest/test_rulebook.py`

**接口：**

```python
class TradingCalendar:
    def next_session(self, trade_date: date) -> date: ...
    def sessions(self, start: date, end: date) -> tuple[date, ...]: ...


class MarketRuleBook(Protocol):
    @property
    def version(self) -> str: ...
    def lot_size(self, instrument: InstrumentId, trade_date: date) -> int: ...
    def earliest_sell_date(self, buy_date: date, instrument: InstrumentId) -> date: ...
    def price_limits(
        self,
        instrument: InstrumentId,
        trade_date: date,
        prev_close: float,
        status: SecurityStatus,
    ) -> PriceBand | None: ...
    def fees(self, fill: SimulatedFill) -> FeeBreakdown: ...
```

- [ ] **步骤 1：先写交易日与规则边界测试**

覆盖周末、春节休市、最后一个交易日越界、T+1 可卖日期、主板/创业板/科创板不同日期的涨跌幅、ST 状态、买入无印花税、卖出印花税、佣金最低收费和过户费。所有期望值写成显式常量。

- [ ] **步骤 2：运行并确认模块不存在**

运行：`uv run pytest tests/unit/backtest/test_calendar.py tests/unit/backtest/test_rulebook.py -v`

- [ ] **步骤 3：实现日历和配置驱动规则**

日历从快照的 `trade_calendar` 加载。规则配置按生效日期、板块和证券状态匹配；加载时校验时间区间不重叠且完整。回测请求只保存版本，不读取“当前规则”。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/backtest/test_calendar.py tests/unit/backtest/test_rulebook.py -v`

```bash
git add src/quant_core/backtest configs/rules/a_share_v1.yaml tests/unit/backtest
git commit -m "feat: add trading calendar and versioned A-share rules"
```

### 任务 2：目标组合、约束与调仓计划

**文件：**
- 新建：`src/quant_core/portfolio/__init__.py`
- 新建：`src/quant_core/portfolio/constraints.py`
- 新建：`src/quant_core/portfolio/constructor.py`
- 新建：`src/quant_core/portfolio/rebalance.py`
- 新建：`tests/unit/portfolio/test_constructor.py`
- 新建：`tests/unit/portfolio/test_rebalance.py`

**接口：**

```python
@dataclass(frozen=True)
class TargetPosition:
    instrument_id: InstrumentId
    target_weight: float
    score: float | None
    reason_code: str


@dataclass(frozen=True)
class TargetPortfolio:
    signal_date: date
    execute_date: date
    positions: tuple[TargetPosition, ...]
    cash_weight: float


class PortfolioConstructor:
    def construct(
        self,
        candidates: pl.DataFrame,
        constraints: PortfolioConstraints,
        signal_date: date,
        execute_date: date,
    ) -> TargetPortfolio: ...
```

- [ ] **步骤 1：先写约束优先级和不可行测试**

验证个股上限、行业上限、最少/最多持仓数、流动性、最大换手和权重和为 1。约束冲突时必须返回 `ConstraintViolation`，其中包含约束名、实际值与边界，不得静默放宽。

- [ ] **步骤 2：写调仓取整测试**

给定现金、当前持仓、目标权重和执行价，验证卖单先于买单、股数按整手向下取整、零股不产生订单、残余现金被保留、同证券只产生一个净订单意图。

- [ ] **步骤 3：实现并测试**

运行：`uv run pytest tests/unit/portfolio -v`

预期：全部通过，Hypothesis 属性测试证明目标权重非负且总和不超过 1（容差 `1e-10`）。

- [ ] **步骤 4：提交**

```bash
git add src/quant_core/portfolio tests/unit/portfolio
git commit -m "feat: construct constrained target portfolios"
```

### 任务 3：成交模型

**文件：**
- 新建：`src/quant_core/backtest/models.py`
- 新建：`src/quant_core/backtest/execution.py`
- 新建：`tests/unit/backtest/test_execution.py`

**接口：**

```python
class ExecutionModel:
    def execute(
        self,
        intents: Sequence[OrderIntent],
        market: MarketSlice,
        account: AccountView,
        rulebook: MarketRuleBook,
        config: ExecutionConfig,
    ) -> ExecutionBatch: ...
```

- [ ] **步骤 1：写成交原因码真值表**

覆盖 `SUSPENDED`、`LIMIT_UP_BUY_BLOCKED`、`LIMIT_DOWN_SELL_BLOCKED`、`INSUFFICIENT_CASH`、`VOLUME_CAP`、`ODD_LOT`、`NO_MARKET_DATA`、完全成交和部分成交。涨跌停使用前收盘与规则价格带，不能仅比较当日 high/low。

- [ ] **步骤 2：写滑点与容量数值测试**

断言买入滑点提高价格、卖出滑点降低价格；最大成交量为当日成交量乘参与率；应用滑点后再次校验现金；成交价不得越过涨跌停边界。

- [ ] **步骤 3：实现向量化成交批次**

市场过滤和容量上限用 Polars 一次计算；只在最终费用精确计算阶段遍历成交集合。每个输入意图恰好产生一个 `FillResult` 或 `RejectResult`。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/backtest/test_execution.py -v`

```bash
git add src/quant_core/backtest/models.py src/quant_core/backtest/execution.py tests/unit/backtest/test_execution.py
git commit -m "feat: simulate A-share daily executions"
```

### 任务 4：会计模型、T+1 与公司行动

**文件：**
- 新建：`src/quant_core/backtest/accounting.py`
- 新建：`tests/unit/backtest/test_accounting.py`
- 新建：`tests/regression/test_accounting_ledger_golden.py`

**接口：**

```python
class PortfolioAccount:
    def begin_session(
        self, trade_date: date, actions: Sequence[CorporateAction]
    ) -> None: ...
    def apply(self, execution: ExecutionBatch) -> None: ...
    def mark_to_market(
        self, trade_date: date, closes: Mapping[InstrumentId, float]
    ) -> AccountSnapshot: ...
```

- [ ] **步骤 1：写复式不变量与 T+1 测试**

买入当日增加总持仓但不增加可卖持仓；下一交易日解锁。每批成交后断言 `期末现金 = 期初现金 - 买入金额 + 卖出金额 - 费用 + 现金行动`；净值等于现金加市值；不得出现负现金或负持仓。

- [ ] **步骤 2：写分红、送股和除权测试**

验证现金分红以持股登记日数量为准，送股调整总股数和成本基础，公司行动处理具备事件 ID 幂等性，同一事件不能应用两次。

- [ ] **步骤 3：实现整数分账本**

所有现金和费用使用整数分；成交金额按明确的四舍五入规则转换。持仓批次记录买入日和可卖日。生成不可变账本事件，`AccountSnapshot` 由账本归约得到。

- [ ] **步骤 4：运行黄金账本测试并提交**

运行：`uv run pytest tests/unit/backtest/test_accounting.py tests/regression/test_accounting_ledger_golden.py -v`

```bash
git add src/quant_core/backtest/accounting.py tests/unit/backtest/test_accounting.py tests/regression/test_accounting_ledger_golden.py
git commit -m "feat: add precise portfolio accounting"
```

### 任务 5：回测引擎与标准产物

**文件：**
- 新建：`src/quant_core/backtest/engine.py`
- 新建：`src/quant_core/backtest/artifacts.py`
- 新建：`tests/integration/test_backtest_timeline.py`
- 新建：`tests/integration/test_backtest_cancellation.py`
- 新建：`tests/regression/test_backtest_golden.py`

**接口：**

```python
@dataclass(frozen=True)
class BacktestRequest:
    experiment_id: UUID
    snapshot_id: UUID
    strategy: Strategy
    start_date: date
    end_date: date
    benchmark: InstrumentId
    initial_cash_fen: int
    rulebook_version: str
    execution_config: ExecutionConfig


class BacktestEngine:
    def run(
        self,
        request: BacktestRequest,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> BacktestResult: ...
```

- [ ] **步骤 1：写 T/T+1 时间线失败测试**

使用只在 T 日收盘后出现的信号，断言目标的 `signal_date=T`、`execute_date=T+1`，成交使用 T+1 配置的参考价，绝不使用 T 日收盘价成交。

- [ ] **步骤 2：写取消和异常完整性测试**

取消令牌在交易日边界生效；取消或异常时关闭 Parquet writer，保留诊断日志和临时状态，但不得发布成功 manifest。

- [ ] **步骤 3：实现日期级编排与流式产物**

每个交易日按 `公司行动 → 解锁可卖数量 → 执行前日目标 → 记账 → 收盘估值 → 生成次日目标 → 进度/取消检查` 顺序执行。产物包含 `nav`、`holdings`、`targets`、`fills`、`costs`，先写临时目录，全部校验后发布 `manifest.json`。

- [ ] **步骤 4：运行时间线和黄金回归**

运行：`uv run pytest tests/integration/test_backtest_timeline.py tests/integration/test_backtest_cancellation.py tests/regression/test_backtest_golden.py -v`

- [ ] **步骤 5：提交**

```bash
git add src/quant_core/backtest/engine.py src/quant_core/backtest/artifacts.py tests/integration/test_backtest_timeline.py tests/integration/test_backtest_cancellation.py tests/regression/test_backtest_golden.py
git commit -m "feat: run reproducible daily backtests"
```

### 任务 6：ETF 轮动与股票多因子策略

**文件：**
- 新建：`src/quant_core/strategies/__init__.py`
- 新建：`src/quant_core/strategies/base.py`
- 新建：`src/quant_core/strategies/etf_rotation.py`
- 新建：`src/quant_core/strategies/multifactor.py`
- 新建：`configs/experiments/examples/etf_rotation.yaml`
- 新建：`configs/experiments/examples/multifactor.yaml`
- 新建：`tests/unit/strategies/test_etf_rotation.py`
- 新建：`tests/unit/strategies/test_multifactor.py`

**接口：**

```python
class Strategy(Protocol):
    strategy_id: str
    version: str

    def validate(self, ctx: StrategyContext) -> list[ValidationIssue]: ...
    def generate_targets(
        self, ctx: StrategyContext, rebalance_date: date, current: PortfolioState
    ) -> TargetPortfolio: ...
```

- [ ] **步骤 1：写 ETF 策略测试**

按配置组合收益、趋势和波动因子；趋势过滤未通过时转现金；按得分稳定排序并处理并列；只选择前 N；信号缺失时遵循配置的剔除规则；每月最后一个交易日调仓。

- [ ] **步骤 2：写多因子策略测试**

只从当日历史股票池取样；执行去极值、中性化、标准化、方向调整和加权合成；辅助字段不得进入 Alpha；按行业和个股约束构造组合；并列按 `instrument_id` 稳定排序。

- [ ] **步骤 3：实现并验证**

运行：`uv run pytest tests/unit/strategies -v`

预期：全部通过，配置校验会拒绝未知因子、权重和不为 1、非正持仓数和缺失快照。

- [ ] **步骤 4：提交**

```bash
git add src/quant_core/strategies configs/experiments/examples tests/unit/strategies
git commit -m "feat: add ETF rotation and multifactor strategies"
```

### 任务 7：绩效分析、归因与摘要物化

**文件：**
- 新建：`src/quant_core/analytics/__init__.py`
- 新建：`src/quant_core/analytics/performance.py`
- 新建：`src/quant_core/analytics/attribution.py`
- 新建：`src/quant_core/analytics/materialize.py`
- 新建：`tests/unit/analytics/test_performance.py`
- 新建：`tests/integration/test_analysis_materialization.py`
- 新建：`tests/performance/test_full_market_backtest.py`
- 新建：`tests/performance/test_acceptance_workload.py`

- [ ] **步骤 1：写绩效口径失败测试**

验证累计/年化收益、年化波动、Sharpe、Sortino、Calmar、最大回撤及起止恢复日、月/年收益、换手、费用率、失败成交率、相对收益和信息比率。零波动、全负收益、未恢复回撤必须有明确结果。

- [ ] **步骤 2：写物化契约测试**

成功后必须产生 `metrics.json`、`nav.parquet`、`drawdown.parquet`、`monthly_returns.parquet`、`exposure_summary.parquet`、`factor_summary.parquet`、`attribution.parquet` 和 `quality_disclosure.json`；schema、指标版本和哈希写入 manifest。

- [ ] **步骤 3：实现分析和摘要**

指标模块只读取标准回测产物。归因按日期汇总行业、风格和证券贡献。Dashboard 摘要控制在可快速读取的规模，不复制完整持仓明细。

- [ ] **步骤 4：执行正确性与性能验收**

运行：`uv run pytest tests/unit/analytics tests/integration/test_analysis_materialization.py tests/regression/test_backtest_golden.py -v`

运行：`uv run pytest tests/performance/test_full_market_backtest.py -v --run-performance`

性能测试使用合成的 20 年日频全市场规模数据，记录总耗时、峰值内存和各阶段耗时；默认目标为单次标准多因子回测不超过 60 分钟，超标时测试失败并输出最慢阶段。

- [ ] **步骤 5：在真实 20 年快照上执行发布验收**

`tests/performance/test_acceptance_workload.py` 从环境变量 `QUANT_ACCEPTANCE_SNAPSHOT_ID` 读取已发布快照。测试先验证快照结束日是运行日之前的最新完整交易日、开始日覆盖向前滚动至少 20 年，并记录 CPU 型号、逻辑核数、内存、磁盘类型、Python/依赖版本、日期区间、证券数、因子数和策略参数到 `acceptance_environment.json`。

运行：`uv run pytest tests/performance/test_acceptance_workload.py -v --run-acceptance`

预期：标准股票多因子完整链路（股票池、因子、组合、成交、分析、产物）总耗时不超过 60 分钟；ETF 轮动完整链路不超过 5 分钟。任一超时均失败，并输出分阶段耗时与峰值内存。该测试是发布验收必跑项，不能用合成数据结果替代。

- [ ] **步骤 6：执行静态检查并提交**

运行：`uv run ruff check src tests && uv run mypy src`

```bash
git add src/quant_core/analytics tests/unit/analytics tests/integration/test_analysis_materialization.py tests/performance/test_full_market_backtest.py tests/performance/test_acceptance_workload.py
git commit -m "feat: materialize backtest analytics"
```

## 验收门槛

- 测试证明 T 日收盘信号最早只能在 T+1 成交。
- A 股停牌、涨跌停、T+1、整手、费用和容量规则都有独立边界测试。
- ETF 轮动和多因子策略通过同一 `TargetPortfolio` 接口运行。
- 标准产物在完整校验前不会以成功状态发布。
- 固定黄金数据的净值、成交、费用和指标稳定复现。
- 20 年全市场性能测试满足 60 分钟目标或明确报告阻塞阶段。
- 真实最新 20 年快照上的多因子链路不超过 60 分钟，ETF 轮动链路不超过 5 分钟，并保存验收机器环境记录。
- pytest、Ruff 和 mypy 全部通过。
