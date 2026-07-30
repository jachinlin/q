# 时点股票池与因子系统实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 建立绑定数据快照的时点研究查询、可审计的历史股票池，以及具备版本、依赖、缓存和诊断能力的完整 MVP 因子库。

**架构：** 研究代码只能读取不可变快照。`ResearchDataRepository` 强制执行数据可用时间过滤，`UniverseBuilder` 同时输出成员资格和剔除原因，因子注册表按依赖 DAG 计算并将结果写入内容寻址的 Feature Parquet。本计划不实现交易仿真、实验队列和界面。

**技术栈：** Python 3.12、Polars、DuckDB、PyArrow、Pydantic v2、NumPy、pytest、Hypothesis。

## 前置条件与约束

- 先完成 `2026-07-30-foundation-data-platform.md`。
- 所有查询必须传入 `SnapshotId`，禁止读取可变的“latest”目录。
- 财务数据仅在 `available_at <= as_of` 时可用；未知公告/可用日期不能用报告期末推断。
- 因子 ID 和语义版本共同标识实现。
- 缓存键必须包含因子版本、参数、代码哈希、依赖哈希、快照 ID、股票池哈希和日期范围。
- 相同快照、配置和代码必须生成排序一致、内容哈希一致的因子结果。

---

## 文件职责

| 路径 | 作用 |
|---|---|
| `src/quant_core/data/repository.py` | 绑定快照的行情、状态和财务查询 |
| `src/quant_core/data/adjustments.py` | 时点安全的价格复权服务 |
| `src/quant_core/universe/rules.py` | 历史股票池规则配置 |
| `src/quant_core/universe/builder.py` | 成员资格与剔除原因生成 |
| `src/quant_core/factors/base.py` | 因子协议、规格、上下文和输出 schema |
| `src/quant_core/factors/registry.py` | 版本注册和依赖 DAG |
| `src/quant_core/factors/cache.py` | 内容哈希和 Feature Parquet 持久化 |
| `src/quant_core/factors/transforms.py` | 去极值、中性化和标准化 |
| `src/quant_core/factors/builtin/*.py` | MVP 因子实现 |
| `src/quant_core/factors/analysis.py` | 覆盖率、相关性、RankIC 和分层诊断 |

### 任务 1：绑定快照的时点研究仓储

**文件：**
- 新建：`src/quant_core/data/repository.py`
- 新建：`tests/fixtures/point_in_time.py`
- 新建：`tests/point_in_time/test_research_repository.py`

**接口：**

```python
class ResearchDataRepository(Protocol):
    def bars(self, snapshot_id: SnapshotId, instruments: Sequence[InstrumentId], start: date, end: date) -> pl.LazyFrame: ...
    def financials_as_of(self, snapshot_id: SnapshotId, field_ids: Sequence[str], as_of: date, instruments: Sequence[InstrumentId] | None = None) -> pl.LazyFrame: ...
    def security_status(self, snapshot_id: SnapshotId, as_of: date, instruments: Sequence[InstrumentId] | None = None) -> pl.LazyFrame: ...
```

- [ ] **步骤 1：先写快照隔离和可用时间失败测试**

fixture 包含同一财务字段的两个修订版、一条未来才可用的记录和两个快照。断言早期快照看不到后来的数据版本；`as_of=2024-04-29` 看不到 `available_at=2024-04-30` 的记录；相同证券、字段、报告期只选择截止时点最新的可用版本。

- [ ] **步骤 2：运行测试并确认模块缺失**

运行：`uv run pytest tests/point_in_time/test_research_repository.py -v`

- [ ] **步骤 3：实现快照解析和参数化查询**

所有物理路径通过 `SnapshotRepository.get(snapshot_id)` 解析；快照缺少所需数据集时抛出 `SnapshotDatasetMissing`。SQL 的值使用位置参数，列名只能来自允许列表。返回规范 schema 且按主键排序的 `pl.LazyFrame`。

- [ ] **步骤 4：验证并提交**

运行：`uv run pytest tests/point_in_time/test_research_repository.py -v`

```bash
git add src/quant_core/data/repository.py tests/fixtures/point_in_time.py tests/point_in_time/test_research_repository.py
git commit -m "feat: add point-in-time research repository"
```

### 任务 2：时点安全的价格复权

**文件：**
- 新建：`src/quant_core/data/adjustments.py`
- 新建：`tests/point_in_time/test_adjustments.py`

**接口：**

```python
class AdjustmentMode(StrEnum):
    RAW = "RAW"
    BACKWARD = "BACKWARD"

class PriceAdjustmentService:
    def bars(self, snapshot_id: SnapshotId, instruments: Sequence[InstrumentId], start: date, end: date, mode: AdjustmentMode, as_of: date) -> pl.LazyFrame: ...
```

- [ ] **步骤 1：先写除权除息和未来事件测试**

构造五个交易日和第四日才发布的公司行动。断言第三日的 `as_of` 不得使用该事件；复权后的 OHLC、成交量严格遵循同一约定；历史原始价格保持不变。

- [ ] **步骤 2：确认测试失败**

运行：`uv run pytest tests/point_in_time/test_adjustments.py -v`

- [ ] **步骤 3：实现复权因子合成**

只读取 `available_at <= as_of` 的公司行动，按证券和除权日合成因子；结果附带 `adjustment_mode` 与 `adjustment_as_of`。禁止从未来完整序列倒推早期研究时点未知的因子。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/point_in_time/test_adjustments.py -v`

```bash
git add src/quant_core/data/adjustments.py tests/point_in_time/test_adjustments.py
git commit -m "feat: add point-in-time price adjustments"
```

### 任务 3：历史股票池规则与审计原因

**文件：**
- 新建：`src/quant_core/universe/__init__.py`
- 新建：`src/quant_core/universe/rules.py`
- 新建：`src/quant_core/universe/builder.py`
- 新建：`tests/point_in_time/test_universe_builder.py`

**接口：**

```python
@dataclass(frozen=True)
class UniverseRules:
    min_listing_days: int = 120
    allowed_boards: frozenset[Board] = frozenset({Board.MAIN, Board.CHINEXT, Board.STAR})
    exclude_st: bool = True
    exclude_suspended: bool = True
    min_avg_amount_20d: float | None = None

class UniverseBuilder:
    def build(self, snapshot_id: SnapshotId, as_of: date, rules: UniverseRules) -> pl.DataFrame: ...
```

- [ ] **步骤 1：写完整规则真值表**

覆盖尚未上市、已退市、新股、ST、停牌、板块不允许、流动性不足和正常证券。要求每个历史证券一行，至少包含 `eligible: bool` 与确定性排序的 `reason_codes: list[str]`。

- [ ] **步骤 2：运行失败测试**

运行：`uv run pytest tests/point_in_time/test_universe_builder.py -v`

- [ ] **步骤 3：实现规则组合**

上市天数使用交易日计数；证券状态取 `as_of` 当日有效记录；20 日成交额只使用 `as_of` 及以前数据。多条剔除原因按固定优先级排序，不得查询当前状态替代历史状态。

- [ ] **步骤 4：运行全部时点测试并提交**

运行：`uv run pytest tests/point_in_time -v`

```bash
git add src/quant_core/universe tests/point_in_time/test_universe_builder.py
git commit -m "feat: build auditable historical universes"
```

### 任务 4：因子契约、注册表、依赖 DAG 与缓存

**文件：**
- 新建：`src/quant_core/factors/__init__.py`
- 新建：`src/quant_core/factors/base.py`
- 新建：`src/quant_core/factors/registry.py`
- 新建：`src/quant_core/factors/cache.py`
- 新建：`tests/unit/factors/test_registry.py`
- 新建：`tests/unit/factors/test_cache.py`

**接口：**

```python
@dataclass(frozen=True)
class FactorSpec:
    factor_id: str
    version: str
    frequency: str
    lookback_sessions: int
    dependencies: tuple[str, ...]
    direction: int
    parameters: Mapping[str, JsonValue]

class Factor(Protocol):
    @property
    def spec(self) -> FactorSpec: ...
    def compute(self, ctx: FactorContext) -> pl.LazyFrame: ...

class FactorEngine:
    def compute(self, factor_ids: Sequence[str], ctx: FactorContext) -> Mapping[str, FactorArtifact]: ...
```

- [ ] **步骤 1：先写注册和缓存键测试**

测试重复 `(factor_id, version)`、缺失依赖、循环依赖及完整循环路径、稳定拓扑顺序。等价的规范参数应产生相同键；快照、股票池、版本、代码或依赖哈希任一变化都必须产生不同键。

- [ ] **步骤 2：运行并确认失败**

运行：`uv run pytest tests/unit/factors/test_registry.py tests/unit/factors/test_cache.py -v`

- [ ] **步骤 3：实现不可变契约与 DAG 校验**

输出列固定为 `trade_date`、`instrument_id`、`factor_id`、`factor_version`、`value`、`available_at`、`is_valid`。参数先按键排序生成规范 JSON 再计算 SHA-256。Feature Parquet 原子写入，完成 schema 和行数校验后才登记缓存。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/factors/test_registry.py tests/unit/factors/test_cache.py -v`

```bash
git add src/quant_core/factors tests/unit/factors
git commit -m "feat: add versioned factor DAG and cache"
```

### 任务 5：横截面因子处理

**文件：**
- 新建：`src/quant_core/factors/transforms.py`
- 新建：`tests/unit/factors/test_transforms.py`

**接口：**

```python
def winsorize_mad(frame: pl.DataFrame, value_col: str, group_cols: Sequence[str], n_mad: float = 3.0) -> pl.DataFrame: ...
def neutralize_wls(frame: pl.DataFrame, value_col: str, industry_col: str, size_col: str) -> pl.DataFrame: ...
def zscore(frame: pl.DataFrame, value_col: str, group_cols: Sequence[str]) -> pl.DataFrame: ...
```

- [ ] **步骤 1：先写数值测试**

覆盖常数组、空值、极端值、单成员行业和秩亏回归；正常样本与显式 NumPy 参考计算比较；有效横截面小于配置阈值时返回无效原因，不得返回伪造的 0。

- [ ] **步骤 2：运行测试并确认失败**

运行：`uv run pytest tests/unit/factors/test_transforms.py -v`

- [ ] **步骤 3：实现确定性处理**

使用 MAD 去极值，行业 one-hot 加对数市值做加权最小二乘中性化，再做横截面 z-score。通过临时行号保持输入顺序，并显式输出有效标记和原因。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/factors/test_transforms.py -v`

```bash
git add src/quant_core/factors/transforms.py tests/unit/factors/test_transforms.py
git commit -m "feat: add factor cross-sectional transforms"
```

### 任务 6：ETF 行情因子库

**文件：**
- 新建：`src/quant_core/factors/builtin/__init__.py`
- 新建：`src/quant_core/factors/builtin/momentum.py`
- 新建：`src/quant_core/factors/builtin/risk.py`
- 新建：`tests/unit/factors/test_etf_factors.py`

**因子：** `return_20d_v1`、`return_60d_v1`、`return_120d_v1`、`trend_120d_v1`、`volatility_60d_v1`。

- [ ] **步骤 1：为五个公式写失败测试**

手工构造复权收盘序列。收益率定义为 `close[t] / close[t-n] - 1`；趋势为 120 日对数价格 OLS 斜率并按均值归一；波动率为日对数收益样本标准差年化。历史不足应标记无效，不得补 0。

- [ ] **步骤 2：运行失败测试**

运行：`uv run pytest tests/unit/factors/test_etf_factors.py -v`

- [ ] **步骤 3：实现并注册五个因子**

优先使用 Polars lazy 表达式，结果稳定按 `(trade_date, instrument_id)` 排序；元数据声明使用后复权价格和所需回看窗口。

- [ ] **步骤 4：测试并提交**

运行：`uv run pytest tests/unit/factors/test_etf_factors.py -v`

```bash
git add src/quant_core/factors/builtin tests/unit/factors/test_etf_factors.py
git commit -m "feat: add ETF market factors"
```

### 任务 7：股票 Alpha、辅助字段与因子诊断

**文件：**
- 新建：`src/quant_core/factors/builtin/valuation.py`
- 新建：`src/quant_core/factors/builtin/quality.py`
- 扩展：`src/quant_core/factors/builtin/momentum.py`
- 扩展：`src/quant_core/factors/builtin/risk.py`
- 新建：`src/quant_core/factors/builtin/auxiliary.py`
- 新建：`src/quant_core/factors/analysis.py`
- 新建：`tests/unit/factors/test_stock_factors.py`
- 新建：`tests/unit/factors/test_factor_analysis.py`
- 新建：`tests/integration/test_factor_materialization.py`

**Alpha 因子：** `earnings_yield_ttm_v1`、`book_to_price_mrq_v1`、`roe_avg_pit_v1`、`cfo_to_np_pit_v1`、`momentum_120_20_v1`、`volatility_60d_v1`、`downside_volatility_60d_v1`、`max_drawdown_120d_v1`。

**辅助字段：** `avg_amount_20d_v1`、`log_market_cap_v1`、`industry_code_pit_v1`。

- [ ] **步骤 1：为每个公式和时点边界写失败测试**

测试安全除法、正市值约束、未来财务修订不可见、120 日动量跳过最近 20 日、下行波动仅使用负收益、120 日最大回撤采用滚动峰谷。辅助字段必须明确禁止进入 Alpha 合成。

- [ ] **步骤 2：为诊断指标写失败测试**

在已知数组上验证覆盖率、与下一期收益的 Spearman RankIC、分位数组、分层收益、多空收益和相关矩阵。信号与收益必须按信号日对齐，收益窗口严格位于未来。

- [ ] **步骤 3：实现股票因子与诊断**

财务因子只使用仓储返回的 PIT 数据；价值、质量、动量方向为 `+1`，风险方向为 `-1`；产物元数据记录源字段 ID 和可用时间。先逐日计算横截面诊断，再进行时间聚合。

- [ ] **步骤 4：连续物化两次验证缓存**

运行：`uv run pytest tests/unit/factors/test_stock_factors.py tests/unit/factors/test_factor_analysis.py tests/integration/test_factor_materialization.py -v`

预期：全部通过；第二次相同计算命中缓存且不重写 Feature Parquet。

- [ ] **步骤 5：执行总体验收并提交**

运行：`uv run pytest tests/point_in_time tests/unit/factors tests/integration/test_factor_materialization.py -v`

运行：`uv run ruff check src tests && uv run mypy src`

```bash
git add src/quant_core/factors tests/unit/factors tests/integration/test_factor_materialization.py
git commit -m "feat: complete MVP stock factors and diagnostics"
```

## 验收门槛

- 每个因子产物都绑定不可变快照和历史股票池哈希。
- 时点测试证明未来公告和财务修订不会影响早期日期。
- 5 个 ETF 因子、8 个股票 Alpha 因子和 3 个辅助字段全部注册并可缓存。
- 股票池对每个剔除结果给出确定性的原因码。
- 相同输入重复计算命中缓存，并得到相同产物哈希。
- pytest、Ruff 和 mypy 全部通过。
