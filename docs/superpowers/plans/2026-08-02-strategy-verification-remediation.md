# 策略验证缺口修复实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标：** 以三个独立测试—复审门禁关闭原回测计划 Task 6 的 B–G 六组承重验证缺口，确认策略状态、PIT、ETF 信号、多因子数值、运行时约束和 YAML 装配均符合既定契约。

**架构：** 不新增策略公共接口，沿用 `StrategyContext`、`PortfolioState`、`validated_factor_values()`、`validated_stock_universe()`、`EtfRotationConfig.from_mapping()`、`MultifactorConfig.from_mapping()` 和 `PortfolioConstructor.construct()`。修复按失效域分为数据边界、数值语义、运行时约束与配置装配三个顺序任务；测试先精确刻画契约，只有测试证明生产缺陷时才做最小实现修改。

**技术栈：** Python 3.12、Polars、NumPy、PyYAML、pytest、Ruff、mypy、uv。

## 全局约束

- 设计规格为 `docs/superpowers/specs/2026-08-02-strategy-verification-remediation-design.md`。
- 所有输入绑定 `SnapshotId`，T 日收盘后生成信号，最早在 T+1 成交；`signal_date` 与 `execute_date` 不得合并。
- 数据 schema、dtype、时点、证券身份或证据不合法时必须 fail closed。
- 每个非法构造用例从合法基线只改变一个目标字段；不得用更早失败的其他不变量充当覆盖证据。
- 策略级数据排除必须断言 target、现金结果或审计原因；只断言底层 validator 抛错不算集成覆盖。
- 多因子期望值必须硬编码，禁止在测试中复制生产算法计算 expected。
- 六项组合约束必须在 `MultifactorStrategy.generate_targets()` 到 `PortfolioConstructor.construct()` 的真实路径上成为 binding。
- 配置负例必须调用 `from_mapping()`；直接构造 dataclass 不能替代 YAML/parser 路径。
- 不处理 ETF composite 溢出和等权尾差两个 deferred Minor，不实现 Task 7、实验室或 Dashboard。
- 不清理既有 `.p*`、`.pytest-*` 和 `.tmp` 未跟踪目录。
- 验证缺口本身可能对应“生产代码已正确但缺测试”；新增精确测试若首次运行即通过，不得制造人工生产缺陷来追求 RED。若失败，则先按 `superpowers:systematic-debugging` 定位根因，再做最小修复。

---

## 文件职责与改动边界

| 文件 | 本计划中的作用 |
|---|---|
| `tests/unit/strategies/test_review_round3_matrix.py` | 补齐 `PortfolioState`、因子矩阵和股票池契约的独立负例 |
| `tests/unit/strategies/test_etf_rotation.py` | 补齐 ETF YAML、缺行、invalid、非有限值和未来时点集成测试 |
| `tests/unit/strategies/test_multifactor.py` | 补齐多因子 invalid 排除、数值 oracle、审计、tie、运行时约束和配置矩阵 |
| `src/quant_core/strategies/base.py` | 仅在边界测试证明现有 validator 有误时最小修复 |
| `src/quant_core/strategies/etf_rotation.py` | 仅在 ETF 集成测试证明 fail-closed 或 parser 有误时最小修复 |
| `src/quant_core/strategies/multifactor.py` | 仅在数值、候选传递或 parser 测试证明实现有误时最小修复 |
| `src/quant_core/portfolio/constructor.py` | 仅在真实运行时约束测试证明 constructor 有误时最小修复 |

### Task 1：数据、状态与 ETF 信号边界

**文件：**
- Modify: `tests/unit/strategies/test_review_round3_matrix.py`
- Modify: `tests/unit/strategies/test_multifactor.py`
- Modify: `tests/unit/strategies/test_etf_rotation.py`
- Conditional Modify: `src/quant_core/strategies/base.py`
- Conditional Modify: `src/quant_core/strategies/etf_rotation.py`
- Conditional Modify: `src/quant_core/strategies/multifactor.py`

**接口：**
- Consumes: `PortfolioState(...)`、`validated_factor_values(frame, signal_date=..., instruments=..., factor_refs=...)`、`validated_stock_universe(frame, signal_date=...)`、`EtfRotationStrategy.generate_targets(...)`、`MultifactorStrategy.generate_targets(...)`。
- Produces: 原 Task 6 的 B、C、D 三组残项全部具有独立、可定位的回归证据。

- [ ] **Step 1：补齐 `PortfolioState` 独立不变量矩阵**

在 `test_review_round3_matrix.py` 增加两个合法仓位和一个合法 state 工厂，避免不同不变量相互遮蔽：

```python
_ID_2 = InstrumentId.parse("SSE:600002")


def _position(
    instrument: InstrumentId = _ID,
    *,
    market_value_fen: int = 40,
    current_weight: float = 0.4,
) -> PortfolioPosition:
    return PortfolioPosition(instrument, 100, market_value_fen, current_weight)


def _valid_state(**change: object) -> PortfolioState:
    values: dict[str, object] = {
        "trade_date": _DAY,
        "cash_fen": 20,
        "nav_fen": 100,
        "total_market_value_fen": 80,
        "positions": (
            _position(),
            _position(_ID_2, market_value_fen=40, current_weight=0.4),
        ),
        "cash_weight": 0.2,
    }
    values.update(change)
    return PortfolioState(**values)  # type: ignore[arg-type]
```

新增独立测试；每个 case 保持 `nav=cash+market`、market 等于 positions 求和等前置不变量合法，直到目标分支：

```python
def test_state_rejects_noninteger_total_market_value_and_negative_nav() -> None:
    with pytest.raises(ValueError, match="total_market_value_fen"):
        _valid_state(total_market_value_fen=80.0)
    with pytest.raises(ValueError, match="nav_fen"):
        _valid_state(nav_fen=-100)


def test_state_rejects_duplicate_and_unsorted_positions() -> None:
    first = _position()
    duplicate = _position()
    with pytest.raises(ValueError, match="unique"):
        _valid_state(positions=(first, duplicate))
    with pytest.raises(ValueError, match="sorted"):
        _valid_state(positions=(_position(_ID_2), first))


def test_state_rejects_each_weight_and_market_sum_invariant() -> None:
    with pytest.raises(ValueError, match="must equal positions"):
        _valid_state(
            positions=(
                _position(market_value_fen=30, current_weight=0.3),
                _position(_ID_2, market_value_fen=30, current_weight=0.3),
            )
        )
    with pytest.raises(ValueError, match="weights must sum"):
        _valid_state(
            positions=(
                _position(current_weight=0.3),
                _position(_ID_2, current_weight=0.3),
            )
        )
    with pytest.raises(ValueError, match="position weight"):
        _valid_state(
            cash_weight=0.2,
            positions=(
                _position(current_weight=0.3),
                _position(_ID_2, current_weight=0.5),
            ),
        )
```

注意：若异常正则显示命中更早分支，调整该 case 的其他字段，使其只触发目标不变量，不得放宽成无 message 的通用 `ValueError`。

- [ ] **Step 2：运行状态矩阵并核验目标分支**

Run:

```powershell
uv run pytest tests/unit/strategies/test_review_round3_matrix.py -q
```

Expected: 所有既有和新增用例 PASS；若 message 暴露前置不变量遮蔽，先修测试基线。只有现有合法状态被错误拒绝或非法状态被接受时才修改 `base.py`。

- [ ] **Step 3：补齐因子矩阵和股票池 schema/identity 矩阵**

在 `test_review_round3_matrix.py` 增加精确 schema 变异：

```python
@pytest.mark.parametrize(
    "frame",
    [
        _factor().drop("value"),
        _factor().with_columns(pl.lit("x").alias("extra")),
        _factor().with_columns(pl.col("value").cast(pl.Float32)),
        _factor().with_columns(pl.col("trade_date").cast(pl.Datetime)),
    ],
)
def test_factor_matrix_rejects_exact_schema_or_dtype_mutation(
    frame: pl.DataFrame,
) -> None:
    with pytest.raises(ValueError, match="schema"):
        validated_factor_values(
            frame, signal_date=_DAY, instruments=(_ID,), factor_refs=("x@1",)
        )


def test_factor_matrix_rejects_null_availability_and_duplicate_request_refs() -> None:
    with pytest.raises(ValueError, match="not available"):
        validated_factor_values(
            _factor(available_at=None),
            signal_date=_DAY,
            instruments=(_ID,),
            factor_refs=("x@1",),
        )
    with pytest.raises(ValueError, match="unique"):
        validated_factor_values(
            _factor(),
            signal_date=_DAY,
            instruments=(_ID,),
            factor_refs=("x@1", "x@1"),
        )
```

股票池 identity/evidence cases 使用两行合法基线，再分别交换顺序、复制 ID、写入 `"BAD"`、`[None]`、null ADV 和空 industry。为 `reason_codes=[None]` 显式构造 `pl.List(pl.String)`，断言 `reason_codes`；其余分别断言 `sorted/unique`、`canonical` 或 eligible evidence 错误。

```python
@pytest.mark.parametrize(
    "frame, message",
    [
        (_universe(reason_codes=[None]), "reason_codes"),
        (_universe(adv_amount=None), "adv_amount"),
        (_universe(industry=""), "industry"),
    ],
)
def test_universe_rejects_null_or_empty_eligible_evidence(
    frame: pl.DataFrame, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        validated_stock_universe(frame, signal_date=_DAY)
```

- [ ] **Step 4：补齐策略级 invalid/future 排除和 ETF 缺行矩阵**

在 `test_multifactor.py` 扩展现有审计测试，同时断言 invalid 证券不进入 target：

```python
selected = {position.instrument_id.canonical() for position in target.positions}
assert _IDS[0] not in selected
assert decision.factor_reasons[_ALPHA_REFS[0]] == "SOURCE_INVALID"
```

在 `test_etf_rotation.py` 对 `_factor_frame()` 结果做三类独立变异：真正删除 `_ETF_A` 的一个必需 factor row；将该行设成 `is_valid=False, value=0.2, invalid_reason="SOURCE_INVALID"`；将 valid 行分别设成 NaN、inf、T+1 `available_at`。每个 case 都保留 `_ETF_B` 为完全合法且可入选的对照：

```python
@pytest.mark.parametrize("mode", ["missing", "invalid", "nan", "inf"])
def test_etf_excludes_one_instrument_for_each_unusable_signal(mode: str) -> None:
    frame = _factor_frame(_fully_valid_etf_values())
    mask = (
        (pl.col("instrument_id") == _ETF_A)
        & (pl.col("factor_ref") == _RETURN_REFS[0])
    )
    if mode == "missing":
        frame = frame.filter(~mask)
    elif mode == "invalid":
        frame = frame.with_columns(
            pl.when(mask).then(False).otherwise(pl.col("is_valid")).alias("is_valid"),
            pl.when(mask).then(0.2).otherwise(pl.col("value")).alias("value"),
            pl.when(mask)
            .then(pl.lit("SOURCE_INVALID"))
            .otherwise(pl.col("invalid_reason"))
            .alias("invalid_reason"),
        )
    else:
        bad = float(mode)
        frame = frame.with_columns(
            pl.when(mask).then(bad).otherwise(pl.col("value")).alias("value")
        )
    target = EtfRotationStrategy(_config(top_n=1)).generate_targets(
        _context(_Data(frame)), _SIGNAL, _empty_state()
    )
    assert [p.instrument_id.canonical() for p in target.positions] == [_ETF_B]
```

未来 `available_at` 对公共 PIT 契约必须 fail closed：分别在 ETF 和多因子数据端将一行改成 `_EXECUTE` 的 UTC datetime，断言 `ValueError` message 含 `available_at`，证明未来数据未进入策略目标。

- [ ] **Step 5：补齐 ETF `from_mapping()` 非 canonical identifier**

复用示例 YAML mapping，只变异 `etf_pool` 的一个字符串：

```python
@pytest.mark.parametrize("identifier", ["BAD", "SSE:51001", "UNKNOWN:510001"])
def test_etf_mapping_rejects_noncanonical_pool_identifier(identifier: str) -> None:
    mapping = yaml.safe_load(
        Path("configs/experiments/examples/etf_rotation.yaml").read_text(
            encoding="utf-8"
        )
    )
    mapping["etf_pool"] = [identifier, *mapping["etf_pool"][1:]]
    with pytest.raises((TypeError, ValueError)):
        EtfRotationConfig.from_mapping(mapping)
```

- [ ] **Step 6：运行 Task 1 全部验证**

Run:

```powershell
uv run pytest tests/unit/strategies/test_review_round3_matrix.py tests/unit/strategies/test_etf_rotation.py tests/unit/strategies/test_multifactor.py -q
uv run pytest tests/unit/portfolio tests/unit/backtest tests/regression/test_backtest_golden.py -q
uv run ruff format --check tests/unit/strategies src/quant_core/strategies
uv run ruff check tests/unit/strategies src/quant_core/strategies
uv run mypy src
git diff --check
```

Expected: 全部 PASS，且无 `xfail`/`skip`。若目录不存在，先用 `rg --files tests` 确认等价的既有 portfolio/backtest 测试路径，只运行已存在路径并在报告中记录实际命令。

- [ ] **Step 7：提交 Task 1**

```powershell
git add -- tests/unit/strategies/test_review_round3_matrix.py tests/unit/strategies/test_etf_rotation.py tests/unit/strategies/test_multifactor.py src/quant_core/strategies/base.py src/quant_core/strategies/etf_rotation.py src/quant_core/strategies/multifactor.py
git commit -m "test: close strategy data boundary gaps"
```

只 add 实际修改文件；未修改的生产文件不得为制造空暂存而改动。

### Task 2：多因子硬编码数值 oracle

**文件：**
- Modify: `tests/unit/strategies/test_multifactor.py`
- Conditional Modify: `src/quant_core/strategies/multifactor.py`

**接口：**
- Consumes: `MultifactorStrategy.generate_targets()` 和 `audit_sink` 暴露的不可变 `MultifactorDecision`。
- Produces: 原 Task 6 的 E 组具备完整的 MAD→neutralize→zscore→direction→category→final score 字面 oracle、辅助字段隔离、transform 审计和 canonical tie 证据。

- [ ] **Step 1：建立六证券固定数值横截面**

在 `test_multifactor.py` 新增 `_literal_oracle_frames()`。六证券行业为 `AAA,AAA,AAA,BBB,BBB,BBB`，`log_market_cap` 为 `10,11,12,10,11,12`。八因子按类别使用以下 raw 数组；三个风险因子使用 `RISK_RAW`，其余同类因子复用对应数组：

```python
VALUE_RAW = [131.0, 131.0, 137.0, 138.0, 138.0, 1000.0]
QUALITY_RAW = [137.0, 131.0, 131.0, 1000.0, 138.0, 138.0]
MOMENTUM_RAW = [131.0, 137.0, 131.0, 138.0, 1000.0, 138.0]
RISK_RAW = [-137.0, -131.0, -131.0, -138.0, -1000.0, -138.0]
```

所有行使用 `_SIGNAL`、同一可见 UTC 时间、`is_valid=True`。股票池六行全部 eligible，ADV 为 `1000.0`。约束使用 `PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 1.0)`，确保数值 oracle 不被非目标约束截断。

同时增加空仓状态工厂，避免后续测试重复构造：

```python
def _empty_multifactor_state() -> PortfolioState:
    return PortfolioState(_SIGNAL, 1_000_000, 1_000_000, 0, (), 1.0)
```

- [ ] **Step 2：写完整 final score 字面 oracle**

通过 `audit_sink=decisions.extend` 取得六个 decision，按 canonical ID 建表。硬编码预期如下：

```python
EXPECTED_FINAL = {
    "SSE:600001": 0.25185993115227834,
    "SSE:600002": -0.22482830505693546,
    "SSE:600003": 0.033656856027000484,
    "SSE:600004": -0.1222622327762512,
    "SSE:600005": 0.09491018128225193,
    "SSE:600006": -0.03333643062834397,
}


def test_multifactor_literal_score_oracle_covers_full_transform_chain() -> None:
    factors, universe = _literal_oracle_frames()
    decisions: list[MultifactorDecision] = []
    strategy = MultifactorStrategy(
        _config(
            constraints=PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 1.0)
        ),
        audit_sink=decisions.extend,
    )
    strategy.generate_targets(
        _context(_Data(factors, universe)), _SIGNAL, _empty_multifactor_state()
    )
    actual = {
        item.instrument_id.canonical(): item.score
        for item in decisions
        if item.reason_code == "MULTIFACTOR_SELECTED"
    }
    assert actual == pytest.approx(EXPECTED_FINAL, abs=1e-12)
```

该 oracle 的 VALUE outlier `1000.0` 必须经 MAD 截断后影响最终值；expected 不得在测试中调用 transforms 重新生成。

- [ ] **Step 3：写风险方向和类别合成的独立字面断言**

用 `monkeypatch` 包装 `quant_core.strategies.multifactor.winsorize_mad`、`neutralize_wls` 和 `zscore`，记录调用顺序和每次 z-score 的结果后调用原函数；断言调用序列严格等于 `["winsorize_mad", "neutralize_wls", "zscore"] * 8`。因子定义顺序固定，`zscore` 记录的第 6–8 组必须逐项等于风险 raw z-score `[-0.42565458364508985, 0.4484675152379841, -0.248118407336624, 1.6443033152990194, -1.6701293097050858, 0.2511314701497959]`，再由 direction `-1` 转成正向风险分量。另外对一个固定证券使用下列字面分量，证明四类权重是 `0.25/0.25/0.30/0.20`：

```python
EXPECTED_COMPONENTS_600001 = {
    "VALUE": 1.6212439465260766,
    "QUALITY": 0.3472186562479454,
    "MOMENTUM": -1.084622120900817,
    "RISK_RAW_Z": -0.42565458364508985,
    "RISK_DIRECTED": 0.42565458364508985,
}


def test_risk_direction_and_category_weights_have_literal_oracle() -> None:
    expected = (
        0.25 * EXPECTED_COMPONENTS_600001["VALUE"]
        + 0.25 * EXPECTED_COMPONENTS_600001["QUALITY"]
        + 0.30 * EXPECTED_COMPONENTS_600001["MOMENTUM"]
        + 0.20 * EXPECTED_COMPONENTS_600001["RISK_DIRECTED"]
    )
    assert expected == pytest.approx(0.25185993115227834, abs=1e-12)
```

测试不得只验证上面的 Python 算式；同一测试必须运行策略并断言 `SSE:600001` 的 decision score 等于 `expected`，同时断言三个风险定义的 direction 均为 `-1`。

- [ ] **Step 4：验证辅助字段隔离、transform invalid reason 和 canonical tie**

辅助字段隔离使用两份输入：只改变 ADV（仍高于门槛）不改变任何 score；只对整个横截面的 `log_market_cap` 加同一常数不改变 score。行业变异使用行业标签整体一一重命名 `AAA→X, BBB→Y`，保持分组结构不变，score 必须相同。分别断言六个 decision score 与基线逐项相等。

transform invalid reason：把一个完整 factor ref 的六个 raw value 全部设成相同常数，使 transform 产生 `ZERO_VARIANCE`；其他七因子仍满足覆盖和四类别要求。断言每个 decision 的 `factor_reasons` 对该 ref 保存字面原因 `ZERO_VARIANCE`，decision/factor_reasons 仍不可变。

tie：构造两个证券在所有 Alpha、industry、size 上完全相同，并保留至少六行以满足横截面与回归秩；设置 `max_positions=1`，断言最终只选择 canonical ID 较小者。

```python
assert tuple(p.instrument_id.canonical() for p in target.positions) == (
    "SSE:600001",
)
with pytest.raises(TypeError):
    decisions[0].factor_reasons["x"] = "mutated"  # type: ignore[index]
```

- [ ] **Step 5：运行 Task 2 数值和回归验证**

Run:

```powershell
uv run pytest tests/unit/strategies/test_multifactor.py -q
uv run pytest tests/unit/factors tests/unit/portfolio tests/unit/strategies -q
uv run ruff format --check tests/unit/strategies/test_multifactor.py src/quant_core/strategies/multifactor.py
uv run ruff check tests/unit/strategies/test_multifactor.py src/quant_core/strategies/multifactor.py
uv run mypy src
git diff --check
```

Expected: 全部 PASS；数值断言使用 `abs=1e-12`，不降低精度以掩盖算法变化。

- [ ] **Step 6：提交 Task 2**

```powershell
git add -- tests/unit/strategies/test_multifactor.py src/quant_core/strategies/multifactor.py
git commit -m "test: lock multifactor numerical semantics"
```

只 add 实际修改文件。

### Task 3：多因子运行时约束与配置装配

**文件：**
- Modify: `tests/unit/strategies/test_multifactor.py`
- Modify: `tests/unit/strategies/test_etf_rotation.py`
- Conditional Modify: `src/quant_core/strategies/multifactor.py`
- Conditional Modify: `src/quant_core/strategies/etf_rotation.py`
- Conditional Modify: `src/quant_core/portfolio/constructor.py`

**接口：**
- Consumes: `MultifactorStrategy.generate_targets()`、`PortfolioConstructor.construct()`、`MultifactorConfig.from_mapping()`、`EtfRotationConfig.from_mapping()`。
- Produces: 原 Task 6 的 F、G 组全部关闭；每项运行时约束和每类非法 mapping 都有真实装配路径证据。

- [ ] **Step 1：增加可记录的真实 constructor 子类和含仓位 state**

在 `test_multifactor.py` 增加：

```python
class _RecordingConstructor(PortfolioConstructor):
    def __init__(self) -> None:
        self.calls: list[tuple[pl.DataFrame, PortfolioConstraints, date, date]] = []

    def construct(
        self,
        candidates: pl.DataFrame,
        constraints: PortfolioConstraints,
        signal_date: date,
        execute_date: date,
    ) -> TargetPortfolio:
        self.calls.append((candidates.clone(), constraints, signal_date, execute_date))
        return super().construct(candidates, constraints, signal_date, execute_date)
```

用两个 canonical 排序的 `PortfolioPosition` 构造 `PortfolioState`：eligible 的 `_IDS[0]` 权重 `0.2`，已不 eligible 的 `_IDS[7]` 权重 `0.1`，现金权重 `0.7`。将 recording constructor 注入 `StrategyContext`，运行策略后断言 candidates 中 `_IDS[0].current_weight == 0.2`；`_IDS[7]` 仍存在，且 `score is None, industry is None, adv_amount == 0.0, current_weight == 0.1`。同时断言 constructor 收到 `(_SIGNAL, _EXECUTE)`，target 日期也相同。

- [ ] **Step 2：让六项约束分别成为 binding**

每个用例复用 `_frames()`，但只修改目标约束所需输入：

```python
def _run_with_constraints(
    constraints: PortfolioConstraints,
    *,
    factors: pl.DataFrame | None = None,
    universe: pl.DataFrame | None = None,
    current: PortfolioState | None = None,
) -> TargetPortfolio:
    default_factors, default_universe = _frames()
    selected_factors = default_factors if factors is None else factors
    selected_universe = default_universe if universe is None else universe
    selected_current = _empty_multifactor_state() if current is None else current
    return MultifactorStrategy(_config(constraints=constraints)).generate_targets(
        _context(_Data(selected_factors, selected_universe)),
        _SIGNAL,
        selected_current,
    )
```

禁止对 Polars DataFrame 使用布尔真值；实际实现应显式判断 `is None`。

分别新增：

- `max_position_weight=0.20`：断言所有 position `<=0.20` 且产生现金余量。
- `max_industry_weight=0.25`：按 target 与 universe 行业映射求和，断言每行业 `<=0.25`，并证明至少一个行业在无 cap 基线中会超过 `0.25`。
- `min_positions=4`：只保留三个证券的完整八因子覆盖，断言 `ConstraintViolation.constraint_name == "min_positions"`。
- `max_positions=2`：使用 Task 2 的 `_literal_oracle_frames()`，断言恰好选择硬编码最高分的 `SSE:600001` 和 `SSE:600005`。
- `min_adv_amount=1500.0`：只把一个原本会入选的高分证券 ADV 设成 `1000.0`，其他设成 `2000.0`，断言该证券被过滤。
- `max_turnover=0.10`：从全现金 state 生成投资目标，断言 `ConstraintViolation.constraint_name == "max_turnover"`。

非 eligible current holding 参与换手：使用持有 `_IDS[7]` 权重 `0.4` 的 state 和 `max_turnover=0.30`，断言抛出 `max_turnover`；对照删除该 current holding 后相同目标不应因该持仓产生同一 turnover 值。

- [ ] **Step 3：建立合法 mapping 工厂**

从示例 YAML 加载合法 mapping，并使用 `copy.deepcopy()` 保证每个参数化 case 只改一处：

```python
def _multifactor_mapping() -> dict[str, object]:
    return yaml.safe_load(
        Path("configs/experiments/examples/multifactor.yaml").read_text(
            encoding="utf-8"
        )
    )


def _etf_mapping() -> dict[str, object]:
    return yaml.safe_load(
        Path("configs/experiments/examples/etf_rotation.yaml").read_text(
            encoding="utf-8"
        )
    )
```

测试文件新增 `from copy import deepcopy`。每个 mutation 函数接收 mapping 并只修改一个字段。

- [ ] **Step 4：补齐多因子 `from_mapping()` 非法矩阵**

参数化覆盖并断言 `TypeError` 或 `ValueError`：

1. factor definitions 添加 `unknown_alpha_v1@1.0.0`。
2. 用 `avg_amount_20d_v1@1.0.0`、`log_market_cap_v1@1.0.0`、`industry_code_v1@1.0.0` 分别替换一个 Alpha ref。
3. `volatility_60d_v1@1.0.0` category 保持 `RISK` 但 direction 改为 `1`。
4. 四类别 key 完整，权重分别变异成总和 `0.9`、负数、NaN、inf。
5. `min_positions` 和 `max_positions` 分别设为 `0`；另设 `max_positions < min_positions`。
6. `mad_multiplier` 分别设为 `0.0`、`-1.0`、NaN、inf。
7. constraints 添加未知 key；删除每个必需 key；六个字段分别使用非法类型或非法范围。
8. factor ref 使用非字符串 key；category 使用整数。

```python
@pytest.mark.parametrize("mutate", MULTIFACTOR_MAPPING_MUTATIONS)
def test_multifactor_mapping_rejects_each_invalid_assembly(mutate: object) -> None:
    mapping = deepcopy(_multifactor_mapping())
    mutate(mapping)  # type: ignore[operator]
    with pytest.raises((TypeError, ValueError)):
        MultifactorConfig.from_mapping(mapping)
```

mutation 列表中的每个 callable 使用具名函数或带默认参数绑定的 lambda，禁止 late-binding 循环变量。

- [ ] **Step 5：验证 ETF parser 与两份合法 YAML 正路径**

ETF 非 canonical identifier 用 `from_mapping()` 覆盖 `BAD`、错误位数和未知交易所；不得复用 direct dataclass constructor 测试。最后加载两份未修改示例 YAML，断言 `EtfRotationConfig.from_mapping()` 和 `MultifactorConfig.from_mapping()` 都成功，并核验关键字段：ETF frequency=`MONTHLY`、多因子 `min_valid_factors=6`、四类权重和为 `1.0`。

- [ ] **Step 6：运行 Task 3 和完整 Task 6 验证**

Run:

```powershell
uv run pytest tests/unit/strategies -q
uv run pytest tests/unit/portfolio tests/unit/backtest tests/integration tests/regression/test_backtest_golden.py -q
uv run ruff format --check tests/unit/strategies src/quant_core/strategies src/quant_core/portfolio
uv run ruff check tests/unit/strategies src/quant_core/strategies src/quant_core/portfolio
uv run mypy src
git diff --check
```

Expected: 全部 PASS，无 `xfail`/`skip`；若 integration 目录包含依赖外部快照的发布验收，使用项目已有 marker 排除仅 `QUANT_ACCEPTANCE_SNAPSHOT_ID` 阻塞的真实 20 年验收，并在报告中逐条列出排除项，不能用合成结果冒充发布验收。

- [ ] **Step 7：提交 Task 3**

```powershell
git add -- tests/unit/strategies/test_multifactor.py tests/unit/strategies/test_etf_rotation.py src/quant_core/strategies/multifactor.py src/quant_core/strategies/etf_rotation.py src/quant_core/portfolio/constructor.py
git commit -m "test: verify multifactor runtime constraints and config"
```

只 add 实际修改文件。

## 计划完成门禁

三个任务分别通过独立任务复审后，生成从本计划起始基线到 HEAD 的汇总审查包。汇总审查者必须按原 Task 6 的 B–G 残项逐条给出 CLOSED/OPEN 和行号证据；只有 B–G 全部 CLOSED、无新 Critical/Important、完整策略与关联回归通过时，才在原 ledger 中解除 Task 6 BLOCKED 并恢复原计划 Task 7。任何任务在五轮修复后仍有承重 Important，继续按 SDD breaker 规则停止，不得跳过。
