# BaoStock 涨跌幅前复权 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增基于 BaoStock 原始 `close/preclose` 的时点安全前复权模式，并将全部内置行情因子迁移到该模式。

**Architecture:** Canonical 继续保存 `adjustflag=3` 的不复权日线；`PriceAdjustmentService` 在每个证券内部用相邻 session 的 `preclose/previous close` 在对数域向前累计前复权因子。因子批量读取以 `ctx.end` 锚定的前复权序列，再用信号日累计因子换基，得到逐信号日锚定且无前视的价格窗口。

**Tech Stack:** Python 3.12、Polars、PyArrow、DuckDB、pytest、Ruff、mypy、uv

## Global Constraints

- 新增 `AdjustmentMode.FORWARD`；`RAW` 和现有 `BACKWARD` 接口保留。
- `FORWARD` 只调整 `open/high/low/close/preclose`；`volume/amount/turnover/pct_change/估值字段` 保持原值。
- 因子计算使用 raw `close/preclose`；复权结果不得写回 Canonical。
- 前复权最后一个有效锚点 session 的 `adjustment_factor` 为 `1.0`。
- 每个信号日独立锚定；延长 `ctx.end` 或追加未来行情不得改变已有信号。
- BaoStock 历史日线 `available_at` 使用上海市场收盘时点；`ingested_at` 保留实际抓取时间。
- 重复键、非法必要价格、非有限累计因子全部 fail closed。
- 内置市场因子统一声明 `adjustment_mode="FORWARD"`，旧缓存必须通过新参数与 source-sensitive code hash 自动失效。
- 不新增生产公司行动或财务采集；财务质量因子能力门禁属于后续整体验收修复。
- 测试不得访问在线 BaoStock；供应商对照使用固定样本。

---

## 文件职责

| 文件 | 作用 |
|---|---|
| `src/quant_core/data/sources/baostock.py` | 阻止当前交易日收盘前的不完整日线进入 Raw 发布链路 |
| `src/quant_core/data/mappers/baostock.py` | 将日线业务可用时间映射为交易日收盘时点，抓取时间只写入 `ingested_at` |
| `src/quant_core/data/adjustments.py` | 定义 `FORWARD`，计算前复权累计因子并输出调整后的价格 |
| `src/quant_core/factors/builtin/momentum.py` | 将市场窗口换基到每个 signal date，并统一请求 `FORWARD` |
| `src/quant_core/factors/builtin/risk.py` | 复用迁移后的市场窗口，更新风险因子元数据 |
| `src/quant_core/factors/builtin/code_hash.py` | 对内置因子源码闭包生成稳定 code hash |
| `src/quant_core/factors/builtin/__init__.py` | 提供顺序无关、同身份同实现幂等的 canonical 注册入口 |
| `tests/unit/data/mappers/test_baostock_mapper.py` | 验证历史日线/状态的双时间语义和未收盘数据 fail closed |
| `tests/unit/data/sources/test_baostock.py` | 验证收盘前日线请求在 Source 边界被拒绝 |
| `tests/point_in_time/test_adjustments.py` | 验证前复权公式、字段边界、异常与 schema |
| `tests/unit/factors/test_etf_factors.py` | 验证 signal-local 换基、元数据、窗口和未来不变性 |
| `tests/unit/factors/test_stock_factors.py` | 验证股票市场因子改用前复权且共享波动率注册正确 |
| `tests/unit/factors/test_registry.py` | 验证 source-sensitive hash 与注册顺序无关 |
| `tests/integration/test_forward_adjustment_pipeline.py` | 验证 BaoStock Raw→Canonical→Snapshot→FORWARD→因子完整链路 |

---

### Task 1: 修正 BaoStock 历史日线的业务可用时间

**Files:**
- Modify: `src/quant_core/data/sources/baostock.py`
- Modify: `src/quant_core/data/mappers/baostock.py`
- Modify: `tests/unit/data/sources/test_baostock.py`
- Modify: `tests/unit/data/mappers/test_baostock_mapper.py`
- Modify: `tests/point_in_time/test_universe_builder.py`

**Interfaces:**
- Consumes: `PublishedPartition.retrieved_at`、BaoStock raw `date`
- Produces: `def _daily_availability(partition: PublishedPartition, trade_date: date) -> dict[str, object | None]`
- Produces: `def _require_completed_session(self, trading_day: date) -> None`
- Produces: daily bar/status 的 `available_at: Datetime(us, UTC)`、`availability_source: String`、`pit_usable: Boolean`、`ingested_at: Datetime(us, UTC)`

- [ ] **Step 1: 写历史日线双时间语义失败测试**

在 mapper 测试中构造 `trade_date=2024-04-26`、`retrieved_at=2026-08-01T02:00:00Z` 的 daily raw partition，同时验证 bar 和 security status：

```python
expected_available = datetime(2024, 4, 26, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")).astimezone(UTC)

assert bars["available_at"].item() == expected_available
assert statuses["available_at"].item() == expected_available
assert bars["ingested_at"].item() == datetime(2026, 8, 1, 2, 0, tzinfo=UTC)
assert bars["availability_source"].item() == "MARKET_CLOSE_DERIVED"
assert bars["pit_usable"].item() is True
```

再构造同一交易日上海时间 14:30 抓取的 row，断言 `available_at=None`、`pit_usable=False`、`availability_source="MARKET_SESSION_INCOMPLETE"`。这是 mapper 的纵深防御；正常生产采集应在 Source 边界更早拒绝。

- [ ] **Step 2: 运行测试并确认失败**

Run:

```powershell
uv run pytest tests/unit/data/mappers/test_baostock_mapper.py -k "market_close or incomplete_session" -v --basetemp=C:\t\fwd1-red
```

Expected: FAIL；当前 `available_at` 等于 2026 抓取时间，且收盘前 row 被标为可用。

- [ ] **Step 3: 在 Source 边界拒绝未完成交易日**

在 `BaoStockClient` 增加：

```python
def _require_completed_session(self, trading_day: date) -> None:
    now = self._clock().astimezone(_SHANGHAI)
    close_at = datetime.combine(trading_day, time(15, 0), _SHANGHAI)
    if now < close_at:
        raise ValueError("daily market session is not complete")
```

`_fetch_all_market_daily_bars()` 在调用 provider API 前逐日检查。显式证券 route 在进入 date/instrument chunks 前调用一次 `_load_trade_calendar(start, end)`，仅对返回的 `open_dates` 逐日检查；不得仅比较 `end` 而误拒周末端点。Source 测试使用注入 clock，断言 14:59 拒绝且 history gateway 未被调用、15:00 允许，并断言周末 `end` 不会被误拒。

- [ ] **Step 4: 实现日线专用 availability helper**

在 mapper 中新增：

```python
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_DAILY_MARKET_CLOSE = time(15, 0)


def _daily_availability(
    partition: PublishedPartition, trade_date: date
) -> dict[str, object | None]:
    retrieved_at = partition.retrieved_at.astimezone(UTC)
    market_close = datetime.combine(
        trade_date, _DAILY_MARKET_CLOSE, _SHANGHAI
    ).astimezone(UTC)
    complete = retrieved_at >= market_close
    return {
        "source": partition.provider,
        "source_version": partition.content_hash,
        "available_at": market_close if complete else None,
        "availability_source": (
            "MARKET_CLOSE_DERIVED" if complete else "MARKET_SESSION_INCOMPLETE"
        ),
        "pit_usable": complete,
        "ingested_at": retrieved_at,
    }
```

`_daily_rows()` 对 bar/status 共用 `_daily_availability(partition, trade_date)`；calendar/instrument 继续使用 `_raw_availability()`。

- [ ] **Step 5: 添加真实 mapper→历史股票池回归**

在 `test_universe_builder.py` 中通过 BaoStock mapper 生成历史 bar/status，而不是手工写 `available_at`。把 2026 抓取的 2024-04-26 数据发布进 snapshot，构建 `as_of=2024-04-26` 股票池，断言该证券不会因 `STATUS_MISSING` 或错误的 availability 被剔除；同时验证 2024-04-26 上海收盘前的 timestamp 不可见。

- [ ] **Step 6: 运行目标、PIT 与静态检查**

Run:

```powershell
uv run pytest tests/unit/data/sources/test_baostock.py tests/unit/data/mappers/test_baostock_mapper.py tests/point_in_time/test_universe_builder.py -v --basetemp=C:\t\fwd1
uv run ruff format --check src/quant_core/data/sources/baostock.py src/quant_core/data/mappers/baostock.py tests/unit/data/sources/test_baostock.py tests/unit/data/mappers/test_baostock_mapper.py tests/point_in_time/test_universe_builder.py
uv run ruff check src/quant_core/data/sources/baostock.py src/quant_core/data/mappers/baostock.py tests/unit/data/sources/test_baostock.py tests/unit/data/mappers/test_baostock_mapper.py tests/point_in_time/test_universe_builder.py
uv run mypy src
git diff --check
```

Expected: all pass。

- [ ] **Step 7: 提交**

```powershell
git add src/quant_core/data/sources/baostock.py src/quant_core/data/mappers/baostock.py tests/unit/data/sources/test_baostock.py tests/unit/data/mappers/test_baostock_mapper.py tests/point_in_time/test_universe_builder.py
git commit -m "fix: map BaoStock daily availability to market close"
```

---

### Task 2: 实现 BaoStock 涨跌幅前复权核心

**Files:**
- Modify: `src/quant_core/data/adjustments.py`
- Modify: `tests/point_in_time/test_adjustments.py`

**Interfaces:**
- Consumes: `ResearchDataRepository.bars(snapshot_id, instruments, start, as_of)` 的 raw bars
- Produces: `AdjustmentMode.FORWARD = "FORWARD"`
- Produces: `def _forward_adjust(frame: pl.DataFrame, end: date) -> tuple[pl.DataFrame, list[float]]`
- Produces: `def _required_positive(value: object, field: str) -> float`
- Produces: `def _validate_unique_bar_keys(frame: pl.DataFrame) -> None`
- Preserves: `PriceAdjustmentService.bars(...) -> pl.LazyFrame`

- [ ] **Step 1: 为公式和字段边界写失败测试**

构造单证券四个 session：

```python
raw_close =    [10.0, 12.0, 8.4, 9.0]
raw_preclose = [0.0, 10.0, 8.0, 8.4]
```

以最后一天为锚点，独立参考：

```python
expected = [2.0 / 3.0, 2.0 / 3.0, 1.0, 1.0]
```

断言：

```python
assert result["adjustment_factor"].to_list() == pytest.approx(expected)
assert result["close"].to_list() == pytest.approx([20.0 / 3.0, 8.0, 8.4, 9.0])
assert result["preclose"].to_list() == pytest.approx([0.0, 20.0 / 3.0, 8.0, 8.4])
assert result["close"].to_list()[1] == pytest.approx(result["preclose"].to_list()[2])
assert result["volume"].to_list() == raw_volume
assert result["amount"].to_list() == raw_amount
assert result["adjustment_mode"].unique().to_list() == ["FORWARD"]
```

另加：两个证券隔离、乱序输入、`end < as_of` 的锚点、空 bars、停牌日期缺口、IPO 首行 `preclose=None/0`。

- [ ] **Step 2: 运行公式测试并确认失败**

```powershell
uv run pytest tests/point_in_time/test_adjustments.py -k "forward" -v --basetemp=C:\t\fwd2-red
```

Expected: FAIL with missing `AdjustmentMode.FORWARD`。

- [ ] **Step 3: 实现对数域累计因子**

实现形状如下：

```python
def _required_positive(value: object, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field} must be finite and positive")
    return float(value)


def _validate_unique_bar_keys(frame: pl.DataFrame) -> None:
    if frame.select(
        pl.struct("instrument_id", "trade_date").is_duplicated().any()
    ).item():
        raise ValueError("duplicate daily bar key")


def _forward_adjust(
    frame: pl.DataFrame, end: date
) -> tuple[pl.DataFrame, list[float]]:
    ordered = frame.sort("instrument_id", "trade_date")
    _validate_unique_bar_keys(ordered)
    factors_by_key: dict[tuple[str, date], float] = {}
    for group in ordered.partition_by("instrument_id", maintain_order=True):
        rows = group.select("instrument_id", "trade_date", "close", "preclose").to_dicts()
        log_factor = 0.0
        last = len(rows) - 1
        factors = [1.0] * len(rows)
        for index in range(last, 0, -1):
            previous_close = _required_positive(rows[index - 1]["close"], "close")
            current_preclose = _required_positive(rows[index]["preclose"], "preclose")
            log_factor += log(current_preclose) - log(previous_close)
            factor = exp(log_factor)
            if not isfinite(factor) or factor <= 0:
                raise ValueError("forward adjustment factor must be finite and positive")
            factors[index - 1] = factor
        for row, factor in zip(rows, factors, strict=True):
            factors_by_key[(cast(str, row["instrument_id"]), cast(date, row["trade_date"]))] = factor
    filtered = ordered.filter(pl.col("trade_date") <= end)
    filtered_factors = [
        factors_by_key[(row["instrument_id"], row["trade_date"])]
        for row in filtered.select("instrument_id", "trade_date").to_dicts()
    ]
    return filtered, filtered_factors
```

返回 factors 与过滤后的 frame 一一对应。价格列通过 `pl.col(column) * factor_series` 调整，非价格列不修改。

- [ ] **Step 4: 接入 `PriceAdjustmentService.bars()`**

`FORWARD` 分支：

```python
raw = self._repository.bars(
    snapshot_id, instruments, start, as_of
).collect()
adjusted, factors = _forward_adjust(raw, end)
return _with_metadata(
    adjusted,
    AdjustmentMode.FORWARD,
    as_of,
    adjustment_factors=factors,
).lazy()
```

公司行动 metadata 列保持稳定 schema，但在 `FORWARD` 下使用单位 event factor、null event availability 和 typed empty components，不读取 `corporate_actions_as_of()`。

- [ ] **Step 5: 添加异常输入失败测试**

覆盖并断言稳定 `ValueError`：

- 重复 `(instrument_id, trade_date)`；
- 非首行 `preclose` 为 null、0、负数、NaN、Inf；
- 前一 session `close` 为 null、0、负数、NaN、Inf；
- 累计结果非有限；
- `start > end`、`as_of < end`。

加入真实变异：临时把 `preclose/previous_close` 改成倒数，确认固定样本失败，再恢复。

- [ ] **Step 6: 运行复权/PIT/全套相关测试**

```powershell
uv run pytest tests/point_in_time/test_adjustments.py -v --basetemp=C:\t\fwd2
uv run pytest tests/point_in_time -v --basetemp=C:\t\fwd2-pit
uv run ruff format --check src/quant_core/data/adjustments.py tests/point_in_time/test_adjustments.py
uv run ruff check src/quant_core/data/adjustments.py tests/point_in_time/test_adjustments.py
uv run mypy src
git diff --check
```

- [ ] **Step 7: 提交**

```powershell
git add src/quant_core/data/adjustments.py tests/point_in_time/test_adjustments.py
git commit -m "feat: add BaoStock forward price adjustment"
```

---

### Task 3: 行情因子迁移、signal-local 换基与 canonical 注册

**Files:**
- Create: `src/quant_core/factors/builtin/code_hash.py`
- Modify: `src/quant_core/factors/builtin/momentum.py`
- Modify: `src/quant_core/factors/builtin/risk.py`
- Modify: `src/quant_core/factors/builtin/__init__.py`
- Modify: `tests/unit/factors/test_etf_factors.py`
- Modify: `tests/unit/factors/test_stock_factors.py`
- Modify: `tests/unit/factors/test_registry.py`

**Interfaces:**
- Consumes: `FORWARD` bars containing `close`、`available_at`、`adjustment_factor`
- Produces: `def builtin_source_hash(spec: FactorSpec) -> str`
- Produces: `def _builtin_source_bytes() -> Mapping[str, bytes]`
- Produces: `def _hash_source_bundle(spec: FactorSpec, sources: Mapping[str, bytes]) -> str`
- Produces: `def register_builtin(registry: FactorRegistry, factor: Factor) -> None`
- Produces: `def _positive_finite(value: object) -> float | None`
- Preserves: `register_etf_factors(...)`、`register_stock_factors(...)`

- [ ] **Step 1: 写 signal-local 前视失败测试**

让 fake service 返回以 `ctx.end` 锚定的 global forward closes/factors。固定早期 `signal_date`，向末尾追加一次跳变并延长 `ctx.end`，断言早期五类市场信号的 value/available_at 不变。

独立参考换基：

```python
signal_factor = window[-1]["adjustment_factor"]
signal_local = [
    row["close"] / signal_factor
    for row in window
]
```

测试必须证明若省略 `/ signal_factor`，`trend_120d_v1` 会改变。

- [ ] **Step 2: 写模式与元数据失败测试**

断言 fake service 收到：

```python
assert mode is AdjustmentMode.FORWARD
assert as_of == ctx.end
```

五个 ETF 因子和股票市场因子的 `FactorSpec.parameters["adjustment_mode"] == "FORWARD"`。`available_at` 只取窗口 bar 的真实最大值；未知时间窗口 invalid/null。

- [ ] **Step 3: 简化市场窗口实现**

删除行情因子对公司行动 component lineage 的依赖。`_validate_adjusted_bars()` 最小要求：

```python
required = {
    "instrument_id": pl.String,
    "trade_date": pl.Date,
    "close": pl.Float64,
    "available_at": pl.Datetime("us", "UTC"),
    "adjustment_factor": pl.Float64,
}
```

实现：

```python
def _positive_finite(value: object) -> float | None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value <= 0
    ):
        return None
    return float(value)


def _signal_local_closes(
    window: Sequence[dict[str, object]],
) -> list[float] | None:
    signal_factor = _positive_finite(window[-1]["adjustment_factor"])
    if signal_factor is None:
        return None
    closes: list[float] = []
    for row in window:
        adjusted = _positive_finite(row["close"])
        factor = _positive_finite(row["adjustment_factor"])
        if adjusted is None or factor is None:
            return None
        value = adjusted / signal_factor
        if not isfinite(value) or value <= 0:
            return None
        closes.append(value)
    return closes
```

这里检查每行 factor 是为了拒绝 malformed service output；换基只除以信号日 factor。保留窗口历史 fallback、多证券隔离、排序、重复键和精确输出 schema。

- [ ] **Step 4: 创建 source-sensitive 内置因子 hash**

`code_hash.py` 对 `quant_core.data.adjustments`、`quant_core.factors.base` 以及 `quant_core.factors.builtin` 目录下所有生产 `.py` 文件按逻辑资源名排序并哈希内容，再加入 spec canonical ref 与 canonical parameters。这样前复权服务或任一内置实现变化都会失效缓存：

```python
def _builtin_source_bytes() -> Mapping[str, bytes]:
    sources: dict[str, bytes] = {
        "quant_core/data/adjustments.py": resources.files("quant_core.data")
        .joinpath("adjustments.py")
        .read_bytes(),
        "quant_core/factors/base.py": resources.files("quant_core.factors")
        .joinpath("base.py")
        .read_bytes(),
    }
    package = resources.files("quant_core.factors.builtin")
    for resource in package.iterdir():
        if resource.is_file() and resource.name.endswith(".py"):
            sources[f"quant_core/factors/builtin/{resource.name}"] = resource.read_bytes()
    return MappingProxyType(sources)


def _hash_source_bundle(
    spec: FactorSpec, sources: Mapping[str, bytes]
) -> str:
    digest = hashlib.sha256()
    for logical_name, payload in sorted(sources.items()):
        digest.update(logical_name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
    digest.update(spec.canonical_ref.encode("utf-8"))
    digest.update(canonical_json_bytes(thaw_json(spec.parameters)))
    return digest.hexdigest()


def builtin_source_hash(spec: FactorSpec) -> str:
    return _hash_source_bundle(spec, _builtin_source_bytes())
```

使用 `importlib.resources.files("quant_core.factors.builtin")`，不得依赖进程随机 hash、mtime 或绝对路径。所有内置源码变化保守地使全部内置因子缓存失效。

- [ ] **Step 5: 实现顺序无关 canonical 注册**

```python
def register_builtin(registry: FactorRegistry, factor: Factor) -> None:
    expected_hash = builtin_source_hash(factor.spec)
    try:
        existing_ref = registry.resolve(factor.spec.canonical_ref)
    except ValueError:
        registry.register(factor, code_hash=expected_hash)
        return
    if registry.code_hash(existing_ref) != expected_hash:
        raise ValueError(
            f"conflicting built-in implementation: {factor.spec.canonical_ref}"
        )
```

ETF/股票入口都通过该 helper 注册。共享 `volatility_60d_v1@1.0.0` 的第二次注册在实现 hash 相同时幂等跳过，在不同时明确冲突。

- [ ] **Step 6: 添加注册/hash 回归**

覆盖：

```python
register_etf_factors(...)
register_stock_factors(...)
```

和相反顺序，断言 canonical refs、`volatility_60d_v1@1.0.0` code hash 完全相同。重复同入口也幂等。对 `_hash_source_bundle()` 传入固定 mapping，复制后变异 `adjustments.py` 或任一 builtin source 的一个字节，断言 digest 改变；同 mapping 重复计算 hash 相同。

- [ ] **Step 7: 运行目标和 factor 回归**

```powershell
uv run pytest tests/unit/factors/test_etf_factors.py tests/unit/factors/test_stock_factors.py tests/unit/factors/test_registry.py -v --basetemp=C:\t\fwd3
uv run pytest tests/unit/factors -v --basetemp=C:\t\fwd3-factors
uv run ruff format --check src/quant_core/factors tests/unit/factors
uv run ruff check src/quant_core/factors tests/unit/factors
uv run mypy src
git diff --check
```

- [ ] **Step 8: 提交**

```powershell
git add src/quant_core/factors/builtin tests/unit/factors
git commit -m "feat: migrate market factors to forward adjustment"
```

---

### Task 4: BaoStock 前复权端到端与缓存验收

**Files:**
- Create: `tests/integration/test_forward_adjustment_pipeline.py`
- Modify: `tests/integration/test_factor_materialization.py`
- Modify: `docs/architecture/2026-07-30-personal-a-share-quant-platform-technical-design.md`

**Interfaces:**
- Consumes: BaoStock raw daily batch、canonical mapper、snapshot publication、`PriceAdjustmentService(FORWARD)`、`FactorEngine`
- Produces: 一条无需 CORPORATE_ACTION 数据集即可运行的市场因子生产链路

- [ ] **Step 1: 写 Raw→Canonical→Snapshot→FORWARD 集成失败测试**

使用固定 BaoStock raw rows：

```text
2024-01-02 close=10.0 preclose=0
2024-01-03 close=12.0 preclose=10.0
2024-01-04 close=8.4  preclose=8.0
2024-01-05 close=9.0  preclose=8.4
```

通过真实 mapper、canonical writer、snapshot catalog 和 `SnapshotResearchRepository`，snapshot 故意不包含 `CORPORATE_ACTION`。调用 `FORWARD`，断言不抛 `SnapshotDatasetMissing` 且结果与 Task 2 参考一致。

- [ ] **Step 2: 运行集成测试并确认失败**

```powershell
uv run pytest tests/integration/test_forward_adjustment_pipeline.py -v --basetemp=C:\t\fwd4-red
```

Expected: 在 Task 2/3 完成前因模式或市场因子仍依赖 BACKWARD 而失败。

- [ ] **Step 3: 扩展为市场因子物化与缓存测试**

构造至少 121 个 raw observed sessions（含一个 `close/preclose` 跳变），发布无公司行动数据集的 snapshot：

- 注册五个 ETF 因子并通过 `FactorEngine.compute()` 物化；
- 第二次相同计算不再次读取 provider、不重写 Parquet/manifest；
- 断言 artifact spec 参数为 `FORWARD`；
- 使用旧 `BACKWARD` spec/hash 计算 key，断言与新 artifact key 不同；
- 追加未来跳变并延长 ctx，断言原 signal rows 内容 hash 不变。

- [ ] **Step 4: 更新技术设计中的研究价格口径**

把 ETF/股票行情因子的统一研究价格从“公司行动后复权”改为：

```text
BaoStock raw close/preclose 涨跌幅前复权；
信号日独立锚定；只调整 OHLC/preclose；volume/amount 保持真实值。
```

明确 `BACKWARD` 是兼容接口，不是 MVP 因子默认口径；财务 capability gate 仍是后续生产门禁。

- [ ] **Step 5: 执行总体验收**

```powershell
uv run pytest tests/point_in_time tests/unit/factors tests/integration/test_forward_adjustment_pipeline.py tests/integration/test_factor_materialization.py -v --basetemp=C:\t\fwd4-plan
uv run pytest -v --basetemp=C:\t\fwd4-full
uv run ruff format --check src tests
uv run ruff check src tests
uv run mypy src
git diff --check
```

Expected: 全部通过；不得复现 `_PartitionLock` 负 sleep。若出现已知 race，仅记录并由整体验收修复任务按 systematic-debugging 处理，不在本任务越界修改。

- [ ] **Step 6: 提交**

```powershell
git add tests/integration/test_forward_adjustment_pipeline.py tests/integration/test_factor_materialization.py docs/architecture/2026-07-30-personal-a-share-quant-platform-technical-design.md
git commit -m "test: verify BaoStock forward-adjusted factor pipeline"
```

---

## 验收门槛

- `FORWARD` 完全由 raw `close/preclose` 计算，不读取公司行动数据集。
- `open/high/low/close/preclose` 正确调整；`volume/amount` 保持不变。
- 今日抓取的历史日线在历史收盘后可用于 PIT 股票池。
- 所有 ETF/股票市场因子默认使用 `FORWARD`，逐信号日无前视。
- ETF/股票注册顺序无关，共享波动率身份和 code hash 一致。
- 内置源码变化会使缓存 key 改变，旧算法产物不再命中。
- 无 `CORPORATE_ACTION` 的真实 snapshot 能物化市场因子。
- 目标测试、PIT/factor/integration 子集、全套 pytest、Ruff、mypy 和 diff-check 全部通过。
