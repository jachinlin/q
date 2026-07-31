# BaoStock 全市场日线混合路由 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 BaoStock 全市场日线采集切换为按开市日调用 `query_daily_history_k_AStock(date)`，同时保留非空证券列表的 `query_history_k_data_plus` 定向采集路径。

**Architecture:** BaoStock Gateway 只扩展 SDK 方法转发；`BaoStockClient.fetch_daily_bars` 根据证券参数选择“按交易日全市场”或“按证券区间”路径。全市场响应仍先发布为供应商原生 Raw Parquet，再由现有 Mapper、Curated、Quality 和 Snapshot 链路处理；适配器版本升级确保旧 checkpoint 不被复用。

**Tech Stack:** Python 3.12、baostock 0.9.30、PyArrow、Polars、pytest、Ruff、mypy、SQLite/Alembic。

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-07-31-baostock-daily-market-route-design.md`。
- `instruments is None` 与空序列必须走 `query_daily_history_k_AStock`；非空序列必须走 `query_history_k_data_plus`。
- 全市场路径只查询交易日历中 `is_trading_day == "1"` 的日期，每个开市日生成一个 RawBatch。
- 全市场路径不得调用 `query_history_k_data_plus`；指定证券路径不得调用 `query_daily_history_k_AStock`。
- BaoStock SDK 的全市场方法只接收 `date`，不得伪造 `fields`、`frequency` 或 `adjustflag` 请求参数。
- Raw 行继续保留 BaoStock 字段名、字段顺序和字符串值；Canonical Mapper 不访问网络。
- 全市场响应字段必须严格等于现有 18 列 `DAILY_BAR_FIELDS`，且每行 `adjustflag == "3"`。
- 开市日空响应、字段漂移、非法行和非法复权语义必须失败关闭，不能发布空或降级 Raw。
- `fetch_range` 必须复用已经发布的交易日历查询结果，不得重复调用 `query_trade_dates`。
- 不并发调用 BaoStock 的全局 socket SDK；Pipeline 已有后台 lease keeper 继续负责长请求续租。
- `BAOSTOCK_SOURCE_ADAPTER_VERSION` 必须升级为 `baostock-source-adapter-v2`。
- 不修改 Canonical schema、Snapshot manifest、TuShare 公共契约、因子、股票池、回测或 Dashboard。
- 代码和测试使用英文标识符；设计、计划和用户文档使用中文。

## 文件结构与职责

| 文件 | 作用 |
|---|---|
| `src/quant_core/data/sources/baostock.py` | SDK Gateway 协议、真实 SDK 转发、混合路由、日历复用、Raw 请求元数据、重试和结构化错误。 |
| `tests/unit/data/sources/test_baostock.py` | Gateway 转发、路由选择、开市日过滤、响应校验、重试和日志的离线单元测试。 |
| `tests/integration/test_baostock_raw_ingest.py` | 新全市场 API 到不可变 Raw Parquet 的集成测试。 |
| `tests/integration/test_raw_to_curated.py` | 新 Raw 请求语义经现有 BaoStockMapper 生成不变 Canonical 结果的测试。 |
| `tests/integration/test_data_pipeline.py` | bootstrap/update、checkpoint 指纹、后台续租与端到端 Snapshot 回归。 |
| `tests/regression/test_data_snapshot_golden.py` | 固定离线 fixture 的 Snapshot 内容语义回归。 |
| `src/quant_core/cli.py` | 默认 BaoStock fetch-config fingerprint 与适配器版本注入。 |
| `docs/architecture/2026-07-30-personal-a-share-quant-platform-technical-design.md` | 将旧“全量按证券分块”描述更新为混合路由。 |

---

### Task 1: 扩展 BaoStock SDK Gateway

**Files:**
- Modify: `src/quant_core/data/sources/baostock.py:49-163`
- Test: `tests/unit/data/sources/test_baostock.py`

**Interfaces:**
- Consumes: BaoStock SDK 0.9.30 的 `query_daily_history_k_AStock(date="")`。
- Produces: `BaoStockGateway.query_daily_history_k_AStock(date: str = "") -> BaoStockCursor`，供 Task 2 的全市场路由调用。

- [ ] **Step 1: 写 Gateway 转发失败测试**

在测试 SDK fake 中记录日期，并断言 Gateway 只传一个日期参数：

```python
class DailyMarketSdk:
    def __init__(self) -> None:
        self.dates: list[str] = []

    def query_daily_history_k_AStock(self, date: str = "") -> FakeCursor:
        self.dates.append(date)
        return FakeCursor([[make_row(date, "sh.600000")]])


def test_sdk_gateway_forwards_daily_market_date_without_extra_parameters() -> None:
    sdk = DailyMarketSdk()
    gateway = BaoStockSdkGateway(sdk=sdk)  # type: ignore[arg-type]

    result = gateway.query_daily_history_k_AStock("2026-01-02")

    assert result.fields == DAILY_BAR_FIELDS
    assert sdk.dates == ["2026-01-02"]
```

- [ ] **Step 2: 运行测试并确认 RED**

Run:

```powershell
uv run pytest tests/unit/data/sources/test_baostock.py -k "sdk_gateway_forwards_daily_market" -v
```

Expected: FAIL，提示 `BaoStockSdkGateway` 没有 `query_daily_history_k_AStock`。

- [ ] **Step 3: 扩展协议和真实 Gateway**

在 `BaoStockGateway`、`_BaoStockSdk` 和 `BaoStockSdkGateway` 中加入完全相同的签名：

```python
def query_daily_history_k_AStock(self, date: str = "") -> BaoStockCursor:
    """Query all A-share daily bars for one provider date."""


class BaoStockSdkGateway:
    def query_daily_history_k_AStock(self, date: str = "") -> BaoStockCursor:
        return self._sdk.query_daily_history_k_AStock(date)
```

不得在 Gateway 中读取 cursor、转换代码或写 Raw。

- [ ] **Step 4: 运行 Gateway 与现有 SDK 边界测试**

Run:

```powershell
uv run pytest tests/unit/data/sources/test_baostock.py -k "sdk_gateway or gateway" -v
uv run mypy src/quant_core/data/sources/baostock.py
```

Expected: PASS；mypy 无协议缺口。

- [ ] **Step 5: 提交 Task 1**

```powershell
git add src/quant_core/data/sources/baostock.py tests/unit/data/sources/test_baostock.py
git commit -m "feat: expose BaoStock daily market API"
```

---

### Task 2: 实现全市场按交易日采集和严格 Raw 契约

**Files:**
- Modify: `src/quant_core/data/sources/baostock.py:165-612`
- Test: `tests/unit/data/sources/test_baostock.py`

**Interfaces:**
- Consumes: Task 1 的 `BaoStockGateway.query_daily_history_k_AStock(date)`；现有 `RawBatch`、`DAILY_BAR_FIELDS`、`InstrumentCatalog` 和 `_retry`。
- Produces: 保持公共签名 `fetch_daily_bars(start: date, end: date, instruments: Sequence[InstrumentId] | None = None) -> Iterable[RawBatch]`；新增 `_fetch_all_market_daily_bars(open_dates: Sequence[date], catalog_instruments: Sequence[InstrumentId]) -> Iterable[RawBatch]` 与 `_fetch_selected_daily_bars(start: date, end: date, instruments: Sequence[InstrumentId]) -> Iterable[RawBatch]`。

- [ ] **Step 1: 扩展 FakeGateway 并写路由 RED 测试**

让 `FakeGateway` 分别记录两类调用：

```python
self.query_calls: list[dict[str, str]] = []
self.daily_market_calls: list[str] = []
self.daily_market_outcomes: dict[str, deque[QueryOutcome]] = {}


def query_daily_history_k_AStock(self, date: str = "") -> FakeCursor:
    self.daily_market_calls.append(date)
    outcomes = self.daily_market_outcomes.get(date)
    if outcomes:
        outcome = outcomes.popleft()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome
    return FakeCursor([[make_row(date, "sh.600000")]])
```

增加三个测试：

```python
@pytest.mark.parametrize("selection", [None, []])
def test_all_market_selection_uses_daily_api_only(selection: object) -> None:
    gateway = FakeGateway()
    client = make_client(gateway)
    client.login()

    batches = tuple(
        client.fetch_daily_bars(
            date(2026, 1, 2),
            date(2026, 1, 3),
            selection,  # type: ignore[arg-type]
        )
    )

    assert gateway.daily_market_calls == ["2026-01-02"]
    assert gateway.query_calls == []
    assert [batch.request["api"] for batch in batches] == [
        "query_daily_history_k_AStock"
    ]


def test_selected_instruments_keep_range_api_only() -> None:
    gateway = FakeGateway()
    client = make_client(gateway)
    client.login()

    tuple(
        client.fetch_daily_bars(
            date(2026, 1, 2),
            date(2026, 1, 2),
            [instrument(Exchange.SSE, "600000")],
        )
    )

    assert gateway.daily_market_calls == []
    assert [call["code"] for call in gateway.query_calls] == ["sh.600000"]
```

`FakeGateway.query_trade_dates` 必须每次返回一个新 cursor，避免已消费 cursor 影响重复调用测试。

- [ ] **Step 2: 运行路由测试并确认 RED**

```powershell
uv run pytest tests/unit/data/sources/test_baostock.py -k "all_market_selection or selected_instruments_keep" -v
```

Expected: 全市场断言失败，因为当前代码仍调用逐证券接口。

- [ ] **Step 3: 写最小混合路由**

保留现有逐证券循环并移动到私有方法；公共方法只负责状态、日期范围和路由：

```python
def fetch_daily_bars(
    self,
    start: date,
    end: date,
    instruments: Sequence[InstrumentId] | None = None,
) -> Iterable[RawBatch]:
    if not self._logged_in:
        raise self._state_error("fetch_daily_bars")
    if start > end:
        raise ValueError("start must not follow end")
    if instruments is None or len(instruments) == 0:
        if instruments is not None:
            self._logger.info(
                "empty instrument selection resolved as all-market daily route",
                extra={"event": "empty_instruments_resolved_as_all", "scope": "ALL"},
            )
        _, catalog_instruments = self._resolve_instruments(start, end, instruments)
        _, open_dates = self._load_trade_calendar(start, end)
        yield from self._fetch_all_market_daily_bars(open_dates, catalog_instruments)
        return
    yield from self._fetch_selected_daily_bars(start, end, instruments)
```

这里同时解决旧 ledger 中 `start > end` 返回零批次的 deferred minor。

- [ ] **Step 4: 写开市日、Schema、复权和空响应 RED 测试**

覆盖以下可观察行为：

```python
def test_all_market_batch_records_response_scope_hash() -> None:
    gateway = FakeGateway()
    gateway.daily_market_outcomes["2026-01-02"] = deque(
        [
            FakeCursor(
                [
                    [
                        make_row("2026-01-02", "sz.000001"),
                        make_row("2026-01-02", "sh.600000"),
                    ]
                ]
            )
        ]
    )
    client = make_client(gateway)
    client.login()

    batch = tuple(client.fetch_daily_bars(date(2026, 1, 2), date(2026, 1, 3), None))[0]

    assert batch.request["scope"] == "ALL"
    assert batch.request["date"] == "2026-01-02"
    assert batch.request["response_instrument_count"] == 2
    assert len(str(batch.request["response_instruments_sha256"])) == 64
    assert batch.request["catalog_instrument_count"] == 2
    assert len(str(batch.request["catalog_instruments_sha256"])) == 64
    assert batch.schema == DAILY_BAR_FIELDS
```

另外分别注入并断言结构化失败：

- cursor fields 顺序改变 → `DATA_PROVIDER_BAOSTOCK_SCHEMA`。
- 行列数不是 18 → `DATA_PROVIDER_BAOSTOCK_SCHEMA`。
- `adjustflag != "3"` → `DATA_PROVIDER_BAOSTOCK_SCHEMA`。
- 开市日连续返回空 cursor → 重试 `max_attempts` 次后返回 `DATA_PROVIDER_BAOSTOCK_EMPTY_OPEN_DAY`，severity 为 FATAL。
- 首次连接异常、第二次成功 → 只重试同一日期并成功。

- [ ] **Step 5: 实现全市场 cursor 读取和请求元数据**

先把“消费并校验一个 cursor”与“创建 cursor、执行 retry”拆开，避免嵌套 retry：

```python
def _consume_cursor(
    self,
    operation: str,
    cursor: BaoStockCursor,
    fields: tuple[str, ...],
) -> list[dict[str, JsonValue]]:
    self._raise_provider_error(cursor, operation=operation)
    if tuple(cursor.fields) != fields:
        raise self._schema_error(
            operation,
            f"cursor fields do not match the fixed {operation} schema",
            expected=list(fields),
            actual=list(cursor.fields),
        )
    rows: list[dict[str, JsonValue]] = []
    while True:
        has_row = cursor.next()
        self._raise_provider_error(cursor, operation=operation)
        if not has_row:
            return rows
        values = tuple(cursor.get_row_data())
        if len(values) != len(fields):
            raise self._schema_error(
                operation,
                f"cursor row length does not match the fixed {operation} schema",
                expected=len(fields),
                actual=len(values),
            )
        rows.append(dict(zip(fields, values, strict=True)))
```

现有 `_read_cursor` 改为在自己的 `_retry` 回调中调用 `_consume_cursor`。然后实现全市场专用查询：

```python
def _fetch_all_market_rows(self, trading_day: date) -> list[dict[str, JsonValue]]:
    operation = "query_daily_history_k_AStock"

    def perform_query() -> list[dict[str, JsonValue]]:
        rows = self._consume_cursor(
            operation,
            self._gateway.query_daily_history_k_AStock(trading_day.isoformat()),
            DAILY_BAR_FIELDS,
        )
        if not rows:
            raise self._empty_open_day_error(trading_day)
        invalid_adjustment = [
            row.get("adjustflag") for row in rows if row.get("adjustflag") != "3"
        ]
        if invalid_adjustment:
            raise self._schema_error(
                operation,
                "daily market response must use adjustflag 3",
                expected="3",
                actual=invalid_adjustment[0],
            )
        return rows

    return self._retry(operation, perform_query)
```

将 `_schema_error` 改为显式接收 `operation: str`，并更新旧调用点：

```python
@staticmethod
def _schema_error(
    operation: str,
    message: str,
    *,
    expected: object,
    actual: object,
) -> QuantError:
    return QuantError(
        ErrorDetail(
            code="DATA_PROVIDER_BAOSTOCK_SCHEMA",
            severity=Severity.SEVERE,
            message=message,
            context={"operation": operation, "expected": expected, "actual": actual},
            remediation="inspect the provider schema before accepting raw data",
            retryable=False,
        )
    )


@staticmethod
def _empty_open_day_error(trading_day: date) -> QuantError:
    return QuantError(
        ErrorDetail(
            code="DATA_PROVIDER_BAOSTOCK_EMPTY_OPEN_DAY",
            severity=Severity.FATAL,
            message="BaoStock returned no A-share daily bars for an open trading day",
            context={
                "operation": "query_daily_history_k_AStock",
                "date": trading_day.isoformat(),
            },
            remediation="retry the date or inspect BaoStock completeness",
            retryable=True,
        )
    )
```

最后一次 `_retry` 失败时原样抛出空开市日错误。

每个开市日生成一个 `RawBatch`：

```python
codes = sorted(str(row["code"]) for row in rows)
catalog_ids = sorted(item.canonical() for item in catalog_instruments)
request: dict[str, JsonValue] = {
    "api": "query_daily_history_k_AStock",
    "scope": "ALL",
    "date": trading_day.isoformat(),
    "frequency": "d",
    "catalog_instrument_count": len(catalog_ids),
    "catalog_instruments_sha256": hashlib.sha256(
        "\n".join(catalog_ids).encode()
    ).hexdigest(),
    "response_instrument_count": len(codes),
    "response_instruments_sha256": hashlib.sha256(
        "\n".join(codes).encode()
    ).hexdigest(),
}
yield RawBatch(
    provider="baostock",
    dataset="daily_bars",
    request=request,
    retrieved_at=self._clock(),
    schema=DAILY_BAR_FIELDS,
    rows=tuple(rows),
)
```

为使目录过滤可验证，在 `InstrumentListing` 末尾增加兼容默认值，`BaoStockHistoricalCatalog.from_raw_rows` 从 Raw `type` 字段赋值：

```python
@dataclass(frozen=True, slots=True)
class InstrumentListing:
    instrument_id: InstrumentId
    list_date: date
    delist_date: date | None
    provider_type: str = "1"
```

全量 `_resolve_instruments` 的生成器必须增加 `listing.provider_type == "1"` 条件。增加一个包含 `type="2"` 指数的测试，证明指数不进入 `catalog_*`；用两个开市日证明所有日期批次的 `catalog_instrument_count` 和 `catalog_instruments_sha256` 完全相同。不得用结束日存量证券替代历史范围。

- [ ] **Step 6: 增加结构化进度日志**

每个日期完成后写一条 INFO 日志：

```python
self._logger.info(
    "BaoStock all-market daily date completed",
    extra={
        "event": "baostock_all_market_daily_progress",
        "date": trading_day.isoformat(),
        "completed_dates": index,
        "total_dates": len(open_dates),
        "response_rows": len(rows),
    },
)
```

用 `caplog` 断言事件名、日期进度和响应行数；禁止把具体证券列表写入日志。

- [ ] **Step 7: 运行 Task 2 测试与静态检查**

```powershell
uv run pytest tests/unit/data/sources/test_baostock.py -v
uv run ruff check src/quant_core/data/sources/baostock.py tests/unit/data/sources/test_baostock.py
uv run mypy src/quant_core/data/sources/baostock.py
```

Expected: PASS；全市场和定向路径均被测试覆盖。

- [ ] **Step 8: 提交 Task 2**

```powershell
git add src/quant_core/data/sources/baostock.py tests/unit/data/sources/test_baostock.py
git commit -m "feat: route all-market bars by trading date"
```

---

### Task 3: 复用日历并贯通 Raw 到 Canonical

**Files:**
- Modify: `src/quant_core/data/sources/baostock.py:394-420`
- Modify: `tests/integration/test_baostock_raw_ingest.py`
- Modify: `tests/integration/test_raw_to_curated.py`

**Interfaces:**
- Consumes: Task 2 的混合路由和全市场 `RawBatch`。
- Produces: `_load_trade_calendar(start, end) -> tuple[RawBatch, tuple[date, ...]]`；`fetch_range` 一次日历查询后依次产出 instruments、trade_calendar 和 daily_bars。

- [ ] **Step 1: 写 fetch_range 日历复用 RED 测试**

扩展离线 Gateway，记录 `trade_calendar_calls`、`daily_market_calls` 和 `selected_calls`。测试：

```python
def test_fetch_range_reuses_calendar_and_uses_daily_market_api() -> None:
    gateway = OfflineGateway()
    client = make_offline_client(gateway)
    client.login()

    batches = tuple(client.fetch_range(date(2026, 1, 2), date(2026, 1, 3)))

    assert gateway.trade_calendar_calls == 1
    assert gateway.daily_market_calls == ["2026-01-02"]
    assert gateway.selected_calls == []
    assert [batch.dataset for batch in batches] == [
        "instruments",
        "trade_calendar",
        "daily_bars",
    ]
```

- [ ] **Step 2: 运行并确认 RED**

```powershell
uv run pytest tests/integration/test_baostock_raw_ingest.py -k "fetch_range_reuses_calendar" -v
```

Expected: FAIL，当前 `fetch_range` 的日线逻辑没有消费同一次日历结果。

- [ ] **Step 3: 抽取可复用日历加载方法**

实现：

```python
def _load_trade_calendar(
    self, start: date, end: date
) -> tuple[RawBatch, tuple[date, ...]]:
    rows = self._read_cursor(
        "query_trade_dates",
        lambda: self._gateway.query_trade_dates(
            start_date=start.isoformat(), end_date=end.isoformat()
        ),
        TRADE_CALENDAR_FIELDS,
    )
    open_dates = tuple(
        date.fromisoformat(str(row["calendar_date"]))
        for row in rows
        if row["is_trading_day"] == "1"
    )
    batch = RawBatch(
        provider="baostock",
        dataset="trade_calendar",
        request={"start_date": start.isoformat(), "end_date": end.isoformat()},
        retrieved_at=self._clock(),
        schema=TRADE_CALENDAR_FIELDS,
        rows=tuple(rows),
    )
    return batch, open_dates
```

`fetch_trade_calendar` 调用该方法后只 yield `batch`。`fetch_range` 调用一次并将 `open_dates` 直接传给私有全市场生成器：

```python
def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
    yield from self.fetch_instruments()
    calendar_batch, open_dates = self._load_trade_calendar(start, end)
    yield calendar_batch
    _, catalog_instruments = self._resolve_instruments(start, end, None)
    yield from self._fetch_all_market_daily_bars(open_dates, catalog_instruments)
```

- [ ] **Step 4: 更新 Raw 幂等集成测试**

将旧“全量证券块 × 日期块产生四个分区”的断言改为按开市日：

```python
assert [partition.request["date"] for partition in first] == ["2026-01-02"]
assert second == first
assert len(list(raw_root.rglob("*.parquet"))) == 1
assert len(list(raw_root.rglob("*.manifest.json"))) == 1
assert gateway.daily_market_calls == ["2026-01-02", "2026-01-02"]
assert gateway.selected_calls == []
```

另外保留一个非空证券列表集成断言，证明旧路径仍发布合法 Raw。

- [ ] **Step 5: 更新 Raw → Canonical 测试**

Fake Gateway 的全市场响应返回两只股票同一交易日，经过 `RawPartitionStore` 和 `BaoStockMapper` 后断言：

```python
assert daily.select("instrument_id").to_series().to_list() == [
    "SSE:600000",
    "SZSE:000001",
]
assert daily.select("trade_date").unique().item() == date(2026, 1, 2)
assert status.height == 2
assert set(daily.columns) == set(CANONICAL_SCHEMAS[DatasetKind.DAILY_BAR].columns)
```

不要修改 Mapper 以识别 API 名称；Mapper 只依赖 Raw dataset 与 schema。

- [ ] **Step 6: 运行 Task 3 集成测试**

```powershell
uv run pytest tests/integration/test_baostock_raw_ingest.py tests/integration/test_raw_to_curated.py -v
uv run pytest tests/unit/data/sources/test_baostock.py -v
```

Expected: PASS；离线测试不访问真实网络。

- [ ] **Step 7: 提交 Task 3**

```powershell
git add src/quant_core/data/sources/baostock.py tests/integration/test_baostock_raw_ingest.py tests/integration/test_raw_to_curated.py
git commit -m "test: verify daily market raw pipeline"
```

---

### Task 4: 版本迁移、端到端回归与中文文档同步

**Files:**
- Modify: `src/quant_core/data/sources/baostock.py:20`
- Modify: `src/quant_core/cli.py:220-267`
- Modify: `tests/integration/test_data_pipeline.py`
- Modify: `tests/regression/test_data_snapshot_golden.py`
- Modify: `docs/architecture/2026-07-30-personal-a-share-quant-platform-technical-design.md:388-425`

**Interfaces:**
- Consumes: Tasks 1–3 的新 Raw 采集语义。
- Produces: `BAOSTOCK_SOURCE_ADAPTER_VERSION = "baostock-source-adapter-v2"`；新的 pipeline/request fingerprint；保持不变的 Canonical/Snapshot 公共输出。

- [ ] **Step 1: 写版本和端到端 RED 测试**

增加明确断言：

```python
def test_daily_market_route_has_new_source_adapter_version() -> None:
    assert BAOSTOCK_SOURCE_ADAPTER_VERSION == "baostock-source-adapter-v2"


def test_bootstrap_all_market_source_never_uses_selected_api(tmp_path: Path) -> None:
    source = OfflineDailyMarketBaoStockSource()
    pipeline, _ = make_pipeline(tmp_path, source)

    result = pipeline.bootstrap()

    assert isinstance(result.snapshot_id, SnapshotId)
    assert source.daily_market_calls == [date(2026, 1, 5)]
    assert source.selected_calls == 0
```

补一个版本变化测试：相同日期窗口下 v1 与 v2 的 `PipelineVersions.source_adapter` 必须产生不同 run/request hash，不能复用旧 INGEST_RAW checkpoint。

- [ ] **Step 2: 运行并确认 RED**

```powershell
uv run pytest tests/integration/test_data_pipeline.py -k "new_source_adapter_version or never_uses_selected_api or component_version_change" -v
```

Expected: 版本断言 FAIL，或 fixture 尚未实现新 Gateway 方法。

- [ ] **Step 3: 升级版本并固定 fetch-config fingerprint**

修改：

```python
BAOSTOCK_SOURCE_ADAPTER_VERSION = "baostock-source-adapter-v2"
```

CLI fingerprint 保留重试参数，并显式区分路由语义：

```python
{
    "all_market_route": "query_daily_history_k_AStock-per-open-date-v1",
    "selected_route": "query_history_k_data_plus-v1",
    "selected_max_days_per_batch": source_config.max_days_per_batch,
    "selected_max_instruments_per_batch": source_config.max_instruments_per_batch,
    "max_attempts": source_config.max_attempts,
    "retry_backoff_seconds": list(source_config.retry_backoff_seconds),
    "retryable_error_codes": retryable_error_codes,
}
```

不要移除 `BaoStockConfig` 的两个分块字段；它们仍服务非空证券列表路径。

- [ ] **Step 4: 更新端到端 fixture 与黄金快照**

所有实现 `BaoStockGateway` 的离线 fake 增加 `query_daily_history_k_AStock`。默认 Pipeline fixture 必须走全市场 API；指定证券单测继续覆盖旧 API。

运行黄金测试，若差异只来自以下允许项则更新期望：

- Raw request metadata 的 `api/scope/date/response_*`。
- source adapter/fetch-config fingerprint。

Canonical 业务列、主键、行数、质量结论和 Snapshot dataset 集合不得变化。若这些内容变化，先修复实现，不更新黄金期望。

- [ ] **Step 5: 同步中文技术设计**

替换 7.1 中旧全量语义：

```text
None/空序列：按交易日调用 query_daily_history_k_AStock；每个开市日一个 RawBatch。
非空序列：按证券和日期块调用 query_history_k_data_plus。
```

说明 Raw 仍先落地、Mapper 后置，流程保持：

```text
BaoStock -> Raw Parquet -> CanonicalMapper -> Curated Parquet -> Snapshot
```

同步标注分块配置只作用于定向证券路径，链接本功能设计文档。

- [ ] **Step 6: 运行端到端与回归测试**

```powershell
uv run pytest tests/integration/test_data_pipeline.py tests/integration/test_source_substitution.py tests/regression/test_data_snapshot_golden.py -v
```

Expected: PASS；Fake TuShare 的 Canonical/Snapshot 替换证明保持通过。

- [ ] **Step 7: 提交 Task 4**

```powershell
git add src/quant_core/data/sources/baostock.py src/quant_core/cli.py tests/integration/test_data_pipeline.py tests/regression/test_data_snapshot_golden.py docs/architecture/2026-07-30-personal-a-share-quant-platform-technical-design.md
git commit -m "feat: migrate BaoStock bootstrap to daily market API"
```

---

### Task 5: 全量门禁与真实环境验收说明

**Files:**
- Modify only if verification exposes a defect: files owned by Tasks 1–4
- Verify: entire repository

**Interfaces:**
- Consumes: 完成后的 v2 BaoStock 适配器和既有四阶段 Pipeline。
- Produces: 可审查的离线门禁证据；真实 20 年 bootstrap 的执行与指标采集命令。

- [ ] **Step 1: 运行全量自动门禁**

使用短 Windows 测试路径，避免长临时路径干扰 Raw lock 测试：

```powershell
New-Item -ItemType Directory -Force C:\tmp | Out-Null
uv run pytest -q --basetemp=C:\tmp\quant-baostock-v2
uv run ruff format --check .
uv run ruff check src tests
uv run mypy src
uv run alembic upgrade head
git diff --check
```

Expected: 所有命令退出码为 0。不得把 Windows Raw lock 的偶发失败简单重跑后忽略；若出现，按 `superpowers:systematic-debugging` 定位并增加回归测试。

- [ ] **Step 2: 检查 API 调用计数验收**

```powershell
uv run pytest tests/unit/data/sources/test_baostock.py tests/integration/test_baostock_raw_ingest.py -k "all_market or fetch_range" -v
```

Expected:

```text
query_daily_history_k_AStock 调用次数 == fixture 开市日数
query_history_k_data_plus 调用次数 == 0
query_trade_dates 调用次数 == 1（fetch_range 路径）
```

- [ ] **Step 3: 记录真实 20 年验收命令但不写入自动测试**

在用户指定、可写且不位于源码树的数据目录执行：

```powershell
$env:QUANT_DATA_ROOT='D:\quant-data'
uv run quant data bootstrap
```

验收记录必须包含：解析后的起止交易日、开市日数、全市场 API 调用数、Raw 总行数、重试次数、阶段耗时、总耗时和 Snapshot ID。目标总耗时为 60 分钟；网络与供应商性能导致超时不允许通过并发 BaoStock 全局 socket 绕过，应先基于日志定位瓶颈。

该命令会连接真实 BaoStock 并写入用户数据目录，因此实施者不得在未获得用户明确执行授权时运行。

- [ ] **Step 4: 独立代码审查**

审查重点：

- 全市场/定向路由没有交叉调用。
- 日历查询只发生一次。
- Raw 请求没有伪造 SDK 参数。
- 开市日空响应失败关闭。
- v2 fingerprint 阻止旧 checkpoint 复用。
- Canonical 与 Snapshot 业务语义没有漂移。

若发现问题，先增加失败测试，再修复并重新运行 Step 1。

- [ ] **Step 5: 提交验证过程中产生的修复**

若 Step 1–4 未产生代码变化，不创建空提交。若产生修复：

```powershell
git add src/quant_core/data/sources/baostock.py src/quant_core/cli.py tests/unit/data/sources/test_baostock.py tests/integration/test_baostock_raw_ingest.py tests/integration/test_raw_to_curated.py tests/integration/test_data_pipeline.py tests/regression/test_data_snapshot_golden.py docs/architecture/2026-07-30-personal-a-share-quant-platform-technical-design.md
git commit -m "fix: complete BaoStock daily market migration"
```

最终 `git status --short` 必须为空。
