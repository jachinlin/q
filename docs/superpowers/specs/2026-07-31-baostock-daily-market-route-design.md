# BaoStock 全市场日线混合路由设计

## 1. 背景与目标

当前 BaoStock 适配器通过 `query_history_k_data_plus` 按“证券 × 日期区间”采集日线。20 年全市场初始化需要大量逐证券请求，不符合低频个人量化平台的全市场采集特征。

BaoStock SDK 0.9.30 提供 `query_daily_history_k_AStock(date="")`：按指定日期返回当日全部 A 股日 K 线。该接口返回的 18 个字段与系统现有 `DAILY_BAR_FIELDS` 一致。本设计将全市场采集切换到该接口，同时保留指定证券查询能力。

目标：

- `instruments is None` 和空序列使用按交易日全市场接口。
- 非空 `instruments` 继续使用逐证券区间接口。
- 保持 Raw 供应商语义、Canonical schema、质量规则和上层管道接口不变。
- 通过适配器版本升级阻止旧 checkpoint 与新采集语义混用。
- 保持 20 年 bootstrap 的流式写入、幂等发布和 lease 续租能力。

官方接口说明：<https://baostock.com/mainContent?file=DailyUpdates.md#query_daily_history_k_AStock>

## 2. 已选方案

采用混合路由：

| 调用条件 | BaoStock API | 用途 |
|---|---|---|
| `instruments is None` | `query_daily_history_k_AStock(date)` | 全市场研究、bootstrap、常规 update |
| `instruments == []` 或 `()` | `query_daily_history_k_AStock(date)` | 与 `None` 相同，并记录空筛选条件审计日志 |
| `instruments` 非空 | `query_history_k_data_plus(code, fields, start_date, end_date, frequency="d", adjustflag="3")` | 少量指定证券采集 |

未选择的方案：

- “始终全市场查询后本地过滤”会让少量证券请求承担不必要的网络与 Raw 存储成本。
- “只保留全市场模式”会破坏已确认的 `SourceClient` 公共契约和定向补数能力。

## 3. 组件边界

### 3.1 Gateway

在 `BaoStockGateway` 与 `_BaoStockSdk` 增加：

```python
def query_daily_history_k_AStock(self, date: str = "") -> BaoStockCursor: ...
```

`BaoStockSdkGateway` 只做 SDK 调用转发，不解析、不映射、不写文件。真实 SDK 的该方法只有 `date` 参数，适配器不得伪造 `fields`、复权方式或证券筛选参数。

### 3.2 Source Client

`BaoStockClient.fetch_daily_bars(start, end, instruments)` 保持原签名，由内部路由分为：

- `_fetch_all_market_daily_bars(start, end)`：全市场按开市日采集。
- `_fetch_selected_daily_bars(start, end, instruments)`：沿用现有逐证券分块实现。

路由判断只存在于 Source Client。Pipeline、Raw store、Canonical Mapper、质量系统和 Snapshot Publisher 不感知具体 BaoStock API。

### 3.3 Calendar

全市场模式先通过 `query_trade_dates` 解析 `[start, end]` 内的开市日，仅对 `is_trading_day == "1"` 的日期调用日线接口。

`fetch_range` 已经需要发布 `trade_calendar` Raw。实现时应复用同一次日历查询结果，不能为了日线路由重复请求交易日历。直接单独调用 `fetch_daily_bars` 时允许自行查询一次日历。

## 4. 数据流

全市场路径：

```text
日期窗口
  -> BaoStock 交易日历
  -> 逐开市日 query_daily_history_k_AStock
  -> 每个交易日一个 RawBatch
  -> Raw Parquet + manifest
  -> BaoStockMapper
  -> 按 year 合并的 Curated daily_bar/security_status
  -> QualityRun
  -> Snapshot
```

指定证券路径保持：

```text
日期窗口 + InstrumentId[]
  -> 证券块 × 日期块
  -> query_history_k_data_plus
  -> RawBatch
  -> 后续公共链路
```

每个全市场 API 响应对应一个 `RawBatch`。不在 Source Client 中跨日期拼接，以便 Raw 请求、返回内容和供应商调用一一对应。约 20 年的 Raw 小文件只用于审计和重建，Dashboard 与研究查询读取按年合并的 Curated 分区。

## 5. Raw 契约

全市场日线 `RawBatch`：

- `provider="baostock"`
- `dataset="daily_bars"`
- `schema=DAILY_BAR_FIELDS`
- `rows` 保留 SDK 返回的字符串值和字段名
- `retrieved_at` 使用注入的 UTC clock

`request` 至少包含：

```json
{
  "api": "query_daily_history_k_AStock",
  "scope": "ALL",
  "date": "2025-12-31",
  "frequency": "d",
  "catalog_instrument_count": 5234,
  "catalog_instruments_sha256": "...",
  "response_instrument_count": 5088,
  "response_instruments_sha256": "..."
}
```

示例数量仅用于展示 JSON 类型，实际值必须由当次目录和响应计算：

- `catalog_*` 描述请求窗口内解析并冻结的完整历史 A 股目录，仅包含 BaoStock `type="1"` 且上市区间与请求窗口相交的证券，用于跨批次审计全量范围。
- `response_*` 描述该交易日实际返回的证券代码集合；停牌、上市和退市会导致不同日期的响应集合不同。
- `frequency="d"` 描述结果语义，不表示 SDK 接收了该参数。
- `request` 不记录虚构的 `adjustflag` 请求参数；适配器必须验证返回行的 `adjustflag` 与系统所需的不复权语义 `"3"` 一致。

所有全市场批次必须携带相同的 `catalog_instrument_count` 与 `catalog_instruments_sha256`。这取代旧设计中按证券块记录全量范围的方式。

## 6. Schema 与映射

全市场 API 的预期字段顺序为：

```text
date, code, open, high, low, close, preclose, volume, amount,
adjustflag, turn, tradestatus, pctChg, peTTM, pbMRQ, psTTM,
pcfNcfTTM, isST
```

它与现有 `DAILY_BAR_FIELDS` 相同，因此：

- `BaoStockMapper` 的 `daily_bar` 与 `security_status` 映射不变。
- Canonical schema、主键和按年分区规则不变。
- 供应商字段顺序或数量不匹配时，必须在 Raw 发布前失败。
- 响应中证券代码仍通过现有严格 `sh./sz.` 转换边界映射为内部 `InstrumentId`。

## 7. 错误处理与恢复

- SDK 非零错误码继续使用现有 `_retry` 策略和结构化 `QuantError`。
- 开市日返回零行视为供应商数据异常；按配置重试该日期，耗尽后以 FATAL 数据错误阻断阶段。
- 任一行列数、字段顺序、证券代码或 `adjustflag` 不合法时，不发布该 RawBatch。
- 周末和节假日不调用日线 API，也不生成空 RawBatch。
- Pipeline 的后台 `_StageLeaseKeeper` 覆盖单个日期请求长时间未返回的场景；owner 丢失后不能完成或失败提交旧 attempt。
- 已成功发布的 Raw 内容仍由内容哈希和 manifest 保证幂等。阶段级 checkpoint 语义保持不变；逐日期网络请求的细粒度断点续传不在本次范围。

## 8. 版本与兼容性

- `BAOSTOCK_SOURCE_ADAPTER_VERSION` 从 v1 升级到 v2。
- CLI 的 fetch-config fingerprint 删除全市场路径不再使用的证券/日期分块含义，或明确将这些参数标注为“仅指定证券路径”。
- `PipelineVersions.source_adapter` 和 `fetch_config` 变化会生成新的 request/run，不复用 v1 INGEST_RAW checkpoint。
- `SourceClient.fetch_daily_bars`、`RawBatch`、Canonical schema 和 Snapshot manifest 保持向后兼容。
- 中文技术设计中“全量模式按证券分块”的描述应同步改为本混合路由，并注明本设计为最新约束。

## 9. 可观测性

结构化日志和阶段指标至少记录：

- 路由：`ALL_MARKET_BY_DATE` 或 `SELECTED_INSTRUMENT_RANGE`
- 请求窗口与开市日数量
- 已完成日期数、当前日期、累计响应行数
- API 重试次数和错误码
- 每个日期请求耗时、Raw 发布耗时
- 全市场路径的日线 API 调用次数

全市场路径的关键验收指标是：

```text
query_daily_history_k_AStock 调用次数 == 窗口内开市日数
query_history_k_data_plus 调用次数 == 0
```

60 分钟 bootstrap 是运行目标而非单元测试中的硬编码时间断言。真实环境验收需要记录 20 年开市日数量、总行数、API 耗时分布和端到端耗时。

## 10. 测试与验收

### 10.1 单元测试

- Gateway 原样转发 `date`。
- `None` 和空序列选择全市场路由；空序列产生审计日志。
- 非空证券列表选择原逐证券路由。
- 全市场路由只遍历开市日，跳过休市日。
- 每个开市日生成一个 RawBatch，字段与请求元数据完整。
- 所有日期批次携带相同目录数量与 SHA-256。
- 字段漂移、列数错误、空开市日、非法代码、非 `"3"` adjustflag 和供应商错误均失败关闭。
- 重试只重试当前日期，不改变已返回批次内容。

### 10.2 集成与回归测试

- 全市场路径不调用 `query_history_k_data_plus`。
- 指定证券路径不调用 `query_daily_history_k_AStock`。
- `None` 与空序列产生相同 Raw/Canonical 业务内容。
- Raw → Canonical 的 `daily_bar` 和 `security_status` 与现有黄金语义一致。
- bootstrap、update、失败恢复、checkpoint 指纹和 Fake TuShare 替换测试继续通过。
- 用短 lease 和首次 yield 前阻塞证明后台续租期间不会出现第二采集者。
- 全套 pytest、Ruff、mypy、Alembic upgrade 和 `git diff --check` 通过。

## 11. 非目标

- 不在本次实现 BaoStock 财务数据或公司行为采集。
- 不改变 Canonical 因子、股票池、回测和 Dashboard 接口。
- 不并发调用 BaoStock 全局 socket SDK；除非后续官方明确保证会话线程安全。
- 不实现逐交易日持久化的阶段子 checkpoint。
- 不改变 TuShare 适配器契约。
