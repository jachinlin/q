# A 股量化策略研究工作台实现说明

文档状态：实现规范，待用户书面审阅　·　日期：2026-08-20

本文是各层**实现级细化**的单文件合并版：深到目录布局、字段映射、类型转换、PIT 计算公式、聚合/去重算法、质量阈值、SQLite DDL、关键伪代码。读者定位：**低级程序员照此即可实现**。设计“为什么”见同目录[总体设计](design.md)与[策略、回测与实验设计](strategy-backtest-experiment-design.md)；本文回答“怎么做”。凡“必须/禁止”为硬约束；伪代码为参考实现，可在不改变输入输出契约与不变量的前提下调整。所有货币金额以**整数分（fen）**表示。文中形如 `第 N 章 §M` 指向本文对应章节，带文档链接的章节号指向对应设计文档。

## 目录

1. 数据层实现
2. 因子层实现
3. 回测引擎实现
4. 策略层实现
5. 实验层实现
6. Worker 与任务队列实现

---

## 1. 数据层实现

### 1.1 目录与文件布局

数据根 `QUANT_DATA_ROOT`（默认 `~/.q-data`，必须在源码树外）。布局：

```text
$QUANT_DATA_ROOT/
├── raw/
│   └── source=<source>/endpoint=<endpoint>/<request_hash>/
│         ├── <content_hash>.parquet      # Raw 内容（供应商原样，全列 String）
│         └── manifest.json               # 该 request 的当前头 + 历史文件列表
├── canonical/
│   └── dataset=<dataset>/<partition_key>/
│         └── <content_hash>.parquet      # 规范化后的 Canonical 分区
└── quant.db                              # SQLite 元数据（唯一权威元数据源）
```

- `<source>`：如 `baostock`。`<endpoint>`：供应商原生接口名，如 `query_history_k_data_plus`。
- `<request_hash>` / `<content_hash>`：见 §1.2。
- `<partition_key>`：`year=2023` / `report_year=2023` / `all`（见 §1.5.1）。
- **不可变**：`raw/` 与 `canonical/` 下的 `<hash>.parquet` 写入后永不就地修改；变化产生新文件。
- 失去引用的旧 Canonical 文件在后续 curate 末尾清理（Windows 占用失败则下次再清）。

---

### 1.2 内容寻址与哈希

三个哈希，全部 SHA-256 小写十六进制，**不含任何物理路径**：

| 名称 | 输入 | 用途 |
|---|---|---|
| `request_hash` | `SHA256(canonical_json(request))` | Raw 请求身份、目录名 |
| `content_hash` | `SHA256(确定性 Arrow IPC 字节)` | Raw/Canonical 内容身份、文件名、完整性校验 |
| `schema_fingerprint` | `SHA256(规范化 Arrow schema 序列化字节)` | 端点契约身份，独立于数据值 |

#### 1.2.1 canonical_json（确定性 JSON）

```python
def canonical_json(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")
```

要求：键排序、无多余空格、UTF-8、拒绝 NaN/Inf。request 中所有值必须是 JSON 标量或其列表/字典。

#### 1.2.2 content_hash（Arrow IPC）

```python
def content_hash(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
```

写入前 `table = table.combine_chunks()` 保证分块一致；空值缓冲区规范化：对每列按 null bitmap
把 null 位置的物理值填成该类型固定占位（String→"", Float→0.0, Int→0, Bool→False, Date→1970-01-01,
Timestamp→1970-01-01T00:00Z），保留原 null bitmap——避免 Parquet 重写不可见字节导致哈希漂移。

---

### 1.3 Raw 层实现

#### 1.3.1 request 规范形态

每个供应商调用对应一个 request 字典。**必须**包含 `endpoint` 以外的全部业务参数，且
参数值只用 JSON 标量。示例（日线全市场按日）：

```json
{"code": "", "start_date": "2023-06-01", "end_date": "2023-06-01",
 "frequency": "d", "adjustflag": "3", "fields": "date,code,open,high,low,close,preclose,volume,amount,pctChg,tradestatus,isST,peTTM,pbMRQ,psTTM,turn"}
```

`request_hash = SHA256(canonical_json(request))`。`fields` 顺序固定写死在采集代码常量里，
保证跨机一致。

#### 1.3.2 RawBatch → Raw 文件（LOCALIZE）

`SourceClient.fetch(request)` 返回 `RawBatch(source, endpoint, request, retrieved_at, schema, rows)`：

- `schema`：供应商返回的列名元组，顺序固定。
- `rows`：每行是 `{列名: 原始字符串}`。**Raw 全列 String，不做任何类型转换**（保真）。
- `retrieved_at`：tz-aware UTC。

写入算法（幂等 + 去重）：

```text
def localize_one(request):
    rh = request_hash(request)
    dir = raw/source=.../endpoint=.../{rh}/
    # 断点续抓：SQLite 有该 request 的当前头且 schema_fingerprint 与当前端点契约一致 → 跳过网络
    head = sqlite.get_raw_request(source, endpoint, rh)
    if head and head.schema_fingerprint == current_endpoint_fingerprint:
        return REUSED
    batch = source.fetch(request)                 # 网络
    table = arrow_table_all_string(batch)         # 全 String
    ch = content_hash(table); sf = schema_fingerprint(table.schema)
    # 空响应特例（财务接口）：empty_discarded，不写文件不写库
    if table.num_rows == 0 and endpoint in EMPTY_DISCARD_ENDPOINTS:
        return EMPTY_DISCARDED
    write_parquet_atomic(dir/{ch}.parquet, table)  # 临时名 + rename
    write_manifest(dir/manifest.json, ...)         # §1.3.3
    sqlite.upsert_raw_request(source, endpoint, rh, request_json, current_content_hash=ch, updated_at=now)
    sqlite.insert_raw_object(source, endpoint, rh, ch, data_path, manifest_path, sf, row_count, retrieved_at, created_at=now)
    return FETCHED
```

原子写：先写 `.<uuid>.tmp` → `content_hash` 校验 → `os.replace` 到 `<ch>.parquet`。

#### 1.3.3 Raw manifest.json

```json
{"source":"baostock","endpoint":"query_history_k_data_plus","request_hash":"<rh>",
 "request":{...},"current_content_hash":"<ch>",
 "files":[{"content_hash":"<ch>","retrieved_at":"2023-06-01T09:30:00+00:00",
           "ingest_date":"2023-06-01","schema_fingerprint":"<sf>","row_count":5405}]}
```

`files` 保留历史内容（同 request 的供应商重述形成多条），`current_content_hash` 指向最新。
`query_stock_basic` 这类会频繁重述的端点，`files` 保留上限 20 条（超出丢最旧）。

#### 1.3.4 SQLite Raw 表（DDL）

```sql
CREATE TABLE raw_request (
  source TEXT NOT NULL, endpoint TEXT NOT NULL, request_hash TEXT NOT NULL,
  request_json TEXT NOT NULL, current_content_hash TEXT NOT NULL, updated_at TEXT NOT NULL,
  PRIMARY KEY (source, endpoint, request_hash));
CREATE TABLE raw_object (
  source TEXT NOT NULL, endpoint TEXT NOT NULL, request_hash TEXT NOT NULL, content_hash TEXT NOT NULL,
  data_path TEXT NOT NULL, manifest_path TEXT NOT NULL, schema_fingerprint TEXT NOT NULL,
  row_count INTEGER NOT NULL CHECK(row_count >= 0),
  retrieved_at TEXT NOT NULL, created_at TEXT NOT NULL,
  PRIMARY KEY (source, endpoint, request_hash, content_hash),
  FOREIGN KEY (source, endpoint, request_hash) REFERENCES raw_request ON DELETE RESTRICT);
```

---

### 1.4 采集单元与 request 生成（BaoStock）

按数据集列出：抓取粒度、request 生成规则、供应商端点。**日期均为 `YYYY-MM-DD`**。

| Canonical 数据集 | 端点 | 抓取粒度 | request 生成 |
|---|---|---|---|
| daily_bar / daily_basic / security_status（三者 fan-out 自同一日线） | `query_history_k_data_plus` | 每个开市日一条（`code=""` 全市场） | 对窗口内每个开市日生成 `{code:"", start_date:d, end_date:d, frequency:"d", adjustflag:"3", fields:...}` |
| daily_bar（ETF 补充，全 A 接口不含 ETF） | `query_history_k_data_plus` | 每只 ETF 按区间 | 白名单 ETF 每只 `{code:etf, start_date, end_date, frequency:"d", adjustflag:"3", fields:...}` |
| index_bar | `query_history_k_data_plus` | 每个指数按区间 | 每指数 `{code:idx, start_date, end_date, frequency:"d", fields:...}` |
| trade_calendar | `query_trade_dates` | 整区间一条 | `{start_date, end_date}`；覆盖末日向后延 90 个自然日供调度，最近 30 个自然日作为日历修订回看窗口 |
| instrument | `query_stock_basic` | 全市场一条 | `{code:""}`；固定为 `SNAPSHOT_REFRESH`，不读取 Canonical 水位或重叠窗口 |
| financial_observation | `query_dupont_data` | 每 (证券, 年, 季) 一条 | 自动更新不使用 Canonical 水位；以最近已结束报告期为候选，严格越过其披露截止日后才生成 `{code, year, quarter}`。bootstrap 补齐范围内全部已到期报告期 |
| industry_classification | `query_stock_industry` | 每个已完整结束开市日一条（全市场） | `{code:"", date:d}`；仅当 `d` 已完整结束（见 §1.6.4）才进 curate |
| corporate_action | `query_dividend_data` | 每 (证券, 年) 一条 | 每只证券每年 `{code, year, yearType:"report"}` |

`adjustflag="3"` = 不复权（Canonical 存未复权，复权在读取侧算）。

财务保守披露截止日（`financial_disclosure_deadline`，用于决定 `(year, quarter)` 是否可抓）：

```text
Q1 → 当年 4-30    Q2 → 当年 8-31    Q3 → 当年 10-31    Q4 → 次年 4-30
report_period_end: Q1→3-31, Q2→6-30, Q3→9-30, Q4→12-31
自动更新仅当 planning_date > disclosure_deadline(year, quarter) 时生成该 request；截止日当天不抓取。
```

自动更新不读取 `financial_observation` 的 Canonical 最大报告期作为水位，也不应用通用
`overlap_days`。计划器先选择最近已结束报告期，再判断计划日是否严格越过该报告期的披露截止日；
尚未越过时返回 `DISCLOSURE_DEADLINE_PENDING` 跳过证据，不展开财务请求。若多个报告期共享该截止日，
必须一起刷新，例如 4 月 30 日对应上一年度 Q4 和本年度 Q1。重复运行同一批次由 Raw request 身份
幂等复用，不重新扫描全部历史季度。Dashboard 使用后端返回的 `trigger_date` 展示“无需更新/待更新”，
财务数据的水位和重叠天数统一显示为“不适用”。

`trade_calendar` 的 `canonical_dataset.end_date` 是未来日历覆盖末日，不是市场数据当前水位；
`dataset_operational_state.localized_through` 才表示最近一次自动计划已检查到的业务日。Dashboard 数据资产
表分别显示“日历覆盖至”和“已检查至”，计划与运行详情将 `overlap_days=30` 显示为“修订回看 30 天”，
不得将覆盖末日和回看窗口解释为行情更新水位或滞后。

---

### 1.5 Curate：标准化与聚合

CURATE 把 Raw（全 String）转成 Canonical（强类型 + PIT + 审计列），按分区原子发布。

#### 1.5.1 分区规则

| 数据集 | 分区 | partition_key | 分区列 |
|---|---|---|---|
| daily_bar/daily_basic/security_status/index_bar/industry_classification | 按年 | `year=<yyyy>` | trade_date（industry 用 as_of_date）的年 |
| financial_observation | 按报告年 | `report_year=<yyyy>` | report_period 的年 |
| corporate_action | 按年 | `year=<yyyy>` | ex_date 的年 |
| instrument / trade_calendar | 单分区 | `all` | 无 |

#### 1.5.2 标准化步骤（单条 Raw → Canonical 行）

对每个 Raw 分区（endpoint 的一批当前头），mapper 执行：

1. **列改名**：供应商列 → Canonical 列（映射表见 §1.5.4）。
2. **类型转换**（String → 目标类型），规则：
   - 空串 `""` → null。
   - 价格/金额/比率 → Float64（`float(s)`）。
   - 成交量 → Int64（`int(float(s))`，BaoStock volume 可能带小数点）。
   - 日期 → Date（`date.fromisoformat`）。
   - 布尔类：BaoStock `tradestatus`（"1"=正常交易,"0"=停牌）→ `is_suspended = (s == "0")`；
     `isST`（"1"=ST）→ `is_st = (s == "1")`。
3. **派生审计列**（见 §1.5.5）：`source / available_at / availability_source / pit_usable / ingested_at`。
4. **按主键排序**。

#### 1.5.3 聚合与去重（多 Raw 覆盖同一主键）

多个 Raw 当前头可能覆盖同一 Canonical 主键（如区间 ETF 抓取与逐日抓取重叠）。规则：

- **跨 Raw 响应**：按 `(retrieved_at DESC, endpoint, request_hash)` 确定性排序，**保留最新响应**的行
  （`unique(subset=primary_key, keep="first")` after that sort）。
- **单个 Raw 响应内部**主键重复 = 数据错误，直接 FATAL（`primary_key_duplicate`）。
- **fan-out**：一次日线 mapper 同时产出 daily_bar/daily_basic/security_status 三个数据集的行，
  只读一次 Raw、跑一次转换。

去重伪代码：

```text
def curate_partition(dataset, raw_heads):     # raw_heads 已按 retrieved_at DESC 排好
    frames = [normalize(head) for head in raw_heads]     # 各自 String→typed + 审计列
    combined = concat(frames)                            # 保持 raw_heads 顺序
    # 单响应内部重复检查
    for f in frames:
        if f.duplicated(primary_key).any(): raise FATAL("primary_key_duplicate")
    # 跨响应保留最新
    result = combined.unique(subset=primary_key, keep="first", maintain_order=True)
    result = result.sort(sort_key).cast(canonical_schema)
    return result
```

#### 1.5.4 字段映射表（BaoStock → Canonical）

**daily_bar**（endpoint `query_history_k_data_plus`）：
```text
code→instrument_id  date→trade_date  open→open  high→high  low→low  close→close
preclose→preclose  volume→volume  amount→amount  pctChg→pct_change
```
**daily_basic**（同一 Raw 行）：`code→instrument_id date→trade_date peTTM→pe_ttm pbMRQ→pb_mrq psTTM→ps_ttm turn→turnover`。
**security_status**（同一 Raw 行）：`code→instrument_id date→trade_date tradestatus→is_suspended(派生) isST→is_st(派生)`；
`is_listed` 由 instrument 生命周期推导（trade_date ∈ [list_date, delist_date)）；`board` 从 instrument 关联；
`price_limit_rule_id` 由板块+日期按规则表推导。
**instrument**（`query_stock_basic`）：`code→instrument_id code_name→name ipoDate→list_date outDate→delist_date type→instrument_type status→listing_status`；`exchange`/`board` 从 `code` 后缀与代码段推导。
`list_date/delist_date` 只描述证券生命周期，不参与更新水位。Canonical 数据集覆盖日使用 Raw
`ingested_at` 对应的上海快照日；Dashboard 显示“全量快照/最近刷新”，当前水位和重叠天数均为
“不适用”。
**trade_calendar**（`query_trade_dates`）：`calendar_date→trade_date is_trading_day→is_trading_day`。
**financial_observation**（`query_dupont_data`，每行含多个 dupont 指标，需 **unpivot** 成长表）：
```text
每个 Raw 行 → 8 条 Canonical 行（metric ∈ {dupont_roe, dupont_assets_to_equity,
dupont_asset_turn, dupont_pnitoni, dupont_nitogr, dupont_tax_burden,
dupont_interest_burden, dupont_ebit_to_gr}），value=对应列，
report_period=report_period_end(year,quarter)，announced_at 见 §1.5.5。
revision 在跨历史 Raw 合并时生成（§1.7）。
```
**corporate_action**（`query_dividend_data`）：`code→instrument_id dividOperateDate→ex_date`；
`action_type` 由分红字段判定（现金→CASH_DIVIDEND，送转→STOCK_DIVIDEND/SPLIT）；
`cash_per_share`=每股税前现金红利（分），`share_ratio`=每股送转比例；`announced_at` 见 §1.5.5。
**industry_classification**（`query_stock_industry`）：`code→instrument_id industry→industry_code/industry_name`；
`as_of_date`=请求 `date`；`taxonomy`="证监会行业分类"；`is_classified`=(industry 非空)。

#### 1.5.5 `available_at` 与审计列（PIT 核心）

每个数据集的 `available_at` 计算规则（上海时区日终 = 当日 23:59:59+08:00 转 UTC）：

| 数据集 | available_at | availability_source | pit_usable |
|---|---|---|---|
| daily_bar/daily_basic/security_status/index_bar | trade_date 的上海日终 | `MARKET_CLOSE` | true |
| trade_calendar | trade_date 的上海日终 | `MARKET_CLOSE` | true |
| instrument | ingested_at（主数据无公告时点） | `INGESTION` | true |
| financial_observation | `max(供应商 statDate/公告时间, 保守披露截止日)` 的上海日终 | `DISCLOSURE_DEADLINE`（缺公告时）或 `ANNOUNCEMENT` | 有有效 announced_at 才 true |
| industry_classification | as_of_date 的上海日终 | `BAOSTOCK_AS_OF_DATE` | true |
| corporate_action | ex_date 的上海日终（保守：除权日才计入账务）；`announced_at` 单独存供更细 PIT | `EX_DATE` | true |

```python
def shanghai_day_end_utc(d: date) -> datetime:
    return datetime.combine(d, time.max, tzinfo=ZoneInfo("Asia/Shanghai")).astimezone(UTC)
```

`ingested_at` = curate 运行时刻（UTC），仅血缘。`source` = "baostock"。

#### 1.5.6 Canonical 写入与原子发布

```text
def publish_partition(dataset, key, frame):
    table = frame.to_arrow()
    ch = content_hash(table); sf = schema_fingerprint(table.schema)
    path = canonical/dataset={dataset}/{key}/{ch}.parquet
    if not path.exists():
        write tmp → verify round-trip equals frame → os.replace → path
    return spec(partition_key=key, content_hash=ch, path=path, schema_fingerprint=sf, row_count=table.num_rows)

# 一个数据集所有目标分区在【单个 SQLite 事务】内切换当前指针：
def commit_dataset(dataset, specs, expected_raw_heads):
    with sqlite.transaction():
        assert current raw heads == expected_raw_heads else raise RETRYABLE(DATA_CURATE_INPUT_CHANGED)
        replace canonical_dataset + canonical_partition rows
    cleanup_unreferenced_files()
```

#### 1.5.7 增量：只重建输入变化的分区

```text
input_hash(partition) = SHA256(canonical_json({
   dataset, partition_key, transform_hash,
   inputs: [sorted by (source,endpoint,request_hash) of
            {source,endpoint,request_hash,content_hash,schema_fingerprint,retrieved_at}]}))
transform_hash = SHA256(mapper 源码 + 分区规则 + 发布规则 + Canonical schema)
```

curate 时：分区 `old.input_hash == new.input_hash` 且当前文件存在 → 复用（不读 Raw）；否则整分区重建。
不存在强制重抓或强制重建参数。**注意（财务）**：Q4 年报次年才披露，落在 `report_year=<上一年>` 分区，
故 `curate financial --from/--to` 用**披露年**映射分区，或直接用 `curate-all` 避免漏跨年重述。

#### 1.5.8 SQLite Canonical 表（DDL）

```sql
CREATE TABLE canonical_dataset (
  dataset TEXT PRIMARY KEY, content_hash TEXT NOT NULL, source TEXT NOT NULL,
  start_date TEXT, end_date TEXT, updated_at TEXT NOT NULL);
CREATE TABLE canonical_partition (
  dataset TEXT NOT NULL, partition_key TEXT NOT NULL, ordinal INTEGER NOT NULL,
  content_hash TEXT NOT NULL, path TEXT NOT NULL, schema_fingerprint TEXT NOT NULL,
  input_hash TEXT NOT NULL, row_count INTEGER NOT NULL,
  PRIMARY KEY (dataset, partition_key),
  FOREIGN KEY (dataset) REFERENCES canonical_dataset ON DELETE CASCADE);
CREATE TABLE data_catalog_state (
  id INTEGER PRIMARY KEY CHECK(id=1), catalog_hash TEXT NOT NULL,
  quality_gate_open INTEGER NOT NULL DEFAULT 0, validated_at TEXT);
```

`catalog_hash` = `SHA256(canonical_json(按 dataset 排序的 {dataset: content_hash}))`；
任一数据集 content_hash 变即变。仅供运行内一致性（非复现回放）。

---

### 1.6 特殊数据集处理

#### 1.6.1 财务 revision（跨历史 Raw 合并）

同一 (instrument, report_period, metric) 的供应商重述形成多个 Raw 当前头历史。curate 财务分区时：
读取该端点全部 schema 兼容历史 Raw 对象 → 按 `retrieved_at` 升序 → 完全相同的连续观测合并；
`value/announced_at/available_at/pit_usable` 任一变化则按抓取顺序追加新 `revision`（从 0 起递增）。

```text
def financial_revisions(rows):  # rows: 同 (instrument,report_period,metric) 的历史观测
    rows.sort(by=retrieved_at)
    out, last_state, rev = [], None, -1
    for r in rows:
        state = (r.value, r.announced_at, r.available_at, r.pit_usable, r.source, r.availability_source)
        if state != last_state:
            rev += 1; out.append({**r, "revision": rev}); last_state = state
    return out
```

读取侧 `financials_as_of(as_of)`：过滤 `available_at <= as_of日终 & pit_usable`，
每业务键取"当时已知的最新 revision"（ROW_NUMBER OVER PARTITION BY key ORDER BY available_at DESC, revision DESC）。

#### 1.6.2 复权因子推导（前复权）

由 `corporate_action` 推导每证券的前复权因子序列，供读取侧 `adjusted_bars`/`log_returns`：

```text
# 后复权累乘因子 → 归一化为前复权（最新日=1.0）
对每证券按 ex_date 升序：
  除权日调整比 factor_d = (preclose_prev) / (preclose_prev - cash_per_share + ...) 的复权系数
  （标准 A 股前复权：adj_close[t] = close[t] * Π(未来所有除权日的复权比)）
前复权价 = 未复权价 × 前复权因子；前复权因子(最新交易日)=1。
```

实现推荐直接用 BaoStock `adjustflag="1"`（前复权）交叉验证，但 Canonical 只存未复权，
复权因子由 corporate_action 事件在读取侧计算并可与供应商前复权价做一致性测试（字面量 oracle）。

#### 1.6.3 ETF 与指数

全 A 日线接口不含 ETF：白名单 ETF 走独立区间请求，curate 时并入 daily_bar / security_status。
指数只进 index_bar，不进 daily_bar。基准优先国证A指。

#### 1.6.4 行业"已完整结束"判定

```text
def industry_raw_complete(as_of_date, now):
    local = now.astimezone(Shanghai); latest = local.date()
    if local.hour < 18: latest -= 1 day     # 当日 18 点前视为未完整
    return as_of_date <= latest
```

只有完整结束日的行业 Raw 才进 curate 候选与 input_hash。行业事件压缩：每年首个完整快照为基线，
后续只存新增证券/行业变化/分类状态变化；显式空行业生成 `is_classified=false` tombstone。

---

### 1.7 VALIDATE：质量规则（确切阈值与严重级）

`validate <dataset>` 只诊断；`validate-all` 通过才开研究门。严重/致命关门。

| rule_id | 严重级 | 触发条件（actual vs threshold） |
|---|---|---|
| required_dataset_missing | FATAL | 必需数据集无当前指针 |
| required_dataset_empty | FATAL | 必需数据集行数=0 |
| trading_window_empty | FATAL | trade_calendar 无 is_trading_day=true 的行 |
| canonical_schema | FATAL | 分区 schema ≠ 声明 schema（列名/顺序/类型），不匹配数 > 0 |
| cross_partition_schema | FATAL | 同数据集跨分区 schema 不一致数 > 0 |
| primary_key_duplicate | FATAL | 主键重复组数 > 0 |
| required_value_null | SEVERE | 必填列 null 数 > 0（例外：`daily_basic.turnover` 在 `security_status.is_suspended=true` 的停牌日允许 null） |
| ohlc_relationship | SEVERE | 交易日行 `high < max(open,low,close)` 或 `low > min(open,high,close)` 数 > 0（仅对 volume/amount 非空的交易行） |
| negative_volume | SEVERE | 交易行 volume < 0 数 > 0 |
| trading_day_coverage | SEVERE | 日线最新日之内、交易日历开市日缺失数 > 0（未来 90 日前瞻不要求有日线） |
| instrument_coverage | SEVERE | 日线引用了 instrument 主数据不存在的证券数 > 0 |
| financial_availability | SEVERE | `pit_usable=true` 但 announced_at/available_at 为空，或 announced_at > available_at，数 > 0 |
| industry_state | FATAL | 行业事件 state 非法（classified 与 code 不一致）、available_at ≠ as_of_date 上海日终、availability_source 错、supplier_update_date > as_of_date，数 > 0 |
| corporate_action_consistency | SEVERE | 除权日 preclose 跳变与事件不符（|实际跳变 − 事件推导跳变| > 阈值 1e-4）数 > 0 |

必填列（`required_value_null` 检查范围）：
```text
daily_bar: instrument_id,trade_date,open,high,low,close,volume,amount + 审计列(除 available_at 外均非空)
trade_calendar: trade_date,is_trading_day
security_status: instrument_id,trade_date,is_listed,is_suspended
financial_observation: instrument_id,report_period,metric,revision
industry_classification: as_of_date,supplier_update_date,instrument_id,taxonomy,is_classified
index_bar: index_id,trade_date,close
```

规则结果记 `quality_run` / `quality_issue` / `quality_rule_result`（PASS/FAIL/SKIPPED）。
只有全部 SEVERE/FATAL 为 0 时 `data_catalog_state.quality_gate_open=1`。

---

### 1.8 研究读取（PIT 物理截断）

`CanonicalResearchRepository` 是上层唯一入口。实现要点：

1. 每次读取先检查 `quality_gate_open=1`，否则抛 `DATA_QUALITY_GATE_CLOSED`。
2. 校验分区文件：路径在可信根内、content_hash/schema_fingerprint/row_count 与 SQLite 记录一致。
3. 用 DuckDB/Polars 扫描分区 Parquet，**谓词物理下推**截断到 PIT：

```sql
-- financials_as_of(as_of) 核心查询
SELECT <cols> FROM (
  SELECT <cols>, ROW_NUMBER() OVER (PARTITION BY instrument_id, report_period, metric
         ORDER BY available_at DESC, revision DESC) AS rk
  FROM data
  WHERE metric IN (?) AND pit_usable=TRUE AND available_at IS NOT NULL
        AND available_at <= ?    -- shanghai_day_end_utc(as_of)
) WHERE rk=1 ORDER BY instrument_id, report_period, metric
```

- `bars(end)` / `daily_basics(end)`：`pit_usable & available_at<=shanghai_day_end(end) & trade_date∈[start,end]`。
- 所有接口 `end`/`as_of` 即 PIT 截止，**不接受独立未来 as_of**。
- `catalog_hash()` 返回当前版本供实验层运行内一致性。

---

### 1.9 测试清单（TDD，先写后实现）

- **哈希确定性**：同输入跨进程同 content_hash；null 缓冲区规范化后 Parquet 往返哈希不变。
- **PIT**：财务修订不早于 available_at 可见；`financials_as_of` 取当时最新 revision；行业事件从首现 as_of_date 可见。
- **类型转换**：空串→null、volume 带小数、is_suspended/is_st 派生 各字面量 oracle。
- **去重优先级**：跨 Raw 重叠主键保留 retrieved_at 最新；单响应内重复 → FATAL。
- **复权**：corporate_action 推导前复权因子与 BaoStock adjustflag=1 一致（样本 oracle）。
- **质量规则**：每条规则正例（触发）+ 负例（合法异常不误报，尤其停牌 turnover）。
- **原子性/增量**：input_hash 不变则复用不读 Raw；事务中 raw head 变则 DATA_CURATE_INPUT_CHANGED。
- **隔离**：模拟 TuShare mapper 产出同 Canonical schema。

---

### 1.10 关键不变量汇总（实现自检）

1. Raw 全 String、内容寻址、不可变；SQLite 是元数据唯一权威。
2. Canonical 强类型 + 五审计列；`available_at` 按 §1.5.5 精确计算。
3. 跨 Raw 主键冲突按 retrieved_at 保最新；单响应内重复即 FATAL。
4. 分区在单事务内切指针；input_hash 驱动增量。
5. 质量门未开则 Repository 拒绝读取；PIT 截断物理下推。
6. 撮合用未复权价，复权因子由 corporate_action 推导供因子/信号侧。
7. 一切金额整数分；一切日期上海时区口径明确。

---

## 2. 因子层实现

### 2.1 因子输出契约

所有因子输出**精确**此 schema（列名、顺序、类型一字不差）：

```text
trade_date:    Date
instrument_id: String
factor_id:     String
value:         Float64        # 无效时为 null
available_at:  Datetime(us, UTC)
is_valid:      Boolean
```

不变量（`validate_factor_output` 校验，任何因子 compute 返回后强制）：

1. `trade_date, instrument_id, factor_id, is_valid` 非空。
2. `is_valid=true` ⇒ `value` 有限（非 null/NaN/Inf）且 `available_at` 非空。
3. `is_valid=false` ⇒ `value` 允许 null；若 `value` 非空则 `available_at` 必须非空。
4. `available_at` 上海时区日期 ≤ `trade_date`（PIT：不得用未来信息）。
5. `trade_date ∈ [ctx.start, ctx.end]`。
6. `(trade_date, instrument_id, factor_id)` 无重复。
7. `factor_id` 全表等于 `spec.factor_id`。

```python
def validate_factor_output(frame, factor_id, start, end):
    assert non_null(frame, ["trade_date","instrument_id","factor_id","is_valid"])
    assert not (frame.is_valid & (frame.value.is_null()|frame.value.is_nan()|frame.value.is_infinite())).any()
    assert not (frame.is_valid & frame.available_at.is_null()).any()
    assert not (~frame.is_valid & frame.value.is_not_null() & frame.available_at.is_null()).any()
    assert (frame.available_at.dt.convert_time_zone("Asia/Shanghai").dt.date() <= frame.trade_date).all()
    assert frame.trade_date.is_between(start, end).all()
    assert not frame.select(struct("trade_date","instrument_id","factor_id").is_duplicated()).any()
    assert (frame.factor_id == factor_id).all()
```

`is_available_on_signal_day(ts, day)`：`ts` tz-aware 且 `ts.astimezone(Shanghai).date() <= day`。

---

### 2.2 FactorSpec 与注册/引擎

```python
@dataclass(frozen=True, slots=True)
class FactorSpec:
    factor_id: str                    # 正则 [A-Za-z0-9][A-Za-z0-9_.-]*，无 '@'
    frequency: str                    # "daily"
    lookback_sessions: int            # >=0，计算所需历史会话数
    dependencies: tuple[str, ...]     # 依赖的其他 factor_id（去重、无环）
    direction: int                    # +1 越大越优 / -1 越小越优
    parameters: Mapping[str, JsonValue]   # 冻结、canonical_json 可序列化
    data_dependencies: tuple[DatasetKind, ...]  # 声明用到的 Canonical 数据集
```

- `FactorRegistry.register(factor)`：校验 spec、`factor_id` 唯一。
- `topological_order(refs)`：DFS，检测环（`dependency cycle`），依赖先于消费者、字典序稳定。
- `FactorEngine.compute(factor_ids, ctx)`：拓扑序逐因子 `compute(ctx)` → 校验 schema → `validate_factor_output`
  → sort(`trade_date,instrument_id,factor_id`) → 收集。**每次运行重算，无跨运行缓存**；运行内可共享
  行情读取缓存（`MarketBarsCache`/`DailyBasicsCache`）。
- preflight：因子 `data_dependencies` 不在当前 provider 能力内 → `FACTOR_CAPABILITY_UNAVAILABLE`。

---

### 2.3 内置因子（逐个精确口径）

窗口统一按**交易所会话**计数（非自然日）。停牌占位行（volume 与 amount 均 null）记零收益；
某会话完全缺 Canonical 行情 → 该会话收益 null 并使覆盖它的窗口失效；历史不足直接无效，
禁止回退不完整窗口。前复权收益来自 `repository.log_returns`（见 §1.6.2）。

#### 2.3.1 估值倒数类（earnings_yield_ttm / book_to_price_mrq）

```text
earnings_yield_ttm = 1 / pe_ttm   （pe_ttm 有限且 ≠0 时 valid；负 PE 保留负倒数）
book_to_price_mrq  = 1 / pb_mrq   （同上）
available_at = daily_basic 行的 available_at；valid 还需 available_at 上海日期 ≤ trade_date
```

向量化实现（无行循环）：

```python
valid = col(field).is_not_null() & col(field).is_finite() & (col(field)!=0.0) \
        & col("available_at").is_not_null() \
        & (col("available_at").dt.convert_time_zone("Asia/Shanghai").dt.date() <= col("trade_date"))
value = when(valid).then(1.0/col(field)).otherwise(None)
```

#### 2.3.2 roe_pit（PIT 财务，190 日计龄）

口径：每个信号日取"当时已知的最新报告期的最新 revision"的 `dupont_roe`；从该 active 记录的
`available_at` 上海日期起 **190 个自然日**内有效；最新报告值非有限则不回退旧报告（直接无效）。

```text
history = repository.financial_history(["dupont_roe"], as_of=ctx.end, instruments)
# 事件化：按 available_at 升序，取每个 available_at 时点已知的"最新 report_period 的最新 revision"
transitions = build_transitions(history)   # (instrument, event_at, active_value, active_available_at)
# 与信号日网格做 join_asof(backward, by=instrument, left_on=signal_at, right_on=event_at)
age_days = (trade_date - active_available_at.shanghai_date).days
valid = active_value.is_finite() & age_days.between(0, 190)
```

`build_transitions`：对每 instrument 按 `available_at` 分组，同一 available_at 内取 `report_period` 最大者
的最新 revision；相邻状态相同则不产生新 transition。

#### 2.3.3 动量 momentum_120_20（跳过近 20 日）

```text
required_prices = 121   （需要 t-120..t 的 121 个会话收益完整）
value = expm1( Σ 前复权对数收益[t-120 .. t-20] )   # 即跳过最近 20 会话
窗口内任一会话收益缺失(真实缺行) → 无效；停牌零收益计入求和
```

向量化：对 `log_return` 先 `shift(20)` 再 `rolling_sum(100, min_samples=100).over(instrument_id)`，
配合"窗口内非空计数==100"门控，最后 `expm1`。

#### 2.3.4 波动率 volatility_60d / downside_volatility_60d

```text
volatility_60d          = std(log_return[t-59..t], ddof=1) * sqrt(252)      # 60 个会话收益
downside_volatility_60d = sqrt( mean( min(log_return,0)^2 )[t-59..t] ) * sqrt(252)
required_prices = 61（60 个收益需 61 个价格点）；窗口内任一真实缺行 → 无效
direction = -1
```

向量化：`rolling_std(60, ddof=1)` / `(min(r,0)^2).rolling_mean(60)` over instrument，`* sqrt(252)`，
配"窗口内有限收益计数==60"门控。

#### 2.3.5 max_drawdown_120d

```text
对前复权对数价格路径 P[t-119..t]（119 个收益构成 120 点，取相对路径，首点=0 累加对数收益）：
running_peak = 累计最大；drawdown = max( 1 - exp(P - running_peak) )
direction = -1；required_prices=120
```

NumPy 批量实现（`sliding_window_view` + `cumsum` + `maximum.accumulate`），禁止逐信号 Python 切片。

#### 2.3.6 辅助 avg_amount_20d（流动性，不入 alpha）

```text
avg_amount_20d = mean(amount[t-19..t])   （20 会话，amount 非负；窗口不足或有 null → 无效）
parameters.eligible_for_alpha = false（assert_alpha_eligible 会拒绝其进 alpha 合成）
```

---

### 2.4 未来收益标签（向量化，禁止行循环）

主键 `signal_date, instrument_id, horizon, label_kind`；列 `return_start, return_end, future_return,
is_valid, invalid_reason`。

```text
entry = signal_date 后第 1 个会话的 open
exit  = signal_date 后第 h 个会话的 close
future_return = exit/entry - 1
```

`label_kind`：
- `THEORETICAL_FORWARD_RETURN`：只要有价即算（纯预测关系）。
- `EXECUTABLE_FORWARD_RETURN`：额外要求入场日可成交（`is_listed & not is_suspended & 非一字涨停`）。

`invalid_reason` **固定优先级**（命中即停）：

```text
1 INCOMPLETE_FORWARD_WINDOW   # signal_date 后不足 h 个会话
2 NOT_LISTED_AT_ENTRY
3 ENTRY_SUSPENDED
4 ENTRY_LIMIT_UP              # 仅 EXECUTABLE
5 ENTRY_LIMIT_DOWN           # 仅 EXECUTABLE（卖出场景对称，视用途）
6 MISSING_ENTRY_PRICE
7 MISSING_EXIT_PRICE
8 DELISTED_WITHOUT_EXIT_PRICE
9 NONFINITE_RETURN
```

向量化实现（join + shift，**无 per-(date,instrument) 循环**）：

```python
sessions = trade_calendar 内开市日升序，赋序号 idx
entry = join(signal(idx=i) , bars on (instrument, session=sessions[i+1]))   # 用序号偏移 join
exit  = join(signal(idx=i) , bars on (instrument, session=sessions[i+h]))
future_return = when(entry.open>0 & exit.close>0).then(exit.close/entry.open - 1).otherwise(None)
invalid_reason = coalesce(优先级 when 链)   # 见上
```

不得仅用 `future_return=null` 表达失败；不得在 join/聚合静默丢无效样本。

---

### 2.5 统计内核（字面量 oracle 覆盖）

所有内核输入 `(signal_date, instrument_id, value/return, is_valid)`，输出保留无效原因。

#### 2.5.1 IC（Pearson + Spearman RankIC）

每 (factor, label_kind, horizon, signal_date)：

```text
有效样本 = factor.is_valid & return 有限，按 instrument 对齐
Pearson IC = corr(factor_value, future_return)
Rank IC    = corr(rank(factor_value), rank(future_return))   # 并列取平均秩
无效原因优先级：
  INSUFFICIENT_CROSS_SECTION (有效因子数 < min_cross)
  INSUFFICIENT_FORWARD_PAIRS (配对数 < min_cross)
  ZERO_FACTOR_VARIANCE / ZERO_RETURN_VARIANCE / NONFINITE_IC
滚动均值 = 最近 N=20 个"有效 IC 日"均值（min_valid=10）；累计=只累加有效日；正值率=IC>0 占比
```

平均秩实现：

```python
def average_rank(x):
    order = argsort(x, kind="stable"); r = empty(len(x))
    i=0
    while i<len(x):
        j=i+1
        while j<len(x) and x[order[j]]==x[order[i]]: j+=1
        r[order[i:j]] = (i+j-1)/2.0; i=j
    return r
```

oracle 例：`factor=[1,2,3]`, `ret=[1,2,3]` → Pearson=RankIC=1.0；`ret=[3,2,1]` → −1.0；
`factor=[1,1,2]`（并列）秩=[0.5,0.5,2]。

#### 2.5.2 分位分组 / 分层收益

```text
默认 STABLE_SPLIT：按 (value, instrument_id) 升序，bucket = floor(rank * Q / n) + 1
（KEEP_TIES / PERCENTILE_BOUNDARY 可配）
每 (signal_date, quantile) 输出：实际边界、样本数、mean_return、is_empty
分层诊断：各组累计净值、单调性、组序号-收益相关、Q−Q1 多空、胜率/年化/Sharpe/最大回撤
```

#### 2.5.3 多空 / 相关 / 显著性

```text
long_short = Q 组均值 − 1 组均值；终端组空/配对不足 → 原因码
相关矩阵：同日同股票池有效截面 Pearson + Rank，跨日均值
显著性（5/20 日重叠持有期必做）：Newey-West/HAC 或 block bootstrap，发布 t-stat/CI/p-value
多因子并检：记 Bonferroni / BH-FDR 校正
```

全部内核用向量化 + NumPy；分组用 `over(signal_date)` / `partition_by`，禁止逐行 Python。

---

### 2.6 因子研究产物（FACTOR_STUDY kind）

产物目录（实验层写，见 experiment-layer §）：
`summary / coverage / ic / quantile_returns / long_short_returns / correlation`（+ 可选 significance/stability）。
主键见设计文档 `design.md §5.2`（各表 `signal_variant, factor_ref, horizon, signal_date` 组合）。

`signal_variant`：`DIRECTION_ADJUSTED`（方向调整基线）、`INDUSTRY_NEUTRALIZED`（显式启用行业时）。
行业中性化 = 方向调整后按信号日 as-of `industry_code` 组内等权去均值；单成员组 `SINGLE_MEMBER_INDUSTRY` 失效。

---

### 2.7 测试清单（TDD）

- **输出契约**：`validate_factor_output` 7 条不变量各正/负例。
- **PIT**：`available_at 上海日期 <= trade_date` 强制；roe_pit 190 日边界（189/190/191）；财务修订不回填。
- **窗口**：动量跳 20、波动率 ddof=1、maxdd 峰谷、缺行使窗口失效、停牌零收益计入。
- **向量化门禁**：因子/标签/分位代码无 per-(date,instrument) Python 行循环（审查 + 性能测试）。
- **标签**：invalid_reason 优先级顺序；THEORETICAL vs EXECUTABLE 分区不混。
- **统计 oracle**：IC/RankIC（含并列、常数截面、单样本、NaN/Inf、空组、零方差）、分位边界、多空、HAC。
- **确定性**：相同输入相同排序与数值。

---

### 2.8 关键不变量汇总

1. 输出精确 schema + 7 条不变量；`available_at` 上海日期 ≤ trade_date。
2. 窗口按会话计数；缺行使窗口失效；不回退不完整窗口。
3. 标签含 label_kind + 固定优先级 invalid_reason，不静默丢样本。
4. 全向量化，禁止行循环；统计内核字面量 oracle 覆盖。
5. 每次运行重算，无跨运行缓存；运行内共享读取缓存。
6. 因子只经 `ResearchDataRepository` 取数，显式声明数据依赖（行业必须显式）。

---

## 3. 回测引擎实现

### 3.1 职责边界

回测引擎负责：按交易日推进时间轴，逐日 ①处理公司行为 ②调策略取订单 ③撮合 ④账务落地 ⑤估值。
它**不产生信号**（策略产生）、**不算因子**（因子层）。引擎消费 `Strategy.on_event` 返回的
`OrderIntent`，产出逐日 `AccountSnapshot` + 成交/拒绝流水 + 费用流水。

> **实现顺序（重要）**：分三段（见 `design.md` 第 12 章）。
> - **P3 首版 = 纯多头无公司行为**：`BUY/SELL` + T+1 + FIFO + 撮合规则 + `equity = cash +
>   long_market_value − accrued_fees`。**无分红送转、无做空**。
> - **[P3b-1] 公司行为**：`corporate_action` ledger（现金分红 `DIVIDEND`、送转调股）。
> - **[P3b-2] 做空**：`SHORT_OPEN/SHORT_COVER/BORROW_FEE/MARGIN_*`、`ShortPosition`、
>   `available_margin`、`short_market_value`，equity 增 `− short_market_value` 项。
> 本文含全部阶段细节，按上述标签区分。P3 阶段引擎收到 `SHORT_OPEN/SHORT_COVER` 拒绝
> `SHORT_NOT_SUPPORTED`；凡未标 [P3b-*] 的即 P3 必做。

---

### 3.2 核心数据结构

#### 3.2.1 OrderIntent

```python
class OrderSide(StrEnum): BUY; SELL; SHORT_OPEN; SHORT_COVER   # SHORT_* 为 [P3b-2]

@dataclass(frozen=True, slots=True)
class OrderIntent:
    """引擎唯一输入——只带整数股数，不含权重。"""
    instrument_id: InstrumentId
    side: OrderSide
    quantity: int                         # 正整数股数（唯一表达）
    reason: str = ""
```

引擎撮合只认 `quantity`（正整数）；**`OrderIntent` 不含 `target_weight`**。目标权重经
`RebalancePlanner`（strategy-layer）翻译成整数股数订单，权重不进入引擎输入。同一 `(instrument_id)`
在一日的意图去重（一标的一方向一笔）。

#### 3.2.2 AccountView / DecisionContext（传给策略）

```python
@dataclass(frozen=True, slots=True)
class AccountView:
    cash_fen: int
    positions: Mapping[InstrumentId, int]      # 正=多头；[P3b-2] 负=空头
    sellable: Mapping[InstrumentId, int]        # T+1 可卖多头数量
    available_margin_fen: int                   # [P3b-2] 可用保证金
```

`DecisionContext`（见 interfaces §3.5）：`signal_date, execute_date, data, account`。`data` 是
**绑定 signal_date 的只读窄视图 `DecisionData`**（`bars/factor_values/industry/...` 方法均无 as_of/end
参数），引擎构造时即绑定到该日——策略在类型上无法请求 > signal_date 的数据（PIT 物理边界）。

#### 3.2.3 Ledger 事件（账务事实来源）

```python
class LedgerEventType(StrEnum):
    OPENING_CASH; BUY; SELL                          # P3
    DIVIDEND                                         # [P3b-1]
    SHORT_OPEN; SHORT_COVER; BORROW_FEE; MARGIN_POST; MARGIN_RELEASE   # [P3b-2]

@dataclass(frozen=True, slots=True)
class LedgerEvent:
    event_id: str                # 唯一，形如 "exec:2023-06-02:3"
    event_type: LedgerEventType
    trade_date: date
    instrument_id: InstrumentId | None
    cash_delta_fen: int          # 现金变动（可负）
    quantity_delta: int          # 头寸变动（多头买+/卖−；空头 open−/cover+）
    cost_basis_delta_fen: int    # 成本基础变动
    gross_value_fen: int         # 成交额（非负）
    fees_fen: int                # 费用（非负）
    source_id: str
```

#### 3.2.4 Lot（多头）与空头头寸

```python
@dataclass(slots=True)
class LongLot:                    # 多头分笔，FIFO
    buy_date: date; sellable_date: date; quantity: int; cost_basis_fen: int
@dataclass(slots=True)
class ShortPosition:              # 空头
    open_date: date; quantity: int; open_proceeds_fen: int; margin_posted_fen: int
```

---

### 3.3 账务模型（PortfolioAccount）

#### 3.3.1 不变量（每次 mark 强制）

```text
long_market_value   = Σ 多头 lot 数量 × 当日 close
short_market_value  = Σ |空头数量| × 当日 close      （欠券方负债）[P3b-2]，P3 恒为 0
accrued_fees        = 已计提未结算费用（含 [P3b-2] 融券费）
equity = cash + long_market_value − short_market_value − accrued_fees
#   P3   : equity = cash + long_market_value − accrued_fees   （short_market_value=0）
```

- **无双算**：空头盈亏已隐含在 `cash`（含开仓所得）与 `short_market_value`（按现价 mark）的差额里，
  **不再单列浮盈亏**。[P3b-2] 保证金是 cash 的占用/约束（`margin_used`），**不计入 equity**。
- **双向对账**：每日由 ledger 归约出的 `(cash, 各标的净头寸, 成本)` 必须等于 lot/空头结构累加出的值；
  不等 → `RuntimeError`（账务 bug，立即暴露）。

```python
def mark_to_market(trade_date, closes):
    cash_l, qty_l, cost_l = reduce_ledger(self.ledger)      # 从流水累加
    cash_p, qty_p, cost_p = totals_from_positions(self.long_lots, self.shorts)  # 从结构累加
    assert cash_l == self.cash and qty_l == qty_p and cost_l == cost_p     # 双向对账
    long_mv   = Σ qty × closes[i] for 多头
    short_mv  = Σ |qty| × closes[i] for 空头        # [P3b-2]，P3 为 0
    equity = self.cash + long_mv - short_mv - self.accrued_fees
    return AccountSnapshot(trade_date, cash, positions, long_mv, short_mv,
                           accrued_fees, margin_used, equity)
```

#### 3.3.2 会话相位

`begin_session(d)` → `apply(execution)` → `mark_to_market(d)`。相位机保证顺序，
`begin_session` 日期严格递增且为已加载会话；`begin_session` 解锁到期 T+1 lot（`sellable_date <= d`）。

#### 3.3.3 成交落地（apply）

对每笔 FillResult：

```text
BUY:
  charges = gross + fees;  assert charges <= cash（否则 bug，撮合层应已裁剪）
  新增 LongLot(buy_date=d, sellable_date=next_session^settlement, qty=filled, cost_basis=charges)
  cash -= charges;  ledger += BUY(cash_delta=-charges, qty_delta=+filled, cost_basis=+charges, gross, fees)
SELL:
  FIFO 消费到期 lot（sellable_date<=d），按比例分摊 cost（ROUND_HALF_UP）
  proceeds = gross - fees;  cash += proceeds
  ledger += SELL(cash_delta=+proceeds, qty_delta=-filled, cost_basis=-consumed, gross, fees)
SHORT_OPEN:   # [P3b-2]
  margin = ceil(gross × margin_ratio);  assert margin <= available_margin
  proceeds = gross - fees;  cash += proceeds - margin_post  （卖空得款入现金、同时冻结保证金）
  新增 ShortPosition(open_date=d, qty=filled, open_proceeds=proceeds, margin_posted=margin)
  ledger += SHORT_OPEN(...) + MARGIN_POST(...)
SHORT_COVER:  # [P3b-2]
  buy_cost = gross + fees;  cash -= buy_cost;  释放对应保证金 MARGIN_RELEASE
  ledger += SHORT_COVER(...) + MARGIN_RELEASE(...)
apply 末尾：assert cash == execution.ending_cash_fen   （撮合层预算与账务一致）
```

#### 3.3.4 公司行为（逐日，在撮合前）

```text
对 corporate_action where ex_date == trade_date, 对持有该标的者：
  CASH_DIVIDEND: cash += 持仓数量 × cash_per_share（税前，可配税率）; ledger += DIVIDEND
                 空头持有者反向支付（欠付股息）
  STOCK_DIVIDEND/SPLIT: lot 数量 × share_ratio 调整，cost_basis 不变（摊薄单位成本）
```

这使 NAV 在除权除息日不因未复权价跳水而失真（因子/信号侧用前复权，账务侧用未复权 + 事件补偿）。

---

### 3.4 交易规则 RuleBook（配置化）

从 `configs/rules/a_share.yaml` 加载，暴露：

```python
class MarketRuleBook:
    def trading_profile(self, instrument, instrument_type, board, trade_date) -> InstrumentTradingProfile
    def price_limits(self, profile, trade_date, preclose, status) -> PriceBand | None
    def fees(self, fill, profile) -> FeeBreakdown          # 整数分
    def borrow_fee(self, short_position, days) -> int      # 融券成本（分）
    @property content_hash: str
```

- `InstrumentTradingProfile`：`price_tick`（最小变动价，如 0.01）、`buy_minimum/buy_increment`
  （整手 100）、`sell_minimum/sell_increment`、`allow_full_odd_lot_sell`、`settlement_sessions`（T+1=1）、
  `short_margin_ratio`。
- `PriceBand`：`upper/lower`（元）。主板 ±10%、创业板/科创板 ±20%、ST ±5%、新股/无涨跌幅日返回 None。
  `upper = round_to_tick(preclose × (1+limit))`，`lower = round_to_tick(preclose × (1−limit))`。
- `FeeBreakdown`：`commission_cents + stamp_duty_cents + transfer_fee_cents = total_cents`；
  佣金 = `max(gross × rate, 最低佣金)`；印花税仅卖出；过户费按沪市规则。全整数分、非负。
- `borrow_fee = ceil(short_notional × annual_rate × days / 365)`。

费率参数**必须与策略侧 TransactionCostModel 同源**（成本双角色一致性，见 strategy-layer）。

---

### 3.5 撮合模型 ExecutionModel

输入：当日待执行 `intents`、当日 `MarketSlice`（含 open/high/low/close/preclose/volume/is_suspended/
instrument_type/board/security_status）、`AccountView`、`RuleBook`、`ExecutionConfig`。

`ExecutionConfig`：`reference_price ∈ {OPEN, CLOSE}`（默认 OPEN，T+1 用次日开盘）、`slippage_bps`、
`max_volume_participation`（成交量参与率上限，如 0.1）。

逐笔撮合（确定性顺序：按 intent 输入序）：

```text
def execute_one(intent, row, cash, account):
    if row.is_suspended: return REJECT(SUSPENDED)
    if row.open is None: return REJECT(NO_MARKET_DATA)
    ref = row.open if config.reference==OPEN else row.close
    profile = rulebook.trading_profile(...)
    band = rulebook.price_limits(profile, date, row.preclose, status)
    # 涨跌停门（保守口径见下方"决策点"）
    if intent.side in {BUY, SHORT_COVER} and band and row.low >= band.upper:
        return REJECT(LIMIT_UP_BLOCKED)          # 全天涨停封死买不进
    if intent.side in {SELL, SHORT_OPEN} and band and row.high <= band.lower:
        return REJECT(LIMIT_DOWN_BLOCKED)        # 全天跌停封死卖不出
    # 数量校验（整手/碎股）
    sellable = account.sellable.get(id,0)  # SELL 用
    if not profile.quantity_valid(side, intent.quantity, position_qty): return REJECT(ODD_LOT)
    # 成交量容量
    capacity = floor(row.volume × config.max_volume_participation)
    if capacity == 0: return REJECT(VOLUME_CAP)
    price = apply_slippage(ref, side, slippage_bps) clipped to band; quantize to tick
    candidate = min(intent.quantity, capacity)
    if side==SELL: candidate = min(candidate, sellable)
    candidate = profile.normalize_quantity(side, candidate)   # 向下取整到整手（碎股卖出例外）
    if candidate == 0: return REJECT(INSUFFICIENT_SELLABLE or VOLUME_CAP)
    # 买入资金约束：二分求最大可负担整手数量（含费用）
    if side in {BUY}: filled = affordable_quantity(cash, candidate, profile, price, rulebook)
    elif side==SHORT_OPEN: filled = margin_affordable_quantity(available_margin, candidate, price, margin_ratio)
    else: filled = candidate
    if filled == 0: return REJECT(INSUFFICIENT_CASH or INSUFFICIENT_MARGIN)
    gross = round_half_up(price × filled × 100)   # 分
    fees = rulebook.fees(SimulatedFill(id, date, side, filled, price), profile)
    unfilled = intent.quantity - filled
    reason = FILLED if filled==intent.quantity else (VOLUME_CAP|INSUFFICIENT_CASH|INSUFFICIENT_SELLABLE)
    return FillResult(..., filled, unfilled, price, gross, fees, reason), updated_cash
```

`affordable_quantity`（二分，含费用，保证不透支）：

```python
def affordable_quantity(cash, candidate, profile, price, rulebook):
    if candidate < profile.buy_minimum: return 0
    lo, hi = -1, (candidate - profile.buy_minimum)//profile.buy_increment
    while lo < hi:
        mid = (lo+hi+1)//2
        q = profile.buy_minimum + mid*profile.buy_increment
        cost = round_half_up(price*q*100) + rulebook.fees(fill(q,price)).total_cents
        if cost <= cash: lo = mid
        else: hi = mid-1
    return 0 if lo<0 else profile.buy_minimum + lo*profile.buy_increment
```

**决策点（涨跌停口径）**：上文用"全天封死才拒绝"（`low>=upper` / `high<=lower`），偏乐观。
更保守可改为"参考价触及涨跌停即拒绝"。建议做成 `ExecutionConfig.limit_fill_policy ∈
{WHOLE_DAY_SEALED, REFERENCE_AT_LIMIT}`，默认 `REFERENCE_AT_LIMIT`（保守，符合"回测可信"）。

拒绝原因码：`SUSPENDED / NO_MARKET_DATA / LIMIT_UP_BLOCKED / LIMIT_DOWN_BLOCKED / ODD_LOT /
VOLUME_CAP / INSUFFICIENT_SELLABLE / INSUFFICIENT_CASH / INSUFFICIENT_MARGIN`。

---

### 3.6 引擎主循环

```python
def run(request, strategy, progress, cancellation):
    calendar = market.calendar(start, end, include_next_session=True)
    sessions = calendar.sessions(start, end)               # 需覆盖 [start,end] 且有 next
    account = PortfolioAccount(initial_cash_fen, calendar)
    pending: list[OrderIntent] | None = None
    for i, d in enumerate(sessions):
        if cancellation.is_cancelled(): abort_staging(); raise BacktestCancelled
        market_slice = market.market_slice(d)
        account.begin_session(d)
        account.apply_corporate_actions(d, corporate_actions_on(d))   # [P3b-1] §3.3.4，撮合前；P3 为 no-op
        view = account.execution_view()
        # 执行上一决策日挂单
        execution = execution_model.execute(pending or [], market_slice, view, rulebook, config)
        account.apply(execution)
        snapshot = account.mark_to_market(d, closes_of(market_slice))
        writer.append(execution, snapshot, benchmark_close_of(d))
        # 生成下一交易日的决策（T 日决策，T+1 执行）
        if i+1 < len(sessions):
            ctx = DecisionContext(signal_date=d, execute_date=sessions[i+1],
                                  data=pit_repo_truncated_to(d), factors=..., account=view)
            pending = list(strategy.on_event(ctx))          # 可空
        progress.update(i+1, len(sessions), d)
    writer.finalize(); return BacktestResult(...)
```

要点：`on_event` 拿到的 `data` 截断到 `signal_date=d`（≤ d 可见）；订单在 `sessions[i+1]` 撮合，
天然 T/T+1 分离。引擎只返回冻结结果和规范化内存表；发布会话失败或取消时清理 staging。

---

### 3.7 产物

逐日生成：`nav`（trade_date, cash_fen, long_market_value_fen, short_market_value_fen,
accrued_fees_fen, margin_used_fen, equity_fen, benchmark_close）、`holdings`（逐标的头寸/可卖/成本/市值）、
`fills`（成交与拒绝，含 reason_code）、`costs`（rule_fees_fen, slippage_fen, total_cost_fen）。
统一交给 Run Publisher 写入不可变目录；可信 Manifest 记录并复核逐文件 SHA-256、字节数、行数、
Schema、主键和排序后，才允许实验 Run 登记成功。

---

### 3.8 测试清单（TDD，字面量 oracle 为主）

- **账务不变量**：P3 `equity = cash + long_mv − accrued_fees`；ledger-vs-结构双向对账。
  ([P3b-2] `equity = cash + long_mv − short_mv − accrued_fees`，验无双算、保证金不进 equity。)
- **T+1**：当日买入当日不可卖；`sellable_date` 解锁。
- **涨跌停**：两种 `limit_fill_policy` 各正/负例；买涨停拒绝、卖跌停拒绝。
- **停牌/缺数据**：REJECT 原因码正确。
- **整手/碎股**：买入向下取整整手；碎股卖出（`allow_full_odd_lot_sell`）。
- **成交量容量 / 部分成交**：capacity 裁剪、unfilled 与 reason 一致。
- **费用**：佣金最低值、印花税仅卖出、过户费；整数分、非负、total 恒等。
- **订单契约**：`OrderIntent` 无权重字段；空头订单（P3b-2 前）拒 `SHORT_NOT_SUPPORTED`。
- **[P3b-1] 公司行为**：现金红利入账、送转调股不改总成本；除权日 NAV 不跳水。
- **[P3b-2] 做空**：SHORT_OPEN 保证金占用、SHORT_COVER 释放、融券费按天计提、空头逐日 mark。
- **买入资金约束**：`affordable_quantity` 二分含费用不透支（边界）。
- **PIT**：`DecisionData` 窄视图物理不含 > signal_date 的行（无 as_of/end 参数）。
- **确定性**：相同输入相同成交/净值序列。

---

### 3.9 关键不变量

1. 订单级驱动；引擎只认 `OrderIntent.quantity`（无权重字段）；权重经 `RebalancePlanner` 翻译。
2. `equity = cash + long_mv − short_mv − accrued_fees`（P3 无 short_mv 项）；无双算；保证金不进 equity；每日双向对账；整数分。
3. T 日决策、T+1 撮合；`DecisionData` 窄视图物理 PIT 边界（无 as_of/end 参数）。
4. 撮合用未复权价；[P3b-1] 公司行为事件补偿账务；因子/信号侧用前复权。
5. 撮合裁剪保证不透支/不超保证金；拒绝有稳定原因码；空头订单 P3b-2 前拒 `SHORT_NOT_SUPPORTED`。
6. 费率与策略 CostModel 同源（一致性见 strategy-layer）。

---

## 4. 策略层实现

### 4.1 分层总览

```text
Strategy(Protocol)                              ← B：异构范式直接实现 on_event
 └ WeightTargetStrategy(基类：target_weights → OrderIntent)
     └ CrossSectionalStrategy(A：五模块装配)
         ├─ 内置配置 etf_rotation
         └─ 内置配置 stock_multifactor
 └ PairsStrategy / TimingStrategy / EventDrivenStrategy   ← B：直接实现 on_event
```

策略只跟稳定契约打交道：`DecisionContext`（绑定 signal_date 的只读窄视图 `DecisionData` + 账户视图）、
`OrderIntent`（输出，只带整数股数）。

---

### 4.2 Strategy 契约与注册表

```python
@dataclass(frozen=True, slots=True)
class StrategySpec:
    strategy_id: str
    frequency: str                              # "daily"
    data_dependencies: tuple[DatasetKind, ...]
    factor_dependencies: tuple[str, ...]        # 需要引擎预算的 factor_id
    parameters: Mapping[str, JsonValue]

class Strategy(Protocol):
    @property
    def spec(self) -> StrategySpec: ...
    def warmup(self, ctx: DecisionContext) -> None: ...        # 可空实现
    def on_event(self, ctx: DecisionContext) -> Sequence[OrderIntent]: ...

class StrategyRegistry:
    def register(self, factory: Callable[[Mapping], Strategy], *, strategy_id: str) -> None: ...
    def build(self, strategy_id: str, params: Mapping) -> Strategy: ...
```

- `factory` 接收 params 字典构造实例；`build` 供实验层按配置装配。
- 数据/因子依赖在 `spec` 声明，实验层 preflight 校验；不满足 → `STRATEGY_CAPABILITY_UNAVAILABLE`。
- `on_event` 每个决策时点调用一次，返回该日订单意图（可空）。

---

### 4.3 WeightTargetStrategy 基类 + RebalancePlanner（权重 → 订单）

覆盖"截面/组合类"策略：子类只实现 `target_weights(ctx) -> TargetWeights`（权重和 ≤ 1，余额为现金）；
基类调 **`RebalancePlanner`** 把权重翻译成整数股数 `OrderIntent`。**权重是独立 DTO，不进引擎输入**——
引擎只消费 `OrderIntent.quantity`。翻译职责单列成 `RebalancePlanner`，边界清晰、可单测。

```python
@dataclass(frozen=True, slots=True)
class TargetWeights:
    signal_date: date; execute_date: date
    weights: Mapping[InstrumentId, float]     # 正=多；[P3b-2] 负=空

class WeightTargetStrategy(Strategy):
    planner: RebalancePlanner
    def should_rebalance(self, ctx) -> bool: ...                 # 默认按 frequency 边界
    def target_weights(self, ctx) -> TargetWeights: ...          # 子类实现

    def on_event(self, ctx):
        if not self.should_rebalance(ctx): return []
        targets = self.target_weights(ctx)
        ref_prices = ctx.data.bars(list(targets.weights), lookback_sessions=1) 的 signal_date close
        return self.planner.plan(targets, ctx.account, ref_prices)
```

#### 4.3.1 RebalancePlanner.plan：权重 → 整数股数订单

```text
def plan(targets, account, ref_price):        # 独立组件，可单测
    nav = account 估值（cash + 持仓市值，用 signal_date close 估）
    orders = []
    # 需要清空的现有持仓（不在 targets 里）
    for i in account.positions where i not in targets.weights:
        orders += SELL(i, quantity=account.positions[i])          # 多头清仓；[P3b-2] 空头则 SHORT_COVER
    for i, w in targets.weights.items():
        target_value = w × nav
        target_qty   = round_down_to_lot( target_value / ref_price[i] )   # 整手
        delta = target_qty - account.positions.get(i, 0)
        if delta > 0: orders += BUY(i, quantity=delta)            # [P3b-2] 或 SHORT_COVER 若在减空头
        if delta < 0: orders += SELL(i, quantity=-delta)          # [P3b-2] 或 SHORT_OPEN 若目标为空
    return dedupe_by_instrument(orders)     # 只发差额、整手取整；负权重→空头为 [P3b-2]
```

要点：只发**差额**订单（减少换手）；股数向下取整整手；负权重 → 空头（`SHORT_OPEN/SHORT_COVER`）。
引擎最终按 T+1 实际价撮合，基类的 ref_price 仅用于估算数量，成交可能部分/失败——这是正常的。

---

### 4.4 A：截面五模块

#### 4.4.1 模块协议（消费者侧）

```python
class AlphaModel(Protocol):
    def expected_returns(self, ctx, universe) -> pl.DataFrame: ...
        # 列: instrument_id, score, is_valid, reason_code
class RiskModel(Protocol):
    def covariance(self, ctx, universe) -> CovarianceEstimate: ...   # NoRisk 返回占位
class TransactionCostModel(Protocol):
    def estimate(self, trades, ctx) -> pl.DataFrame: ...              # 列: instrument_id, est_cost_fen
class ConstraintSet(Protocol):
    def apply(self, weights, ctx) -> pl.DataFrame: ...                # 裁剪到约束内
    def validate(self, weights) -> None: ...                          # 构建后二次校验
class PortfolioConstructionModel(Protocol):
    def construct(self, alpha, risk, cost, constraints, ctx, current) -> Mapping[InstrumentId, float]: ...
```

每类一注册表 `model_id → 实现`。

#### 4.4.2 内置 AlphaModel：multi_factor_composite

口径（固定顺序，复用因子层共享 transform，禁止另写近似）：

```text
对每个因子 f：
  取 ctx.data.factor_values([f], universe) 的有效值（视图已绑定 signal_date）
  MAD 去极值(winsorize_mad, n_mad=3, 按 signal_date 分组)
  截面标准化(zscore, 按 signal_date 分组)
  方向调整: value *= direction[f]
按类别聚合：category_score = mean(该类别下有效因子的标准化值)
综合分 = Σ category_weight[c] × category_score[c]
有效性：有效因子数 < min_valid_factors(默认5) 或 某类别全缺 → 排除(reason_code)
```

类别与权重（默认，可配）：`VALUE:0.25 QUALITY:0.25 MOMENTUM:0.30 RISK:0.20`；
因子→(类别,方向) 定义表见因子层。`winsorize_mad`/`zscore` 直接调因子层 `transforms`。

#### 4.4.3 内置 RiskModel

- `none`：返回占位（对角=1），优化器据此退化为纯打分（不含风险项）。
- `sample_cov` / `shrinkage`（Ledoit-Wolf）：Σ 只由 `available_at ≤ signal_date` 的 `log_returns` 估计
  （PIT 铁律，估计窗口不越 signal_date）。

#### 4.4.4 内置 TransactionCostModel：fixed_bps

```text
est_cost_fen = max(规则簿最低佣金, round_half_up( |trade_notional| × cost_bps / 10000 ))
cost_bps 与最低佣金由 configs/rules/a_share.yaml 的同一规则簿实例注入，Run YAML 不得覆盖
linear_impact 追加 × 参与率项
```

#### 4.4.5 内置 ConstructionModel：top_n_equal_weight

```text
候选 = alpha.is_valid & adv_amount >= min_adv_amount
按 (score DESC, instrument_id) 排序取前 max_positions
若数量 < min_positions → ConstraintViolation
等权 base = 1/n，clip 到 max_position_weight
初次建仓豁免换手约束；否则校验 turnover <= max_turnover
返回 target_weights（余额为现金）
```

`mean_variance`（进阶）：最大化 `αᵀw − λ·wᵀΣw − TC(w)` s.t. ConstraintSet；先闭式/轻量 QP，
不引重型求解器依赖。要求 RiskModel 非退化，否则 `PIPELINE_MODEL_UNAVAILABLE`。

#### 4.4.6 ConstraintSet（YAML 声明）

`max_position_weight / min_positions / max_positions / min_adv_amount / max_turnover /
industry_neutral | industry_exposure_bound / max_gross / max_net`。构建后 `validate` 二次校验。
行业约束用信号日 as-of `industry_code`（PIT），单成员组失效。

#### 4.4.7 CrossSectionalStrategy 装配

```python
class CrossSectionalStrategy(WeightTargetStrategy):
    pipeline: StrategyPipeline    # alpha/risk/cost/construction/constraints
    def target_weights(self, ctx) -> TargetWeights:
        universe = ctx.data.stock_universe().filter(eligible)     # 视图已绑定 signal_date，无需传日期
        alpha = self.pipeline.alpha.expected_returns(ctx, universe)
        risk  = self.pipeline.risk.covariance(ctx, universe)
        w = self.pipeline.construction.construct(alpha, risk, self.pipeline.cost,
                                                 self.pipeline.constraints, ctx, ctx.account)
        w = self.pipeline.constraints.apply(w, ctx); self.pipeline.constraints.validate(w)
        return TargetWeights(ctx.signal_date, ctx.execute_date, w)
```

内置策略 = 该类的两份配置：`etf_rotation`（动量/趋势/波动 alpha + none risk + fixed_bps + top_n）、
`stock_multifactor`（七因子 composite + none/shrinkage + fixed_bps + top_n/mvo）。

---

### 4.5 B：异构范式插件（直接实现 on_event）

> **做空依赖 [P3b-2]**：本节 `SHORT_OPEN/SHORT_COVER` 需回测层做空账务（P3b-2）。P3 阶段引擎会拒绝空头订单
> （`SHORT_NOT_SUPPORTED`），故配对纯对冲、CTA 空头腿随 P3b-2 解锁；多头择时、事件驱动多头版首版即可跑。
> 权重→订单翻译中的"负权重→空头"（§4.3.1）同属 [P3b-2]。

#### 4.5.1 配对交易 PairsStrategy（做空对冲）[P3b-2]

```text
参数: (leg_a, leg_b, lookback, entry_z, exit_z, notional)
on_event:
  spread = log(pa) - beta*log(pb)  用 ctx.data ≤ signal_date 的前复权价估 beta 与 z
  z = (spread - rolling_mean)/rolling_std
  持仓状态机：
    无仓 & z > entry_z → SHORT_OPEN(a, q_a) + BUY(b, q_b)     # 做空价差
    无仓 & z < -entry_z → BUY(a) + SHORT_OPEN(b)
    有仓 & |z| < exit_z → 平两腿(SHORT_COVER + SELL)
```

#### 4.5.2 择时/CTA TimingStrategy

```text
参数: (instrument, fast, slow) 或 (instrument, breakout_window)
on_event:
  signal = 快线上穿慢线 → 目标满仓多；下穿 → 平仓（或做空）
  返回对应 BUY/SELL/SHORT_OPEN/SHORT_COVER 达到目标仓位
```

#### 4.5.3 事件驱动 EventDrivenStrategy

```text
参数: (event_source_factor 或 data 条件, holding_days)
on_event:
  若 signal_date 触发事件（如财务披露、状态变化，全部 ≤ signal_date 可见）→ 稀疏下单
  到持有期满 → 平仓；无事件日返回 []
```

三者都只经 `ctx.data`（绑定 signal_date 的窄视图，含 `factor_values`）取输入，直接产 `OrderIntent`，无需 target_weights。

---

### 4.6 成本双角色一致性（硬约束）

`TransactionCostModel`（事前，优化器/翻译用）与回测 `RuleBook.fees/borrow_fee`（事后，撮合实扣）
**必须由同一费率配置构造**。装配时校验：

```text
assert cost_model.rate_config == rulebook.rate_config    # 同源
一致性测试：对同一笔成交，cost_model.estimate 与 rulebook.fees 的费用项在同参数下逐项对账
不一致 → COST_MODEL_INCONSISTENT
```

否则优化器会对着一个与实际脱节的成本模型下单。

---

### 4.7 配置 schema（YAML，Pydantic 严格）

```yaml
strategy_id: stock_multifactor          # 或 etf_rotation / pairs / timing / event_driven
frequency: daily
pipeline:                                # 仅 CrossSectionalStrategy 需要
  alpha:        {model_id: multi_factor_composite, params: {...}}
  risk:         {model_id: none}
  cost:         {model_id: fixed_bps}
  construction: {model_id: top_n_equal_weight, params: {max_positions: 50}}
  constraints:  {max_position_weight: 0.05, min_positions: 20, max_positions: 50,
                 min_adv_amount: 50000000, max_turnover: 0.4}
params: {}                               # 插件策略在此放自身参数
```

`extra=forbid, strict, frozen`；未知键报错。

---

### 4.8 测试清单（TDD）

- **契约**：`on_event` 返回合法 `OrderIntent`（只带整数股数，无权重字段）；spec 依赖声明；缺能力 → `STRATEGY_CAPABILITY_UNAVAILABLE`。
- **RebalancePlanner**（独立单测）：差额订单、整手向下取整、清仓路径；负权重→空头为 [P3b-2]；字面量 oracle。
- **multi_factor_composite**：MAD→zscore→方向→类别聚合顺序；复用因子层 transform（不另写）；
  有效因子数不足→排除；oracle。
- **RiskModel PIT**：Σ 估计窗口不越 signal_date（`DecisionData` 窄视图保证）。
- **成本一致性**：事前/事后同参数对账；负向用例触发 `COST_MODEL_INCONSISTENT`。
- **top_n**：min/max_positions、max_turnover、min_adv 约束；初次建仓豁免。
- **多头范式**：timing（多头）、event-driven（多头）各跑通一个最小样例；pairs 做空对冲随 [P3b-2]。
- **扩展性（核心）**：新增一个 stub 插件策略注册后端到端跑通，**不改 runner/引擎/基础设施**。
- **回归黄金结果**：etf_rotation / stock_multifactor 固定小样本锁定输出。

---

### 4.9 关键不变量

1. 策略只跟 `DecisionContext`(含 `DecisionData` 窄视图)/`OrderIntent` 契约打交道；不触 runner/引擎/基础设施。
2. 截面策略经五模块装配（配置驱动）；异构策略直接实现 `on_event`（插件）。
3. multi_factor_composite 固定 MAD→zscore→方向→类别聚合，复用因子层 transform。
4. RiskModel/CostModel 估计窗口受 `DecisionData` 的 signal_date PIT 约束（无 as_of/end 参数）。
5. 事前成本与回测费率同源可对账。
6. 权重是 `TargetWeights` DTO，经独立 `RebalancePlanner` 翻译成整数股数 `OrderIntent`（只发差额、整手取整；负权重→空头为 [P3b-2]）；权重不进引擎。

---

## 5. 实验层实现

### 5.1 职责

实验层编排"一次策略回测或因子研究"：装配管线 → 分阶段执行 → 运行内一致性守卫 → 落盘 → 追踪与比较。
**不做跨运行数据回放**（无历史 catalog、无 source/env 指纹和 run_identity），但每个 Run 的冻结配置与
产物不可覆盖；可信 Manifest 逐文件记录并复核 SHA-256、字节数、行数、Schema、主键和排序。
运行内一致性靠 `catalog_hash`（见 §5.5）。

---

### 5.2 追踪实体与状态机

```python
class ExperimentKind(StrEnum): FACTOR_STUDY; STRATEGY_BACKTEST
class RunStatus(StrEnum): CREATED; QUEUED; RUNNING; SUCCEEDED; FAILED; CANCELLED
class ResearchMark(StrEnum): BASELINE; CANDIDATE; DISCARDED; NONE
```

状态迁移（乐观并发 CAS，携带期望前态）：

```text
CREATED → QUEUED → RUNNING → SUCCEEDED
                        ├→ FAILED
                        └→ CANCELLED
合法迁移表（其余一律拒绝，抛 EXPERIMENT_STATE_CONFLICT）：
  CREATED→QUEUED, QUEUED→RUNNING, RUNNING→{SUCCEEDED,FAILED,CANCELLED}
transition(run_id, expected, target)：UPDATE ... WHERE id=? AND status=expected；
  影响行数==0 → EXPERIMENT_STATE_CONFLICT
```

- `SUCCEEDED` 必须已写 `artifact_dir` 且产物验证通过（§5.6）。
- `FAILED` 只存 `error_json`（code + 安全上下文），不写 traceback/敏感路径。
- 一个 Run 只绑一个 task；重试建新 Run + 新 task（不覆盖旧 Run）。
- 重跑同一配置：只允许复制冻结配置并创建新 Run、新 task 和新产物目录。

---

### 5.3 SQLite DDL

```sql
CREATE TABLE experiment (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL, description TEXT NOT NULL,
  definition_json TEXT NOT NULL, definition_hash TEXT NOT NULL,
  baseline_run_id TEXT, created_at TEXT NOT NULL);
CREATE TABLE experiment_tag (experiment_id TEXT NOT NULL, tag TEXT NOT NULL,
  PRIMARY KEY(experiment_id, tag), FOREIGN KEY(experiment_id) REFERENCES experiment ON DELETE CASCADE);
CREATE TABLE run (
  id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL, task_id TEXT NOT NULL,
  status TEXT NOT NULL, stage TEXT NOT NULL, config_json TEXT NOT NULL,
  config_hash TEXT NOT NULL, catalog_hash TEXT NOT NULL,
  uses_test_region INTEGER NOT NULL, research_mark TEXT NOT NULL DEFAULT 'UNREVIEWED',
  artifact_dir TEXT, manifest_hash TEXT, created_at TEXT NOT NULL, started_at TEXT,
  completed_at TEXT, error_json TEXT,
  FOREIGN KEY(experiment_id) REFERENCES experiment ON DELETE CASCADE);
CREATE TABLE run_metric (
  id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, name TEXT NOT NULL,
  value REAL NOT NULL, unit TEXT, p_value REAL, adjusted_p_value REAL, created_at TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES run ON DELETE CASCADE);
CREATE TABLE run_tag (run_id TEXT NOT NULL, tag TEXT NOT NULL, PRIMARY KEY(run_id, tag));
CREATE TABLE run_artifact (
  id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, artifact_type TEXT NOT NULL,
  relative_path TEXT NOT NULL, content_hash TEXT NOT NULL, byte_count INTEGER NOT NULL,
  row_count INTEGER, schema_json TEXT, created_at TEXT NOT NULL,
  UNIQUE(run_id, artifact_type), FOREIGN KEY(run_id) REFERENCES run ON DELETE CASCADE);
CREATE TABLE audit_event (
  id INTEGER PRIMARY KEY, run_id TEXT, subject_kind TEXT NOT NULL, subject_id TEXT NOT NULL,
  task_id TEXT, event_type TEXT NOT NULL, actor TEXT NOT NULL,
  details_json TEXT NOT NULL, created_at TEXT NOT NULL);   -- append-only
CREATE TABLE task (
  id TEXT PRIMARY KEY, subject_kind TEXT, subject_id TEXT, task_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0, idempotency_key TEXT UNIQUE,
  worker_id TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0,
  heartbeat_at TEXT, created_at TEXT NOT NULL, available_at TEXT,
  locked_at TEXT, completed_at TEXT, error_json TEXT);
CREATE TABLE task_attempt (
  id TEXT PRIMARY KEY, task_id TEXT NOT NULL, attempt_no INTEGER NOT NULL, status TEXT NOT NULL,
  started_at TEXT, heartbeat_at TEXT, completed_at TEXT, error_json TEXT,
  FOREIGN KEY(task_id) REFERENCES task ON DELETE CASCADE);
```

> `task`/`task_attempt` 是全平台通用任务队列（不止实验），完整 DDL 与语义以 `§6.2` 为准；
> 此处为实验相关字段的摘要。

`config_json` 是冻结配置快照（canonical_json）。`catalog_hash` 记录提交时的数据版本（§5.5）。

---

### 5.4 阶段图与执行器（kind 无关）

```text
STRATEGY_BACKTEST: VALIDATE → PREPARE_INPUTS → STRATEGY_RUN(交织 BACKTEST) → ANALYTICS → PERSIST
FACTOR_STUDY:      VALIDATE → PREPARE_INPUTS(股票池+因子) → ANALYZE_FACTORS → PERSIST
```

`StageGraph.stages(kind)` 返回阶段序列；`ExperimentRunner` 遍历，每阶段：

```python
def run_stage(stage, ctx):
    runtime.assert_run_internal_consistency(stage)   # 前置一致性门（§5.5）
    result = stage.execute(ctx)                       # 纯函数
    runtime.assert_run_internal_consistency(stage)   # 后置再校验
    checkpoint(stage, result)
    if cancellation.is_cancelled(): cleanup_staging(); raise EXPERIMENT_CANCELLED
    return result
```

各阶段职责：
- **VALIDATE**：校验配置、数据质量门开启、交易日历覆盖研究区间 + 最长未来窗口、策略/因子能力满足。
- **PREPARE_INPUTS**：构建 PIT 股票池；预算本次需要的因子（`FactorEngine.compute`）；装配策略/管线。
- **STRATEGY_RUN**：`BacktestEngine.run(request, strategy, ...)`，逐日 `on_event`→撮合→账务，仅返回冻结结果与规范化内存表，不发布目录（见 backtest-engine）。
- **ANALYTICS**：直接消费内存中的 `equity_fen/total_cost_fen` 当前 Schema，计算绩效、风险、执行质量和归因（§5.7）。
- **ANALYZE_FACTORS**（因子研究）：覆盖率/IC/分位/多空/相关/显著性（因子层 §5.6）。
- **PERSIST**：统一写 staging、生成 Manifest、原子重命名、从最终目录复核并登记有限且已定义的 metrics；因子 Run 也只在此阶段发布。

失败/取消：清理 staging 临时目录，不留半成品；`FAILED` 写 error_json。

---

### 5.5 运行内一致性门（非复现）

```python
def assert_run_internal_consistency(stage):
    current = repository.catalog_hash()
    if current != run.catalog_hash:
        raise QuantError(EXPERIMENT_DATA_DRIFT, {stage, expected: run.catalog_hash, actual: current})
```

作用：运行中数据被并发更新（另一个 update 任务改了 Canonical）时，本次运行立即失败，
**防止同一次结果混用两批数据**。不存历史版本、不做回放（这是"运行内一致性"，非"跨运行复现"）。

---

### 5.6 产物与登记

产物目录 `artifacts/experiments/<experiment_id>/<run_id>/`，使用可信 Manifest：

```text
STRATEGY_BACKTEST: nav.parquet holdings.parquet fills.parquet costs.parquet
                   signals.parquet orders.parquet performance.parquet
                   monthly_returns.parquet annual_returns.parquet
                   execution_summary.parquet exposure_summary.parquet attribution.parquet
                   config.json metrics.json quality_disclosure.json manifest.json
FACTOR_STUDY:      summary.parquet coverage.parquet ic.parquet quantile_returns.parquet
                   long_short_returns.parquet correlation.parquet config.json metrics.json
                   quality_disclosure.json manifest.json
```

发布：先写同文件系统 staging → 生成 Manifest → 原子 `os.replace` 到最终目录 → 从最终目录复核路径、
SHA-256、字节数、行数、Schema、主键、排序和输入身份。`PERSIST` 成功后才
`RUNNING→SUCCEEDED` 并写 `artifact_dir/manifest_hash`。最终目录已存在立即失败；重试创建新 Run，
绝不接管或覆盖旧目录。

---

### 5.7 分析层指标（字面量 oracle）

输入 `nav` 使用 `cash_fen/long_market_value_fen/short_market_value_fen/accrued_fees_fen/equity_fen`，
并校验 `equity = cash + long_market_value - short_market_value - accrued_fees`。成本输入校验
`rule_fees_fen + slippage_fen = total_cost_fen`。

```text
normalized_nav[t]       = equity_fen[t] / equity_fen[0]
daily_return[t]         = equity_fen[t]/equity_fen[t-1] - 1     （t>=1；daily_return[0]=0）
sample                  = daily_return[1:]                 # 剔除首日 0，用于 vol/Sharpe/Sortino
annualized_return       = normalized_nav[-1] ** (252/N) - 1        （N=观测数；N=1 时未定义→记 undefined）
annualized_volatility   = std(sample, ddof=1) * sqrt(252)
sharpe                  = mean(sample)/std(sample,ddof=1) * sqrt(252)     （std=0→undefined）
sortino                 = mean(sample)/downside_std(sample) * sqrt(252)   （downside_std 用 min(r,0)）
running_peak[t]         = max(normalized_nav[0..t])
drawdown[t]             = normalized_nav[t]/running_peak[t] - 1     （首日=0，用完整序列）
max_drawdown            = min(drawdown)
calmar                  = annualized_return / |max_drawdown|        （max_dd=0→undefined）
active_return           = sample - benchmark_daily[1:]
information_ratio       = mean(active)/std(active,ddof=1) * sqrt(252)
beta, alpha             = OLS(sample ~ benchmark_daily[1:])
gross/net exposure、多空分腿归因（[P3b-2]）：由 holdings 的多头/空头市值分别累计
```

**首日 0 收益口径**（易错点）：波动率/Sharpe/Sortino/IR/beta 用 `daily_return[1:]`（剔首日）；
drawdown/time-under-water 用含首日 0 的完整序列。undefined 指标记入 `undefined_metrics` 而非填 0/NaN。
全部指标字面量 oracle 覆盖。

---

### 5.8 防过拟合治理

```python
@dataclass(frozen=True, slots=True)
class SampleWindows:
    train: tuple[date, date]; validation: tuple[date, date]; test: tuple[date, date]

# Run 提交时：uses_test_region = 回测/研究区间与 test 区间有交集
# Experiment 维度累计 test 预算消耗次数（COUNT(run WHERE uses_test_region=1)），Dashboard 显式展示
# 多重检验记账：Experiment 记录尝试 Run 数、参数组合数、校正方法(BONFERRONI|BH_FDR)
#   显著性报告（因子层）据此校正 p-value
```

不硬阻断超预算 Run（保留研究灵活性），但让样本外偷看**可见、可审计**。

---

### 5.9 Worker（详见 `design.md` 第 10 章 / `第 6 章`）

实验任务统一为 `EXPERIMENT_RUN`，由通用 Worker 队列驱动，与数据类任务共用同一
`task`/`task_attempt` 表和生命周期。实验侧只需知道：

- 提交：`TaskQueue.submit(EXPERIMENT_RUN, {run_id}, subject_kind="EXPERIMENT_RUN",
  subject_id=<run_id>, idempotency_key="experiment-run:<run_id>")`；重复提交按 key 收敛。
- 执行：`ExperimentHandler` 委派 `ExperimentRunner.run(run_id, progress, cancellation)`；
  阶段边界响应心跳与协作取消。
- 取消/重试：协作取消在阶段边界退出、清 staging；重试建**新 Run + 新 task**，不覆盖旧 Run。

队列的 CAS 领取、租约回收、幂等、run_id 可空等机制见 Worker 专文，不在此重复。

---

### 5.10 比较 / 血缘（读侧）

- **排行榜**：`SELECT run.id, run_metric.value FROM ... WHERE experiment_id=? AND name=? ORDER BY value`。
- **配置 diff**：两 Run 的 `config_json` 结构化差异。
- **血缘**：Run → `catalog_hash`（数据版本，展示用）→ 产物目录，追溯到唯一 Run。
- **结论标记**：`research_mark` = BASELINE/CANDIDATE/DISCARDED；`baseline_run_id` 指精确 Run，
  不回退"所属实验最新 Run"。

---

### 5.11 配置 schema（实验 YAML）

```yaml
kind: STRATEGY_BACKTEST                 # 或 FACTOR_STUDY
name: mf_baseline_2020_2023
strategy: {strategy_id: stock_multifactor, ...}     # 见 strategy-layer §5.7；FACTOR_STUDY 则为 factor 配置
start_date: 2020-01-02
end_date: 2023-12-29
benchmark: 000300.SH
initial_cash_fen: 100000000
execution: {reference_price: OPEN, slippage_bps: 5, max_volume_participation: 0.1,
            limit_fill_policy: REFERENCE_AT_LIMIT}
sample_windows: {train: [2015-01-01, 2019-12-31], validation: [2020-01-01, 2021-12-31],
                 test: [2022-01-01, 2023-12-31]}
```

`extra=forbid, strict, frozen`；日期必须明确 `YYYY-MM-DD`。

---

### 5.12 测试清单（TDD）

- **状态机**：合法迁移通过、非法迁移 `EXPERIMENT_STATE_CONFLICT`、并发 CAS 冲突不覆盖。
- **一致性门**：运行中 catalog_hash 变 → `EXPERIMENT_DATA_DRIFT`；前后双校验。
- **阶段**：失败/取消不留半成品目录；`SUCCEEDED` 前产物已落地。
- **指标 oracle**：Sharpe/Sortino/Calmar/回撤/IR/beta；首日 0 口径；undefined 记录不填 0。
- **治理**：`uses_test_region` 正确标记；test 预算计数累加；多重检验记账。
- **Worker**：幂等键收敛、取消协作退出、超时回收、重试建新 Run。
- **kind 复用**：FACTOR_STUDY 与 STRATEGY_BACKTEST 共用同一 runner。

---

### 5.13 关键不变量

1. `Experiment → Run` 轻量追踪；无历史 catalog 回放（但存 `catalog_hash`）；重跑不可覆盖。
2. 状态迁移乐观并发 CAS，携期望前态；一 Run 一 task，重试建新 Run。
3. 每阶段前后校验 `catalog_hash`（运行内一致性，非复现回放）。
4. 产物经 staging、可信 Manifest 和最终目录复核后原子发布；`SUCCEEDED` 前落地成功。
5. 绩效指标字面量 oracle；首日 0 收益口径明确；undefined 显式记录。
6. 防过拟合护栏保留（test 预算 + 多重检验记账），不硬阻断。
7. runner kind 无关；因子研究与策略回测共享同一执行器与比较视图。

---

## 6. Worker 与任务队列实现

### 6.1 职责

Worker = 后台长任务执行器：领取 SQLite 队列中的任务 → 按 `task_type` 分派给 handler → 记录
生命周期与 attempt。**不含业务逻辑**（委派 handler）。默认单进程，队列用 SQLite，乐观并发 CAS
支持多 Worker。

---

### 6.2 SQLite 表（DDL，修正 run_id 可空）

```sql
CREATE TABLE task (
  id TEXT PRIMARY KEY,                       -- ULID
  task_type TEXT NOT NULL,                   -- DATA_UPDATE|DATA_VALIDATION|EXPERIMENT_RUN
  subject_kind TEXT, subject_id TEXT,         -- 实验任务绑定 EXPERIMENT_RUN/run_id
  payload_json TEXT NOT NULL,                -- 冻结参数（canonical_json）
  status TEXT NOT NULL,                       -- QUEUED|RUNNING|SUCCEEDED|FAILED|CANCELLED
  priority INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT UNIQUE,               -- 如 run-<run_id> / data-update-<plan_hash>
  worker_id TEXT,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  available_at TEXT,                         -- 延迟可见；NULL=立即
  locked_at TEXT, heartbeat_at TEXT, completed_at TEXT,
  error_json TEXT,
  FOREIGN KEY(run_id) REFERENCES run ON DELETE SET NULL);
CREATE INDEX ix_task_claim ON task(status, priority DESC, created_at);

CREATE TABLE task_attempt (
  id TEXT PRIMARY KEY, task_id TEXT NOT NULL, attempt_no INTEGER NOT NULL,
  status TEXT NOT NULL,                       -- RUNNING|SUCCEEDED|FAILED|CANCELLED
  worker_id TEXT, started_at TEXT, heartbeat_at TEXT, completed_at TEXT,
  progress_json TEXT, error_json TEXT,
  UNIQUE(task_id, attempt_no),
  FOREIGN KEY(task_id) REFERENCES task ON DELETE CASCADE);
```

> 相对 `§5.3` 的差异：`task.run_id` 由 `NOT NULL` 改为**可空 + ON DELETE SET NULL**，
> 使数据类任务（无 Run）与实验类任务共用同一表。experiment-layer 的 DDL 以本文为准。

---

### 6.3 契约（consumer-side Protocol）

```python
class TaskType(StrEnum):
    DATA_UPDATE; DATA_VALIDATION; EXPERIMENT_RUN
class TaskStatus(StrEnum):
    QUEUED; RUNNING; SUCCEEDED; FAILED; CANCELLED

@dataclass(frozen=True, slots=True)
class ClaimedTask:
    id: str; task_type: TaskType; run_id: str | None
    payload: Mapping[str, JsonValue]; attempt_no: int

@dataclass(frozen=True, slots=True)
class TaskProgress:
    completed: int; total: int; stage: str
@dataclass(frozen=True, slots=True)
class TaskOutcome:
    status: TaskStatus; result: Mapping[str, JsonValue]; error: ErrorDetail | None = None

class TaskQueue(Protocol):
    def submit(self, task_type: TaskType, payload: Mapping[str, JsonValue], *,
               idempotency_key: str, priority: int = 0,
               available_at: datetime | None = None, run_id: str | None = None) -> str: ...
    def claim(self, worker_id: str, now: datetime) -> ClaimedTask | None: ...
    def heartbeat(self, task_id: str, worker_id: str, progress: TaskProgress, now: datetime) -> None: ...
    def complete(self, task_id: str, worker_id: str, outcome: TaskOutcome, now: datetime) -> None: ...
    def request_cancel(self, task_id: str) -> None: ...
    def is_cancel_requested(self, task_id: str) -> bool: ...

class TaskHandler(Protocol):
    def execute(self, task: ClaimedTask, progress: ProgressSink, cancellation: CancellationToken) -> TaskOutcome: ...

class CancellationToken(Protocol):
    def is_cancelled(self) -> bool: ...
```

---

### 6.4 提交（submit，幂等）

```python
def submit(task_type, payload, *, idempotency_key, priority=0, available_at=None, run_id=None):
    canonical_json_bytes(payload)                       # 校验可序列化
    try:
        with tx():
            id = ulid()
            insert task(id, task_type, run_id, payload_json=canonical_json(payload),
                        status='QUEUED', priority, idempotency_key,
                        created_at=now, available_at)
        return id
    except UNIQUE(idempotency_key) 冲突:
        return SELECT id FROM task WHERE idempotency_key=?   # 收敛到已有 task
```

---

### 6.5 领取（claim，CAS）

```python
def claim(worker_id, now):
    with tx():                                           # SQLite 事务保证原子
        row = SELECT * FROM task
              WHERE status='QUEUED' AND (available_at IS NULL OR available_at<=now)
              ORDER BY priority DESC, created_at ASC LIMIT 1
        if row is None: return None
        n = UPDATE task SET status='RUNNING', worker_id=?, locked_at=?, heartbeat_at=?
            WHERE id=? AND status='QUEUED'               # CAS：期望前态 QUEUED
        if n == 0: return None                            # 被别的 Worker 抢先，下轮再试
        attempt_no = (SELECT COALESCE(MAX(attempt_no),0)+1 FROM task_attempt WHERE task_id=?)
        insert task_attempt(id=ulid(), task_id, attempt_no, status='RUNNING',
                            worker_id, started_at=now, heartbeat_at=now)
        return ClaimedTask(row.id, row.task_type, row.run_id, thaw(row.payload_json), attempt_no)
```

---

### 6.6 主循环（run once / run loop）

```python
def run_once(worker_id, now_fn):
    now = now_fn()
    if first_poll or now-last_orphan_scan >= 30s:
        recover_stale(now)                              # §6.7
    task = queue.claim(worker_id, now)
    if task is None: return False
    handler = handlers[task.task_type]                   # §6.8 分派表
    progress = _QueueProgressSink(queue, task.id, worker_id, now_fn)   # 内部调 heartbeat
    cancellation = _QueueCancellation(queue, task.id)    # 内部查 cancel_requested
    try:
        outcome = handler.execute(task, progress, cancellation)
    except BacktestCancelled | ExperimentCancelled:
        queue.complete(task.id, worker_id, TaskOutcome(CANCELLED, {}), now_fn()); return True
    except QuantError as e:
        queue.complete(task.id, worker_id, TaskOutcome(FAILED, {}, e.detail), now_fn()); return True
    except Exception as e:                                # 未知异常转结构化错误，不泄 traceback
        queue.complete(task.id, worker_id, TaskOutcome(FAILED, {}, _to_error_detail(e)), now_fn()); return True
    queue.complete(task.id, worker_id, outcome, now_fn())
    return True

def run_loop(worker_id, now_fn, poll_seconds, stop_flag):
    while not stop_flag():
        if not run_once(worker_id, now_fn):
            sleep(poll_seconds)
```

`complete` 以 CAS 写终态（`WHERE id=? AND worker_id=? AND status='RUNNING'`）；同时把当前 attempt 置终态。
CLI `quant worker once` 调 `run_once`，`quant worker run` 调 `run_loop`。

---

### 6.7 心跳与陈旧回收（lease）

```python
def heartbeat(task_id, worker_id, progress, now):
    UPDATE task SET heartbeat_at=?, updated_at=? WHERE id=? AND worker_id=?
           AND status IN ('RUNNING','CANCEL_REQUESTED')
    UPDATE task_attempt SET heartbeat_at=?, progress_json=? WHERE task_id=?
           AND status IN ('RUNNING','CANCEL_REQUESTED')

def recover_stale(now, lease_timeout=60s):
    with tx():
        for row in SELECT * FROM task
                   WHERE status IN ('RUNNING','CANCEL_REQUESTED')
                     AND updated_at < now-lease_timeout:
            attempts = SELECT * FROM task_attempt WHERE task_id=row.id
                       AND status IN ('RUNNING','CANCEL_REQUESTED')
            if any(coalesce(a.heartbeat_at, a.started_at) >= now-lease_timeout
                   for a in attempts):
                continue
            UPDATE task_attempt SET status='ORPHANED', error_json='{"code":"TASK_ORPHANED"}',
                   completed_at=? WHERE id IN attempts
            UPDATE task SET status='ORPHANED', error_json='{"code":"TASK_ORPHANED"}',
                   completed_at=?, updated_at=?
                   WHERE id=row.id AND status IN ('RUNNING','CANCEL_REQUESTED')  # CAS

    for task in list(status='ORPHANED', subject_kind='EXPERIMENT_RUN'):
        if task.run.status in ('CREATED','QUEUED','RUNNING'):
            transition_run(task.run, FAILED, error_code='TASK_ORPHANED')
```

Worker 首次轮询必执行回收，之后按 30 秒间隔节流。任务和活动 attempt
进入终态 ORPHANED，不自动重领；没有 attempt 的历史活动任务也会收敛。
若 attempt 仍有新鲜心跳则不回收。关联活动实验 Run 同步进入 FAILED；重跑必须走显式重试。

---

### 6.8 Handler 分派（组合根装配）

```python
handlers: dict[TaskType, TaskHandler] = {
    DATA_UPDATE:       DataUpdateHandler(pipeline),           # 按 payload 固化计划跑 LOCALIZE→CURATE→VALIDATE
    DATA_VALIDATION:   DataValidationHandler(pipeline),       # 全目录/单数据集诊断
    EXPERIMENT_RUN:    ExperimentRunHandler(registry, guard, strategy, factor),
}
```

新增任务类型 = 实现 `TaskHandler` + 注册，**不改主循环**。handler 内部在阶段边界调
`progress.update(...)`（触发 heartbeat）与 `cancellation.is_cancelled()`。

---

### 6.9 各 task_type 的 payload 与幂等键

| task_type | payload 关键字段 | idempotency_key |
|---|---|---|
| DATA_UPDATE | `plan_hash, window_mode, dataset_windows[]`（固化计划，Worker 不重解析） | `data-update-<plan_hash>` |
| DATA_VALIDATION | `scope: ALL|DATASET, dataset?` | `data-validate-<scope>-<dataset?>` |
| EXPERIMENT_RUN | `run_id` + `subject_kind/subject_id` | `experiment-run:<run_id>` |

DATA_UPDATE 重试复用原 `plan_hash` 对应的固化计划；水位变化不重解析（预览时若水位变则拒绝旧计划要求刷新，属提交侧）。

---

### 6.10 错误码

```text
TASK_STATE_CONFLICT     CAS 迁移冲突（期望前态不符）
TASK_ORPHANED           心跳超时被回收
TASK_HANDLER_MISSING    task_type 无注册 handler
TASK_PAYLOAD_INVALID    payload 反序列化/校验失败
# 业务错误码由 handler 透传（EXPERIMENT_*, DATA_*）
```

---

### 6.11 测试清单（TDD）

- **CAS 并发**：模拟两 worker_id 对同一 QUEUED 调 claim，仅一个拿到；另一个得 None。
- **幂等**：相同 idempotency_key 两次 submit 返回同一 id；handler 重跑无重复副产物。
- **领取顺序**：priority DESC、同优先级 created_at ASC；`available_at>now` 不被领取。
- **心跳/回收**：heartbeat 推进；超 lease_timeout 的 RUNNING/CANCEL_REQUESTED
  被 recover_stale 终结为 ORPHANED；缺少 attempt 也可回收，新鲜 attempt 不误回收，
  关联活动 Run 进入 FAILED。
- **取消**：request_cancel 后 handler 在边界退出，task=CANCELLED，无 staging 残留。
- **重试**：数据类复用 plan_hash；实验类新 Run+新 task；attempt_no 递增。
- **run_id 可空**：DATA_UPDATE 无 run_id 全生命周期流转；删除 Run 时 task.run_id 置 NULL（SET NULL）。
- **未知异常**：转结构化 FAILED，不泄 traceback。
- **确定性时钟**：`now_fn` 可注入，租约/超时测试无真实 sleep。

---

### 6.12 关键不变量

1. 单队列服务四类任务；`run_id` 可空（数据类无 Run）。
2. 领取/迁移全用 CAS + 期望前态；影响行数 0 即冲突，不覆盖。
3. 任务必须幂等；`idempotency_key` UNIQUE 收敛重复提交。
4. Worker 首次轮询必扫描失联任务，之后每 30 秒至多扫描一次；超时任务终结为 ORPHANED。
5. 取消是协作式、阶段边界检查，不强杀，不留 staging。
6. 重试：数据类复用固化计划、实验类建新 Run（不覆盖历史）。
7. 新增任务类型只注册 handler，不改主循环；Worker 不含业务逻辑。
8. 未知异常在进程边界转结构化错误，不泄敏感信息。


