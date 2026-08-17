# 数据层设计

## 1. 范围与不变量

数据流水线包含三个相互独立的阶段：

```text
LOCALIZE → CURATE → VALIDATE
```

系统不包含数据集版本或数据发布 Snapshot 层。Canonical 存储为每个数据集提供一个当前指针。只有当前目录对应的精确状态通过 `validate-all` 后，研究接口才允许读取数据。

核心不变量：

- Raw 和 Canonical 文件均为内容寻址的 Parquet 文件。
- SQLite 是请求断点、Canonical 当前指针、质量结果、实验和任务的权威元数据源。
- Curate 在一个 SQLite 事务内切换同一数据集的全部分区。
- Canonical 指针发生变化时，全局读取门禁立即失效；内容完全相同时不会使门禁失效。
- 实验提交时记录当前 `data_hash`。该哈希只用于检测数据漂移，不能选择或恢复历史数据。
- 文件路径只负责定位文件，不参与内容身份计算。
- 同一 `QUANT_DATA_ROOT` 同一时刻只允许一个会修改数据状态的流水线执行者；组合入口及
  单阶段入口共享这一边界。
- 不迁移不兼容的数据根目录。发生不兼容的结构变更后，应重新构建 `~/.q-data`。

数据流水线使用固定的 `state/data-pipeline.lock` 实现数据根单写者：进程内使用可重入
锁支持 `bootstrap`、`update` 对阶段入口的嵌套调用，进程间使用 Windows 文件字节范围
锁阻止 CLI、Dashboard Worker 等执行者重叠。争用时立即以可重试错误
`DATA_PIPELINE_ALREADY_RUNNING` 失败，不排队等待。锁文件本身可以永久存在，其存在不
表示有执行者；所有权只由操作系统锁决定，进程退出或崩溃后由 Windows 自动释放。系统
不保存 PID、owner token，不探测进程存活，也不执行陈旧锁回收。Raw 与 Canonical 的内容
寻址、临时文件和原子重命名继续负责单次发布的崩溃一致性。

## 2. Canonical 数据集

| 数据集 | 说明 | 主键 |
|---|---|---|
| `instrument` | 证券主数据、交易所、板块及上市生命周期 | `instrument_id` |
| `trade_calendar` | 市场开市日和休市日 | `trade_date` |
| `daily_bar` | 未复权日频 OHLCV 和成交额 | `instrument_id, trade_date` |
| `daily_basic` | 日频估值和换手指标 | `instrument_id, trade_date` |
| `security_status` | 上市、停牌、ST 和可交易状态 | `instrument_id, trade_date` |
| `financial_observation` | BaoStock Dupont PIT 财务指标及供应商重述记录 | `instrument_id, report_period, metric, revision` |
| `industry_classification` | BaoStock 按历史查询日期重建的行业状态事件 | `as_of_date, instrument_id, taxonomy` |
| `index_bar` | 指数日频 OHLCV 和成交额 | `index_id, trade_date` |

### 2.1 通用审计列

每个 Canonical 数据集都以以下五列结尾：

| 字段 | 类型 | 含义与作用 |
|---|---|---|
| `source` | `String` | 生成该证据的数据供应商，例如 `baostock`；用于数据血缘追踪和故障分析。 |
| `available_at` | `Datetime(us, UTC)` | 从业务角度看，该信息最早可以用于研究或交易决策的时间。PIT 查询只允许读取不晚于指定 `as_of` 日期边界的数据。 |
| `availability_source` | `String` | `available_at` 的确定依据，例如供应商公告时间或有文档说明的市场时间规则。 |
| `pit_usable` | `Boolean` | 可用时间证据是否足以支持 PIT 研究。值为 false 的记录仍保留以供审计，但不能参与 PIT 决策。 |
| `ingested_at` | `Datetime(us, UTC)` | 本系统抓取该证据的时间，仅用于运行血缘追踪，不能替代 `available_at`。 |

对于只接收日期的研究接口，`as_of=2026-04-30` 表示“上海本地日历日期不晚于 2026-04-30 的信息”，不表示 `15:00` 的决策时点。盘中决策时点必须使用接收时间戳的接口。

### 2.2 完整业务字段

以下各表列出业务字段；每个表尾部还包含上述五个通用审计列。可执行的权威定义位于 `src/quant_research/data/schemas.py` 中的 `CANONICAL_SCHEMAS`。

#### `instrument`

| 字段 | 类型 | 定义 |
|---|---|---|
| `instrument_id` | `String` | Canonical 证券代码，例如 `600000.SH`。 |
| `exchange` | `String` | 交易所标识。 |
| `board` | `String` | `MAIN`、`CHINEXT`、`STAR` 或其他已登记板块。 |
| `name` | `String` | 该条记录对应的证券名称。 |
| `instrument_type` | `String` | 股票、ETF、指数或其他受支持类型。 |
| `listing_status` | `String` | 当前、已上市或已退市等生命周期状态。 |
| `list_date` | `Date` | 首次上市日期。 |
| `delist_date` | `Date` | 退市日期；仍处于上市状态时为空。 |

#### `trade_calendar`

| 字段 | 类型 | 定义 |
|---|---|---|
| `trade_date` | `Date` | 上海市场日历日期。 |
| `is_trading_day` | `Boolean` | 当日是否开市。 |

Localize 始终抓取至最新完整交易日之后 90 个日历日，使交易日历可以支持未来调度和下一交易日解析；该前瞻范围不扩大日行情的抓取窗口。

#### `daily_bar`

| 字段 | 类型 | 定义 |
|---|---|---|
| `instrument_id` | `String` | Canonical 证券代码。 |
| `trade_date` | `Date` | 交易日期。 |
| `open`, `high`, `low`, `close`, `preclose` | `Float64` | 未复权价格。 |
| `volume` | `Int64` | 成交量；停牌占位记录可以为空。 |
| `amount` | `Float64` | 成交额；停牌占位记录可以为空。 |
| `adjustment_flag` | `String` | Raw 请求使用的供应商复权模式；Canonical 日线本身为未复权数据。 |
| `pct_change` | `Float64` | 供应商提供的日涨跌幅。 |

BaoStock 全市场 A 股日线接口不返回 ETF。Localize 因此将白名单 ETF 通过按证券
区间接口写入独立的 `query_etf_history_k_data_plus` Raw 端点目录；Curate 再把 ETF
行情合并进 `daily_bar`，把 ETF 停牌和可交易状态合并进 `security_status`。指数基准
仍只从 `index_bar` 读取。

#### `daily_basic`

| 字段 | 类型 | 定义 |
|---|---|---|
| `instrument_id` | `String` | Canonical 证券代码。 |
| `trade_date` | `Date` | 观测日期。 |
| `pe_ttm` | `Float64` | 滚动市盈率。 |
| `pb_mrq` | `Float64` | 最近季度市净率。 |
| `ps_ttm` | `Float64` | 滚动市销率。 |
| `turnover` | `Float64` | 日换手率。 |

#### `security_status`

| 字段 | 类型 | 定义 |
|---|---|---|
| `instrument_id` | `String` | Canonical 证券代码。 |
| `trade_date` | `Date` | 状态日期。 |
| `is_listed` | `Boolean` | 当日是否处于上市状态。 |
| `is_suspended` | `Boolean` | 当日是否停牌。 |
| `is_st` | `Boolean` | 是否处于风险警示状态。 |
| `board` | `String` | 市场规则使用的板块。 |
| `price_limit_rule_id` | `String` | 适用的涨跌停规则标识。 |
| `tradable_reason` | `String` | 确定性的可交易性原因。 |

#### `financial_observation`

| 字段 | 类型 | 定义 |
|---|---|---|
| `instrument_id` | `String` | Canonical 证券代码。 |
| `report_period` | `Date` | 财务报告期末日期。 |
| `metric` | `String` | Dupont 指标标识：`dupont_roe`、`dupont_assets_to_equity`、`dupont_asset_turn`、`dupont_pnitoni`、`dupont_nitogr`、`dupont_tax_burden`、`dupont_interest_burden` 或 `dupont_ebit_to_gr`。 |
| `value` | `Float64` | 指标数值。 |
| `revision` | `Int64` | 供应商财务重述序号。它表示业务重述，而不是系统版本，因此继续保留。 |
| `announced_at` | `Datetime(us, UTC)` | 供应商公告时间。 |

`financials_as_of(fields, as_of, instruments)` 先筛选在指定日期前已可用且 `pit_usable=true` 的记录，再为每个业务键选择当时已知的最新 `revision`。

`financial_history(fields, as_of, instruments)` 使用相同的上海日终可见性与 `pit_usable=true` 过滤，但保留截止时点可见的全部 revision，不做业务键折叠。该接口用于按披露事件重建历史 PIT 状态；研究代码不得用最终修订值回填早期信号日。

#### `industry_classification`

行业数据从来源语义、Raw、年度事件压缩到跨业务消费的完整契约见[行业分类 PIT 整体设计](industry-classification-pit-design.md)。本节保留数据层必须直接可见的 Schema 与核心不变量。

| 字段 | 类型 | 定义 |
|---|---|---|
| `as_of_date` | `Date` | BaoStock 请求参数 `date=D`；供应商重建快照和事件的可见日期。 |
| `supplier_update_date` | `Date` | BaoStock `updateDate` 原值；仅作供应商状态元数据，不解释为发布时间。 |
| `instrument_id` | `String` | Canonical 证券代码。 |
| `taxonomy` | `String` | BaoStock `industryClassification` 原值；当前接口返回“证监会行业分类”。 |
| `industry_code` | `String`，可空 | BaoStock `industry` 原值；显式未分类事件为空。 |
| `industry_name` | `String`，可空 | 当前与 `industry_code` 保持相同的供应商原值；显式未分类事件为空。 |
| `is_classified` | `Boolean` | 是否具有有效行业；供应商明确返回空行业时为 false。 |

行业数据唯一来自 BaoStock `query_stock_industry(code="", date=...)`。该接口每只证券只返回一条行业记录，且没有选择分类体系的请求参数；系统不得把字段存在误解为接口同时提供多套分类。本项目已验证的最新及代表性历史日期响应均为“证监会行业分类”，不提供可筛选的申万一级行业。

2026-08-16 的只读来源门禁验证了三个已知变化窗口：2023-12-22/25/26、2024-12-27/30/31、2025-12-26/29/30。每组均为变化日前旧哈希、变化日新哈希、后一交易日保持新哈希，且所有响应满足 `updateDate <= 请求日期`。例如 2024 窗口的行数为 5405/5407/5407，后两日响应哈希相同。该证据支持“供应商按请求日期重建 as-of 状态”，不证明历史响应永不重述，也不把 `updateDate` 解释为发布时间。

Localize 复用交易日历，为每个已完整结束的交易日保存一份全市场 Raw 快照，并在行业端点每行合成 `as_of_date`；其他 Raw 端点不统一增加该列。显式未来或未完整交易日请求被拒绝。`--full` 会重新请求完整窗口内已有的兼容 Raw 当前头。历史遗留未来 Raw 保持不可变，但在日期完整结束前不得进入 Curate 候选分区或输入身份。

Canonical 按 `year=as_of_date.year` 分区。每年第一份完整快照形成全市场基线；后续仅保存新增证券、行业代码或名称变化以及分类状态变化。明确空行业生成 `is_classified=false` 的 tombstone；证券整行缺失不会清除旧状态。事件日期始终取首次出现该状态的 `as_of_date`，不得回写到较早的 `supplier_update_date`。`available_at` 为 `as_of_date` 的上海日终，`availability_source` 固定为 `BAOSTOCK_AS_OF_DATE_RECONSTRUCTED`，`ingested_at` 只保留本机抓取血缘。Raw 当前头重述会重建请求年份、切换目录身份并关闭全局验证门禁。

所有业务只通过 `CanonicalResearchRepository` 使用行业数据。单日入口 `industry_classifications_as_of` 和批量入口 `industry_classifications_on_dates` 共用同一事件重建内核，保留最新 tombstone；批量入口一次读取全部请求日期，避免 N+1 扫描。每日复盘区分“此前没有快照”和“有快照但零覆盖/全为 tombstone”。因子、策略、组合、回测和归因仅在显式声明行业依赖、taxonomy 与未分类策略时启用：信号或调仓使用决策日状态，回测沿用历史决策状态，归因使用各归因日状态。默认未分类策略为排除；显式 `UNCLASSIFIED` 分组必须进入配置哈希。所有行业产物披露 taxonomy、日期口径、覆盖率、未分类策略和供应商重述风险，并继续受 `catalog_hash` 漂移门禁约束。

#### `index_bar`

| 字段 | 类型 | 定义 |
|---|---|---|
| `index_id` | `String` | Canonical 指数代码。 |
| `trade_date` | `Date` | 交易日期。 |
| `open`, `high`, `low`, `close`, `preclose` | `Float64` | 指数价格。 |
| `volume` | `Int64` | 指数成交量。 |
| `amount` | `Float64` | 指数成交额。 |
| `pct_change` | `Float64` | 日涨跌幅。 |

BaoStock 默认采集国证A指、上证50、沪深300、中证500和中证1000。已有数据根新增指数后必须显式回补至少 21 个交易日，再执行 Curate 与全局校验；回补前市场全景只将缺失指数显示为空，不阻断其他指数和市场统计。国证A指覆盖沪深北交易所的非 ST、非 *ST A 股，作为 BaoStock 可得的全市场基准；BaoStock 不提供中证全指 `000985` 历史行情。

## 3. LOCALIZE

Localize 只执行供应商 I/O，不读取或校验此前已完成请求对应的 Parquet 和 manifest。普通增量断点过滤会使用 SQLite 中的 `schema_fingerprint` 与当前端点契约做纯元数据比较；指纹匹配的当前头跳过网络访问，指纹不匹配的当前头视为未完成并重新抓取。显式 `--full` 则绕过兼容当前头并重新请求解析窗口中的全部业务单元。财务数据唯一使用 BaoStock `query_dupont_data`，请求按证券生命周期过滤，只请求报告期末不早于上市日、且不晚于退市日的季度；不抓取 profit、cash-flow、operation、growth 或 balance 端点。每个实际处理的 Raw 操作最终只输出一条 `localize.raw_completed` 日志。

Raw 文件布局：

```text
raw/source=<source>/endpoint=<endpoint>/<request_hash>/<content_hash>.parquet
raw/source=<source>/endpoint=<endpoint>/<request_hash>/manifest.json
```

`request_hash` 是 Canonical 请求 JSON 的 SHA-256；`content_hash` 是确定性 Arrow IPC 字节的 SHA-256；`schema_fingerprint` 是规范化 Raw Arrow Schema 的 SHA-256，用于独立于数据值检测端点契约变化。财务接口的空响应记录为 `empty_discarded`，不写入文件和 SQLite。

### SQLite：`raw_request`

主键：`(source, endpoint, request_hash)`。

| 列 | 类型 | 可空 | 作用 |
|---|---|---:|---|
| `source` | `VARCHAR(64)` | 否 | 数据供应商。 |
| `endpoint` | `VARCHAR(128)` | 否 | 供应商 API。 |
| `request_hash` | `VARCHAR(64)` | 否 | 稳定的请求身份。 |
| `request_json` | `TEXT` | 否 | 包含全部请求参数的 Canonical 请求。 |
| `current_content_hash` | `VARCHAR(64)` | 否 | 断点过滤使用的当前 Raw 对象指针。 |
| `updated_at` | `VARCHAR(32)` | 否 | UTC 更新时间。 |

### SQLite：`raw_object`

主键：`(source, endpoint, request_hash, content_hash)`；通过外键关联 `raw_request`，删除规则为 `RESTRICT`。

| 列 | 类型 | 可空 | 作用 |
|---|---|---:|---|
| `source`, `endpoint`, `request_hash` | 同上 | 否 | 所属 Raw 请求。 |
| `content_hash` | `VARCHAR(64)` | 否 | Raw 内容身份。 |
| `data_path`, `manifest_path` | `TEXT` | 否 | 物理文件路径。 |
| `schema_fingerprint` | `VARCHAR(64)` | 否 | Raw Schema 身份。 |
| `row_count` | `INTEGER` | 否 | 非负行数。 |
| `retrieved_at`, `created_at` | `VARCHAR(32)` | 否 | 供应商响应时间和目录登记时间。 |

## 4. CURATE

Curate 先使用 SQLite 中的 Raw 当前头元数据计算分区输入身份。输入未变化且当前 Canonical 文件存在的分区直接复用，不读取或校验 Raw Parquet；输入变化、文件丢失或显式请求 `--full` 的分区，使用该分区关联的全部 Raw 当前头从零重建。财务分区还会读取这些当前头所属请求的全部 Schema 兼容历史 Raw 对象，完全相同的连续观测合并，值或可用时间等业务状态发生变化时按抓取顺序追加 `revision`。重建结果写入内容寻址 Parquet并校验，最后在一个事务内确认 Raw 当前头未变化、切换数据集当前指针并更新增量检查点。

各数据集的 Raw request 到 Canonical 分区映射如下：

| 数据集 | request 字段 | Canonical 分区 |
|---|---|---|
| `daily_bar`、`daily_basic`、`security_status` | `date` 或 ETF Raw 的 `start_date/end_date` | 请求覆盖的每个 `year=<year>` |
| `index_bar` | `date` 或 `start_date/end_date` | 请求覆盖的每个 `year=<year>` |
| `financial_observation`（仅 `query_dupont_data`） | `report_year` | `report_year=<report_year>` |
| `industry_classification` | `as_of`/`date` | `year=<as_of_date.year>` |
| `instrument`、`trade_calendar` | 完整请求范围 | `all` |

一次 `curate-all` 对相同 Raw 当前头只执行一次校验和 Mapper；日行情映射结果同时分发到三个 fan-out 数据集。多个 Raw 时间窗包含相同 Canonical 主键时，按照 `retrieved_at`、endpoint、request hash 的确定性顺序保留最新响应。单个 Raw 响应内部出现重复主键仍是数据错误。

Canonical 文件布局：

```text
canonical/dataset=<dataset>/<partition-key>/<content_hash>.parquet
```

失去全部引用的旧文件会自动删除。Windows 文件占用导致删除失败时不影响本次 Curate，后续 Curate 会再次尝试清理该孤立文件。

哈希层级：

```text
partition content_hash = SHA256(确定性 Arrow IPC 字节)
dataset content_hash   = SHA256(dataset/source/日期范围以及按分区键排序的
                                partition key/content/schema/row count 的 Canonical JSON)
data_hash              = SHA256(按数据集名称排序的 dataset/content hash 的 Canonical JSON)
```

所有内容身份都不包含物理路径。

分区输入检查点：

```text
input_hash = SHA256(dataset、partition key、Curate transform hash，以及按 Raw 请求身份
                    排序的 source/endpoint/request/content/schema/retrieved_at 元数据)
```

Curate transform hash 由 Mapper 源码、Curate 规划和发布源码、Canonical Schema、分区规则与复用语义计算。实现或 Schema 变化会自动使相关分区的输入检查点失效。`input_hash` 是计算断点，不属于数据内容身份，不进入 partition content hash、dataset content hash 或 `data_hash`。因此 Raw 输入发生变化但 Canonical 内容相同时，只更新检查点，不关闭已经通过的 Validate 门禁。

### SQLite：`canonical_dataset`

| 列 | 类型 | 可空 | 作用 |
|---|---|---:|---|
| `dataset` | `VARCHAR(64)` PK | 否 | Canonical 数据集。 |
| `content_hash` | `VARCHAR(64)` | 否 | 当前数据集内容身份。 |
| `source` | `VARCHAR(64)` | 否 | 数据供应商。 |
| `start_date`, `end_date` | `VARCHAR(10)` | 是 | 当前业务日期覆盖范围。 |
| `updated_at` | `VARCHAR(32)` | 否 | 当前指针更新时间。 |

### SQLite：`canonical_partition`

主键：`(dataset, partition_key)`；删除数据集时级联删除分区元数据。

| 列 | 类型 | 可空 | 作用 |
|---|---|---:|---|
| `dataset` | `VARCHAR(64)` FK | 否 | 所属当前数据集。 |
| `partition_key` | `VARCHAR(64)` | 否 | 逻辑分区身份。 |
| `ordinal` | `INTEGER` | 否 | 确定性的读取顺序。 |
| `content_hash` | `VARCHAR(64)` | 否 | 当前分区内容身份。 |
| `path` | `TEXT` | 否 | 当前 Parquet 路径。 |
| `schema_fingerprint` | `VARCHAR(64)` | 否 | 精确的 Arrow Schema 身份。 |
| `input_hash` | `VARCHAR(64)` | 否 | 当前分区已经消费的 Raw 当前头和 Curate transform 聚合身份；仅用于增量判断。 |
| `row_count` | `INTEGER` | 否 | 行数。 |

Canonical 文件写入完成后，SQLite 事务会重新读取所选 endpoint 的全部 Raw 当前头；若与规划快照不一致，本次指针切换以可重试的 `DATA_CURATE_INPUT_CHANGED` 失败。已经写入但没有获得引用的文件由后续 Curate 清理。`curate-all` 已完成的数据集已经持久化 `input_hash`，因此中途失败后重跑会跳过这些数据集的未变化分区。

## 5. VALIDATE

`validate <dataset>` 只创建诊断结果，不开启全局读取门禁。`validate-all` 校验精确的当前 `data_hash`，是唯一可以开启读取门禁的操作。如果校验期间 Canonical 指针发生变化，本次质量结果仍会记录，但不能开启门禁。

阻断规则：

| 规则 | 严重级别 | 触发条件 |
|---|---|---|
| `required_dataset_missing` | FATAL | 必需的基础数据集不存在当前指针。 |
| `required_dataset_empty` | FATAL | 必需的基础数据集行数为零。 |
| `trading_window_empty` | FATAL | 交易日历中不存在开市日。 |
| `canonical_schema` | FATAL | 分区 Schema 与 `CANONICAL_SCHEMAS` 不一致。 |
| `cross_partition_schema` | FATAL | 同一数据集不同分区的 Schema 不一致。 |
| `primary_key_duplicate` | FATAL | Canonical 主键重复。 |
| `required_value_null` | SEVERE | 必填值为空；`daily_basic.turnover` 在 `security_status.is_suspended=true` 的停牌日允许为空，非停牌或状态缺失时仍为错误。 |
| `positive_finite_price` | SEVERE | 已交易日线的 open、high、low、close 必须有限且严格大于零。 |
| `ohlc_relationship` | SEVERE | 最高价、最低价等 OHLC 关系不可能成立。 |
| `negative_volume` | SEVERE | 已交易日线的成交量为负数。 |
| `trading_day_coverage` | SEVERE | 日线在其最新可用日期以内缺少交易日历中的开市日期；交易日历的未来 90 日前瞻范围不要求存在日线。 |
| `instrument_coverage` | SEVERE | 日线引用了未知证券。 |
| `financial_availability` | SEVERE | PIT 财务记录缺少有效的公告或可用时间证据。 |

任意 SEVERE 或 FATAL 问题都会保持读取门禁关闭，并使 `bootstrap` 或 `update` 失败。

每次质量运行还会按“规则 × 适用数据集”记录完整执行结果：已执行且满足条件为
`PASS`，产生问题为 `FAIL`，因 Schema 或依赖数据集不可用而未执行为 `SKIPPED`。
规则结果保存标题、说明、通过条件、严重级别、actual、threshold、scope 和跳过原因的
运行时快照；`quality_issue` 继续保存失败问题及修复建议。结果状态不增加新的门禁条件，
门禁仍只由 SEVERE/FATAL issue 判定。

### SQLite：质量与门禁表

- `quality_run(id PK, scope, input_hash, status, results_complete, started_at, completed_at, created_at)`
- `quality_run_dataset(quality_run_id PK/FK, dataset PK, content_hash)`
- `quality_issue(id PK, quality_run_id FK, rule_id, severity, dataset, scope_json, actual_json, threshold_json, message, remediation)`
- `quality_rule_result(id PK, quality_run_id FK, dataset, rule_id, status, severity, title, description, pass_criterion, scope_json, actual_json, threshold_json, skip_reason, evidence)`；`(quality_run_id, dataset, rule_id)` 唯一。
- `data_catalog_state(id PK CHECK id=1, catalog_hash, validated_catalog_hash NULL, quality_run_id FK NULL, updated_at, validated_at NULL)`

迁移前创建的运行保持 `results_complete=false`。查询历史详情时，已有 issue 对应项只能
确认为 `FAIL`，其余适用规则显示 `UNKNOWN`，不得根据“没有 issue”推断为 `PASS`。

当前指针变化时，系统清空 `validated_catalog_hash`、`quality_run_id` 和 `validated_at`。只有在提交时仍满足 `quality_run.input_hash == data_catalog_state.catalog_hash`，成功的 `validate-all` 才能写入有效门禁状态。

## 6. 研究读取与实验

`CanonicalResearchRepository` 在每次查询前检查全局门禁，并在扫描分区前校验登记路径、内容哈希、Schema 指纹和行数。普通研究调用方只依赖 `ResearchDataRepository`，不得自行装配 `MetadataRepository`、具体仓库或复权服务；需要目录身份或日期覆盖时通过 `repository.catalog()` 使用只读 `CanonicalCatalog`。组合根使用 `CanonicalResearchRepository.from_sqlite(...)` 完成 SQLite 目录适配器装配，数据库迁移和 Engine 生命周期仍由组合根负责。

`bars(instruments, start, end)` 返回未复权 Canonical 行情；`adjusted_bars(instruments, start, end)` 返回前复权行情；`log_returns(instruments, start, end, lookback_sessions=...)` 返回按交易会话补齐的前复权对数收益。三个接口都将 `end` 作为 PIT 信息截止日期，不接受独立 `as_of`，因此不得使用区间结束后的信息重算历史结果。

```python
from quant_research.data.repository import CanonicalResearchRepository

research = CanonicalResearchRepository.from_sqlite(
    engine,
    trusted_curated_root=settings.curated_root,
)
catalog = research.catalog()
bars = research.bars(instruments, start, end)
adjusted = research.adjusted_bars(instruments, start, end)
returns = research.log_returns(
    instruments,
    start,
    end,
    lookback_sessions=120,
)
financials = research.financials_as_of(fields, as_of, instruments)
```

实验 YAML 使用明确日期和唯一策略 ID：

```yaml
strategy_id: etf_rotation
start_date: 2024-01-02
end_date: 2024-12-31
benchmark: 000300.SH
initial_cash_fen: 100000000
strategy_config: {}
```

实验提交时持久化 `data_hash`、`config_hash`、源码或 Git 哈希、锁文件哈希和规则文件哈希。每个实验阶段都会检查当前已通过校验的 `catalog_hash` 是否仍等于提交时记录的 `data_hash`；不一致时以 `EXPERIMENT_DATA_DRIFT` 终止。

## 7. CLI

```text
quant data localize <dataset> [--from YYYY-MM-DD --to YYYY-MM-DD] [--full]
quant data localize-all [--from YYYY-MM-DD --to YYYY-MM-DD] [--full]
quant data curate <dataset> [--from YYYY-MM-DD --to YYYY-MM-DD] [--full]
quant data curate-all [--full]
quant data validate <dataset>
quant data validate-all
quant data bootstrap
quant data update [--start YYYY-MM-DD --end YYYY-MM-DD]
```

`localize-all` 在完整的数据集处理序列中只保持一个供应商会话。单独执行 `localize` 时，每次命令拥有一组 login/logout。不可重试的登录拒绝会在发出任何数据请求前终止命令。

`bootstrap` 和 `update` 都以 `validate-all` 结束，并返回 `run_id`、`quality_run_id` 和 `data_hash`。CLI 不提供 `snapshot` 或 `snapshots` 命令。

Dashboard 创建 `DATA_UPDATE` 任务前必须通过供应商实时交易日历和当前 Canonical 水位生成计划预览。用户可以选择非空的可执行数据集子集，省略选择时仍表示全部数据集；计划按名称稳定排序，并只根据所选数据集的水位固化实际开始日、结束日、窗口依据、水位和重叠天数。共享供应商端点可以复用 Raw 请求，但不得因此把未选 Canonical 数据集加入发布范围。计划以排除生成时间的确定性 `plan_hash` 校验预览与提交的一致性。任务入库和 Worker 执行使用同一计划；Worker 不得在领取任务后重新解析自动窗口，重试也必须复用原计划。自动模式不在 payload 中保存空 `start/end`。部分更新只对所选数据集执行 LOCALIZE 和 CURATE，随后仍执行 `validate-all`，只有精确的完整目录通过校验才重新开放研究门。

Dashboard 还可以创建独立 `DATA_VALIDATION` 后台任务。范围只允许全目录或单个数据集：全目录运行沿用 `validate-all` 语义，通过时重新绑定研究门，发现阻断问题时登记完整质量证据并使任务失败；单数据集运行只登记诊断结果，不改变研究门，任务成功仅表示校验过程完成。相同范围的活动任务按幂等键收敛，终态后允许再次创建；Worker 在数据集和质量规则边界响应协作取消。

Curate 命令默认执行分区级增量检查。`--from/--to` 只选择需要检查的业务年份，但选中年份需要重建时会完整重建整个 Canonical 分区；`all` 分区始终整体处理。`--full` 强制重建全部选中分区。结果包含 `rebuilt_partitions`、`reused_partitions` 和 `raw_inputs_read`，用于直接观察增量执行效果。

命令日志使用统一的 JSON Lines 外层字段（时间、级别、事件、阶段和关联 ID），但不使用统一的业务 `context`。每个命令和阶段定义自己的业务日志结构：

- Localize 的核心对象是供应商请求。每个实际 Raw 操作最终输出一条 `localize.raw_completed`，包含完整 `request`、dataset、source、endpoint、disposition、request/content hash、schema fingerprint、行数、抓取时间和落盘路径；失败事件包含同一请求及供应商错误。
- Curate 的核心对象是 Canonical 分区，不要求或复制 Raw request，也不记录 Raw content hash 或 `input_hash`。`curate.partition_completed` 包含 dataset、source、run ID，以及 `partition` 对象中的分区键、写入处置、重建原因、关联 Raw 输入数量、Canonical content hash、schema fingerprint、行数、路径和重建前状态。该事件只在 Canonical 当前指针事务完成后输出。
- Validate 的核心对象是精确的 catalog、数据集和质量规则。日志包含 scope、catalog hash、完整数据集/分区元数据和规则组；每个适用组合输出 `validate.rule_evaluated`，包含 rule ID、severity、dataset、status、scope、actual、threshold 和 skip reason；每个问题继续输出 `validate.issue_detected` 及修复建议；质量运行提交及读取门禁开启分别记录完成事件。
- 其他 CLI 命令同样按自身业务对象定义 context。例如任务命令记录完整 task payload、attempt、progress 和 outcome，实验任务通过 experiment/task/attempt 关联 ID 串联，不为满足统一格式伪造 request。

日志结构以事件语义为契约；新增命令应记录可复现输入、业务身份、关键写入结果、内容哈希、数量、时间和错误信息，同时继续由 `StructuredLogger` 统一执行敏感信息脱敏。

`StructuredLogger` 采用带写锁的缓冲追加：锁只保证并发 JSON Lines 不交错，逐条
`emit()` 不调用 `flush` 或 `fsync`。Pipeline 主文件和 stderr 镜像只在命令服务正常
关闭边界显式刷新；进程崩溃时允许丢失尚未刷新的日志。sink 的写入、刷新和关闭异常
属于诊断能力降级，不得覆盖命令或任务的业务结果；日志参数和结构校验错误仍按契约
抛出。

任务日志在显式 `TaskLogSession.flush()`、封存、物化和正常关闭边界尝试
`flush+fsync`，这些 I/O 失败同样按最佳努力处理。日志路径只由 `task_log_root`、数据库
中的 task ID 和 attempt ID 确定性推导，不使用能力凭证或文件 `dev/inode` 身份校验。
路径越界、重解析点、Worker 所有权、根目录配置不一致和日志关联字段冲突仍必须阻断。
成功实验无法取得合法真实任务日志时，
必须在 staging 中生成一条 `level=WARNING`、`event=task.log_unavailable` 的结构化
占位 `run.log`，保留 experiment、task、attempt、worker 和 stage 标识，不记录原始异常
文本。占位日志继续参与 manifest、哈希和最终产物验证；若占位文件本身无法写入，则按
整体产物存储失败处理。

## 8. 数据阶段之外的最终 SQLite 表

唯一的初始化迁移还会创建以下表：

- `experiment(id, strategy_id, config_json, config_hash, data_hash, source_tree_hash, git_commit_hash, lockfile_hash, rulebook_hash, fingerprint, status, research_mark, created_at, queued_at, started_at, completed_at)`
- `experiment_tag(experiment_id, tag)`
- `experiment_metric(id, experiment_id, name, value, unit, created_at)`
- `experiment_artifact(id, experiment_id, name, artifact_type, path, content_hash, metadata_json, created_at)`
- `task(id, experiment_id, task_type, payload_json, status, priority, progress_json, created_at, available_at, updated_at, heartbeat_at, completed_at, idempotency_key, worker_id, locked_at, error_json)`
- `task_attempt(id, task_id, attempt_no, status, worker_id, started_at, heartbeat_at, completed_at, log_path, progress_json, error_json)`
- `audit_event(id, experiment_id, task_id, event_type, actor, details_json, created_at)`

应用 JSON、YAML 和 manifest 只接受当前唯一结构，不包含 Schema、格式或指标版本字段。Python、依赖、包版本以及唯一 Alembic revision 等基础设施版本仍会记录。
