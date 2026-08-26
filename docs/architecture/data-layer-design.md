# 数据层设计：从 Tushare 到可研究数据

本文是本项目数据采集、清洗、校验和研究读取的权威设计，也是写给第一次接触量化研究和数据工程的读者的入门说明。

读完本文后，你应该能回答以下问题：

- 为什么不能把 Tushare 返回的数据直接交给策略？
- Raw、Canonical、Parquet、分区和 Schema 分别是什么？
- 股票、场内基金和指数为什么使用不同的标识与读取接口？
- 什么是复权、`preclose`、PIT 和未来函数？
- `LOCALIZE → CURATE → VALIDATE` 每一步做什么？
- 一次更新怎样做到可重试、可审计，并且不会让研究读到半成品？
- 数据发生变化时，实验为什么能够发现数据漂移？

## 1. 数据层解决什么问题

量化研究的输入不是“几个能下载的表格”，而是一套必须保持一致的数据事实。

例如，我们想计算某只股票 2026-08-25 的收益率。仅有收盘价还不够，还需要知道：

- 代码是否正确，证券当时是否已经上市；
- 当日是不是交易日，股票是否停牌；
- 昨收价是否已经正确处理除权除息；
- 涨跌幅字段是 `2.5` 还是 `0.025`；
- 财务指标在当时是否已经公告；
- 本次回测期间数据是否被后台更新过。

外部供应商负责提供数据，数据层负责把这些数据变成项目内部稳定、可验证、可追踪的研究事实。它有四个核心目标：

1. **统一语义**：供应商字段、日期、单位和代码转换为项目约定。
2. **防止未来函数**：研究只能看到观察时点已经公开的数据。
3. **保证一致性**：研究不会读到更新到一半或未经校验的数据。
4. **记录身份**：每个文件和整套数据都有哈希，可以判断数据是否变化。

## 2. 先认识几个基础概念

### 2.1 端点与数据集

**端点（endpoint）**是供应商提供的查询接口，例如 Tushare 的 `daily`。

**数据集（dataset）**是项目内部具有固定含义、字段和主键的一组数据，例如 `stock_daily_bar`。端点属于外部世界，Canonical 数据集属于本项目的领域模型。

本项目坚持“一端点一数据集”。这样可以避免把多个来源、不同发布时间和不同更新规律的数据强行拼成一张难以解释的大表。

### 2.2 行、列、Schema 与主键

- **行**表示一条记录，例如某只股票某个交易日的行情。
- **列**表示一个属性，例如 `close` 或 `volume`。
- **Schema**规定列名、顺序和数据类型。
- **主键**是一组能够唯一确定一行的列。

例如，股票日行情的主键是：

```text
(instrument_id, trade_date)
```

同一只股票同一天出现两行，就是主键重复，通常意味着抓取或清洗存在错误。

### 2.3 快照、时间序列与事件

- **快照**：某个时点看到的完整状态，例如全部股票基础信息。
- **时间序列**：按时间持续追加的观察，例如每日行情。
- **事件**：只在事情发生时出现的记录，例如停牌或风险警示。

事件表中“没有记录”通常表示事件没有发生，而不是应该人为补一行 `False`。

### 2.4 Parquet、SQLite 与分区

**Parquet**是适合分析型数据的列式文件格式。行情和财务数据存为 Parquet，便于 Polars、DuckDB 等工具只读取需要的列和分区。

**SQLite**不保存大规模行情，而是保存控制信息，例如：

- 哪些 Raw 请求已经成功；
- 当前 Canonical 分区指向哪个文件；
- 数据集和整套目录的哈希；
- 质量运行是否通过。

**分区**是把一个大数据集拆成多个文件。本项目的日行情按年分区，因此查询 2026 年数据时不必扫描所有历史年份。

### 2.5 Instrument、Index 与 Security

`security` 是金融语境中的上位概念，可以泛指股票、基金、债券和其他证券；它不等于“可以直接下单的资产”。

本项目没有使用一个宽泛的 `SecurityId` 承担所有职责，而是明确区分：

- `InstrumentId`：可直接交易的股票和场内基金，支持 `.SH`、`.SZ`、`.BJ`；
- `IndexId`：不可直接交易的指数，只用于市场参考和 benchmark。

例如：

```text
600000.SH  -> InstrumentId，浦发银行，可以进入订单和持仓
510300.SH  -> InstrumentId，场内 ETF，可以进入订单和持仓
000300.SH  -> IndexId，沪深 300，只能作为指数或 benchmark
```

代码字符串可能长得相似，领域含义却不同。组合、订单和持仓只接受 `InstrumentId`，从类型上阻止把沪深 300 指数当成可买卖资产。

## 3. 总体架构

数据从供应商到研究代码只沿着一条路径流动：

```text
Tushare
   │
   ▼
LOCALIZE：保存供应商原始响应
   │
   ▼
Raw Parquet ──────────────┐
   │                      │ 请求、文件和哈希登记到 SQLite
   ▼                      │
CURATE：改名、转换、排序、分区
   │                      │
   ▼                      │
Canonical Parquet ────────┘
   │
   ▼
VALIDATE：Schema、主键、数值、覆盖和 PIT 校验
   │
   ▼
全局研究读取门禁
   │
   ▼
CanonicalResearchRepository
   │
   ├── 股票池与 ETF 池
   ├── 因子与因子研究
   ├── 策略与回测
   └── Dashboard
```

固定流水线为：

```text
LOCALIZE → CURATE → VALIDATE
```

研究代码不得绕开 Repository 直接扫描 Raw 或 Canonical 目录。否则它可能读到未校验分区、旧文件、更新中间态或错误的 PIT 数据。

## 4. Canonical 数据集目录

Canonical 表示“项目内部唯一认可的标准形式”。以下 15 个数据集构成当前数据目录。

| Canonical 数据集 | Tushare 端点 | 内容与量化用途 | 采集粒度 | Canonical 分区 |
|---|---|---|---|---|
| `stock_master` | `stock_basic` | 股票名称、上市状态、板块和上市日期；构建股票候选集合 | 按上市状态的全市场快照 | `all` |
| `fund_master` | `fund_basic` | 全部场内基金基础信息；构建 ETF 等基金候选集合 | 全市场快照 | `all` |
| `index_master` | `index_basic` | 指数名称、市场和基日；解释 benchmark | 按指数市场的全量快照 | `all` |
| `trade_calendar` | `trade_cal` | 判断某天是否开市以及前一交易日 | 交易所日期区间 | `all` |
| `stock_daily_bar` | `daily` | 股票 OHLC、昨收、成交量和成交额 | 每个交易日一次全市场请求 | `year=<YYYY>` |
| `stock_adjustment_factor` | `adj_factor` | 股票本地复权所需因子 | 每个交易日一次全市场请求 | `year=<YYYY>` |
| `fund_daily_bar` | `fund_daily` | 场内基金每日行情 | 每个交易日一次全市场请求 | `year=<YYYY>` |
| `fund_adjustment_factor` | `fund_adj` | 场内基金本地复权所需因子 | 每个交易日一次全市场请求 | `year=<YYYY>` |
| `index_daily_bar` | `index_daily` | 配置的 benchmark 指数行情 | 按基准指数代码和日期区间 | `year=<YYYY>` |
| `stock_daily_basic` | `daily_basic` | 估值、换手率、市值和股本等每日截面指标 | 每个交易日一次全市场请求 | `year=<YYYY>` |
| `stock_suspension` | `suspend_d` | 股票停牌事件 | 每个交易日一次全市场请求 | `year=<YYYY>` |
| `stock_risk_warning` | `stock_st` | ST 等风险警示事件 | 每个交易日一次全市场请求 | `year=<YYYY>` |
| `stock_financial_indicator` | `fina_indicator_vip` | ROE、利润率、偿债和成长等财务指标 | 每个报告期一次全市场请求 | `report_year=<YYYY>` |
| `industry_catalog` | `index_classify` | 申万 2021 行业层级与代码目录 | 一级行业全量快照 | `all` |
| `industry_membership` | `index_member_all` | 股票进入、退出各级行业的关系 | 按一级行业切片 | `all` |

### 4.1 为什么优先使用全市场端点

假设有 5,000 只股票。逐股票抓取会产生 5,000 次请求，失败恢复、限流和一致性处理都很复杂。逐交易日全市场抓取通常只需要一次请求，并具有三个优点：

- 请求数量更少，更新窗口容易重放；
- 同一交易日的全市场截面来自一致的供应商查询；
- 研究股票池不会反向决定我们保存哪些原始数据。

因此，除 `index_daily` 外，采集请求不得携带 `ts_code`。行业成员可以因行数限制按一级行业切片，但不能按股票切片。

ETF 策略池和股票池只在 Canonical 数据已经发布后过滤。例如策略只研究 20 只 ETF，也仍然采集全部场内基金行情。

代理与官方端点的单页上限并不完全一致。`fund_daily` 使用 `limit=5000`，
`fund_adj` 使用 `limit=2000`；两者都从 `offset=0` 开始，持续翻页到返回行数小于
单页上限。分页发生在一个逻辑 Raw 请求内部，最终合并为该交易日的一份完整 Raw
响应；每个真实页面仍独立经过限流和重试。合并时若发现空主键或跨页重复的
`(ts_code, trade_date)`，采集失败关闭，避免静默保存截断或漂移数据。

`index_member_all` 使用完整的申万一级行业代码（例如 `801010.SI`），并为每个行业
分别采集 `is_new=Y` 的当前成员与 `is_new=N` 的历史退出成员。这样
`industry_membership` 才能支持按任意历史日期重建行业归属。

当前代理的 `daily_basic` 不返回官方字段 `limit_status`。Raw 请求因此只声明代理实际
提供的 18 个字段；Canonical 仍保留可空的 `limit_status`，映射值为 `null`，以便未来
数据源恢复该字段时维持稳定研究 Schema。涨跌停判断继续只使用行情 `preclose`、交易
画像、ST 状态和 `MarketRuleBook`，不依赖 `limit_status` 形成第二套口径。

### 4.2 `index_daily` 为什么是例外

指数不可交易，并且项目只需要少量 benchmark。配置文件中的基准指数为：

```yaml
tushare:
  benchmark_indexes:
    - 399317.SZ
    - 000016.SH
    - 000300.SH
    - 000905.SH
    - 000852.SH
```

`index_daily` 是唯一允许携带 `ts_code` 的请求。客户端在运行时也会拒绝其他端点包含该参数。

## 5. LOCALIZE：保存原始事实

LOCALIZE 的职责是“把供应商当时返回了什么”完整保存下来，不在这一阶段决定研究语义。

以股票日行情为例，请求形态类似：

```text
endpoint = daily
request  = {trade_date: 20260825}
```

其中没有股票代码。一次响应包含该交易日的全市场股票行情。`daily` 单次最多返回
6,000 行；响应达到 6,000 行时系统必须按“可能被截断”失败，不能发布不完整数据。

### 5.1 Raw 为什么不能只保存最新文件

供应商可能修订历史数据，同一个请求在不同日期返回的内容可能不同。Raw 层同时记录：

- 请求内容；
- 请求时间；
- 请求哈希 `request_hash`；
- 响应内容哈希 `content_hash`；
- 字段列表、行数和发布文件。

相同请求和相同响应可以幂等复用；相同请求得到不同响应时，新内容会作为新的 Raw 对象保存，而不是覆盖旧文件。

### 5.2 Raw 路径

```text
$QUANT_DATA_ROOT/
└── raw/
    └── source=tushare/
        └── endpoint=daily/
            └── <request_hash>/
                └── <content_hash>.parquet
```

路径方便人和工具定位文件，但身份来自文件内容和确定性元数据，不来自绝对路径。因此把整个数据根移动到另一块磁盘，不应改变数据身份。

## 6. CURATE：生成稳定的 Canonical 数据

CURATE 从 Raw 读取数据，并执行以下步骤：

1. 检查端点和数据集的一对一映射；
2. 验证供应商字段没有静默漂移；
3. 重命名字段并转换日期、类型和单位；
4. 添加审计列；
5. 按稳定键排序；
6. 按年、报告年或 `all` 生成分区；
7. 计算输入和输出哈希；
8. 在临时目录完成写入和校验后原子发布；
9. 更新 SQLite 中唯一的当前分区指针。

“原子发布”可以理解为：读者只会看到旧版本或完整的新版本，不会看到写到一半的文件。

### 6.1 字段标准化

股票和基金使用：

```text
ts_code   → instrument_id
pre_close → preclose
pct_chg   → pct_change
vol       → volume
```

指数使用：

```text
ts_code → index_id
```

字段不是运行时随意猜测的。每个端点的完整输出字段在代码中显式声明，Canonical Schema 也显式规定列顺序和类型。供应商突然增加、删除或改变字段时，流水线应失败并要求开发者检查，而不是悄悄生成含义未知的数据。

### 6.2 单位标准化

同一个数字在不同单位下可能相差几百或几万倍。Canonical 尽量使用可直接计算的基础单位：

| 供应商语义 | Canonical 语义 | 示例 |
|---|---|---|
| 百分数 | 小数 | `2.5 → 0.025` |
| 成交量“手” | 股 | `100 手 → 10,000 股` |
| 成交额“千元” | 元 | `12 千元 → 12,000 元` |
| 金额“万元” | 元 | `3 万元 → 30,000 元` |
| 股本“万股” | 股 | `20 万股 → 200,000 股` |

策略和因子不得再次猜测或重复转换这些单位。

### 6.3 五个统一审计列

每个 Canonical 数据集都包含：

| 列 | 含义 |
|---|---|
| `source` | 数据供应商，当前固定为 `tushare` |
| `available_at` | 这条信息最早可被研究使用的时间 |
| `availability_source` | `available_at` 的证据来源，例如公告日期或采集时间 |
| `pit_usable` | 是否具备足够证据用于时点安全研究 |
| `ingested_at` | 本系统实际接收到数据的时间 |

`available_at` 和 `ingested_at` 不是同一个概念。供应商今天补发一条上周已经公告的数据时：业务上可能在上周就已公开，但本系统今天才采集到。系统必须根据端点能够提供的证据谨慎决定 `available_at`，证据不足时不能假装知道历史可用时间。

### 6.4 Canonical 路径

```text
$QUANT_DATA_ROOT/
└── canonical/
    └── source=tushare/
        └── dataset=stock_daily_bar/
            └── year=2026/
                └── <content_hash>.parquet
```

其他示例：

```text
canonical/source=tushare/dataset=stock_master/all/<hash>.parquet
canonical/source=tushare/dataset=stock_financial_indicator/report_year=2025/<hash>.parquet
canonical/source=tushare/dataset=industry_membership/all/<hash>.parquet
```

旧路径 `canonical/dataset=...` 和其他供应商命名空间不会迁移。检测到旧布局时，系统要求使用新的 `QUANT_DATA_ROOT` 并重新 bootstrap，避免一套目录里存在两种语义。

## 7. VALIDATE：在研究读取前做质量门禁

Canonical 文件成功写出不代表数据一定正确。VALIDATE 会检查数据契约和常见数据问题，例如：

- 必需数据集或分区是否缺失、是否为空；
- Schema、列顺序和类型是否符合声明；
- 主键是否重复，关键字段是否为空；
- OHLC 是否满足 `high ≥ open/close ≥ low`；
- 价格是否为正且为有限数；
- 成交量是否为负；
- `pct_change` 是否与 `close / preclose - 1` 一致；
- 股票和基金行情代码是否存在于相应 Master；
- 财务公告日期和可用时间是否合理；
- 行业进入、退出状态是否自洽。

两个命令的作用不同：

```text
quant data validate stock_daily_bar  # 诊断单个数据集
quant data validate-all              # 校验完整目录并决定是否开放研究读取
```

单数据集 `validate` 只用于诊断，不能打开研究门禁。只有完整 `validate-all` 通过，Repository 才允许研究代码读取当前目录。

这条规则避免出现“股票行情已更新，但复权因子或交易日历仍是旧数据”的混合状态。

## 8. 行情与收益率

### 8.1 OHLC 和昨收

日行情中常见字段为：

- `open`：开盘价；
- `high`：最高价；
- `low`：最低价；
- `close`：收盘价；
- `preclose`：用于当日涨跌幅计算的昨收价；
- `volume`：成交量；
- `amount`：成交额。

简单收益率和对数收益率统一计算为：

```text
pct_change = close / preclose - 1
log_return = log(close) - log(preclose)
```

例如，`preclose=10.00`、`close=10.25`：

```text
pct_change = 10.25 / 10.00 - 1 = 0.025
```

Canonical 中保存的 `0.025` 表示 2.5%。Tushare 的 `pct_chg=2.5` 会转换为 `0.025`，但它只用于交叉校验，研究收益仍由价格公式计算，避免形成两套口径。

### 8.2 为什么要复权

公司分红、送股或拆股可能让价格在除权日机械下降，但投资者财富并没有同比例损失。如果直接用未复权收盘价计算跨日收益，会把公司行动误认为市场暴跌。

本项目分别使用：

- `stock_daily_bar + stock_adjustment_factor` 计算股票复权价；
- `fund_daily_bar + fund_adjustment_factor` 计算基金复权价。

前复权以查询结束日的因子为基准。OHLC 使用当日因子，`preclose` 使用前一交易日因子，使除权日收益能够连续。项目不会调用逐证券的 `pro_bar`。

### 8.3 涨跌停

核心公式为：

```text
upper = round_to_tick(preclose × (1 + limit_rate))
lower = round_to_tick(preclose × (1 - limit_rate))
```

计算前还要确定股票当日交易画像：

- 主板普通股票通常为 10%；
- ST 等风险警示股票通常为 5%；
- 创业板、科创板和北交所对应 20% 或 30% 规则组；
- 上市初期等无涨跌幅限制日不计算上下限；
- 停牌股票不能因为没有成交而被判断为涨停或跌停；
- 指数不参与涨跌停判断。

规则集中在 `MarketRuleBook` 和 `configs/rules/a_share.yaml`，策略不得各自复制一套公式。

## 9. PIT：只使用当时已经知道的信息

PIT 是 Point-in-Time 的缩写，意思是“站在某个历史时点看，当时能知道什么”。

### 9.1 财务指标例子

一家公司的 2025 年年报报告期是 2025-12-31，但可能到 2026-03-30 才公告。

错误做法：在 2026-01-15 的回测中使用 2025 年年报数据。

正确做法：只有公告和可用时间不晚于研究观察时点，数据才可见。

`stock_financial_indicators(as_of=...)` 根据观察日过滤可见修订，而不是简单按报告期过滤。

### 9.2 行业成员例子

股票可能在历史上进入或退出某个行业。`industry_membership` 保存 `in_date`、`out_date` 及相应可用时间，`industry_memberships_on_dates(...)` 按查询日期重建当时有效的行业关系。

如果供应商只提供今天看到的全量关系，而无法证明过去何时公开，系统不能凭空制造历史知识。这种记录需要以谨慎的 `available_at` 和 `pit_usable` 表达证据边界。

## 10. 更新计划、新鲜度与增量重建

不同数据集的更新方式不同：

| 类别 | 更新频率 | 重用方式 | 回看窗口 |
|---|---|---|---|
| 股票、基金、指数 Master | 每日检查 | 全量刷新 | 0 天 |
| 交易日历 | 每日 | 追加并允许尾部修订 | 30 天 |
| 日行情、复权因子、每日指标和事件 | 每个交易日 | 追加并允许尾部修订 | 5 天 |
| 财务指标 | 季度披露触发 | 追加并允许历史重述 | 按报告期 |
| 行业目录和成员 | 每周 | 全量刷新 | 0 天 |

“回看 5 天”表示更新时不仅抓最新一天，也重新抓最近几天，以吸收供应商对尾部数据的修订。

CURATE 不会无条件重写全部历史。每个 Canonical 分区记录其 Raw 输入身份 `input_hash`：

- Raw 输入未变化，分区可以复用；
- Raw 输入变化，只重建受影响分区；
- 数据集声明为全量刷新语义时，才重建全部目标分区。

新鲜度不是简单比较电脑日期：

- 行情按最近完整交易会话判断；
- 交易日历需要覆盖未来规划窗口；
- 财务数据按披露截止日判断是否应该更新；
- 快照类数据按最近成功刷新时间判断。

## 11. 数据身份与可重复性

系统使用多层 SHA-256 身份回答“究竟什么发生了变化”：

| 身份 | 回答的问题 |
|---|---|
| `request_hash` | 这是不是同一个供应商请求？ |
| Raw `content_hash` | 供应商对该请求返回的内容是否变化？ |
| Canonical `input_hash` | 生成该分区的 Raw 输入集合是否变化？ |
| 分区 `content_hash` | 清洗后的分区内容是否变化？ |
| 数据集 `data_hash` | 该数据集当前全部分区的组合是否变化？ |
| `catalog_hash` | 15 个当前 Canonical 数据集的整体身份是否变化？ |

绝对路径不参与这些哈希。相同内容从 `D:` 盘移动到 `E:` 盘不会变成不同数据。

实验或因子运行提交时捕获当前 `catalog_hash`。运行阶段前后都会检查它：

```text
提交时 catalog_hash = A
运行中后台完成数据更新，catalog_hash = B
因为 A != B，本次运行以数据漂移错误失败
```

这比把更新前后的数据混在一次回测中更安全。项目没有可供选择的历史 Snapshot 或 `snapshot_id`；`catalog_hash` 是一致性身份，不是一个可回放版本仓库。

## 12. 研究读取接口

研究层通过 `CanonicalResearchRepository` 读取数据。基础实体接口为：

```text
stocks()
funds()
indexes()
```

行情接口为：

```text
stock_bars(InstrumentId, start, end)
fund_bars(InstrumentId, start, end)
index_bars(IndexId, start, end)
```

其他主要接口包括：

```text
adjusted_stock_bars(...)
adjusted_fund_bars(...)
stock_log_returns(...)
fund_log_returns(...)
stock_daily_basics(...)
stock_financial_indicators(as_of, ...)
stock_suspensions(...)
stock_risk_warnings(...)
industry_catalog()
industry_memberships_on_dates(...)
trade_calendar(...)
```

接口返回 Polars `LazyFrame`。可以把它理解为“尚未执行的查询计划”：Repository 先描述需要哪些列、日期和证券，调用 `.collect()` 时才真正读取 Parquet。这有助于减少不必要的磁盘扫描。

不存在通用 `instruments()`、`bars()`、`adjusted_bars()` 或 `security_status()`。拆分接口让调用者明确自己处理的是股票、基金还是指数，也避免把停牌和 ST 两种不同事件混成一个来源不清的状态表。

## 13. 各类消费者怎样组合数据

### 13.1 股票池

股票池不是供应商抓取过滤器。它在 Canonical 读取后显式组合：

```text
stock_master
  + stock_daily_bar
  + stock_suspension
  + stock_risk_warning
  + 规则与研究配置
  = 某日可研究股票池
```

### 13.2 ETF 策略

ETF 策略读取 `fund_master` 和 `fund_daily_bar`，再按策略配置过滤候选 ETF。ETF 属于可交易的场内基金，因此使用 `InstrumentId`。

### 13.3 Benchmark

实验配置中的 benchmark 在解析时转换为 `IndexId`。回测通过 `index_bars()` 单独读取指数收盘价，不把指数插入可交易 MarketSlice，也不允许进入订单或持仓。

### 13.4 Dashboard

Dashboard 分别读取股票、基金和指数。它可以在响应层组合展示结果，但不会把跨数据集拼接结果再次持久化为一张来源含混的大表。

数据中心同时读取持久化的初始化状态。尚未开始时展示「初始化数据」，失败后处于
`IN_PROGRESS` 时展示「继续初始化」并锁定首次冻结的历史年数；只有状态达到
`COMPLETED` 后才展示日常更新和质量运行入口。Dashboard 只创建
`DATA_BOOTSTRAP` 后台任务，真正的 `LOCALIZE → CURATE → VALIDATE` 仍由 Worker
执行，因此关闭浏览器不会中断初始化。

## 14. 日常操作

首次运行前设置数据根和 Tushare Token。数据根必须位于源码目录之外；未设置
`QUANT_DATA_ROOT` 时默认使用 `~/qlab-data`：

```powershell
$env:QUANT_DATA_ROOT = "D:\quant-data"
```

数据源 Token 通过 Dashboard「设置」页维护，或直接写入数据根下的明文
`$QUANT_DATA_ROOT/.env`：

```dotenv
QUANT_TUSHARE_TOKEN=<your-token>
QUANT_TUSHARE_REQUESTS_PER_MINUTE=480
QUANT_TUSHARE_PROXY_URL=https://proxy.example.com
```

运行时优先读取数据根 `.env`，缺失时才回退到进程环境变量。源码树不提供
`.env.example`，也不得在源码根保存 Token。

Tushare 请求限流同样可在 Dashboard「设置」页动态修改，合法范围为每分钟 1–10000
次，未配置时默认 480 次。限流器使用单调时钟把真实 API 调用均匀分布到时间轴；
首次请求立即执行，重试请求也占用额度，请求规划和本地 Raw 复用不占用额度。
采集客户端和交易日历客户端在同一进程内共享限流器，但不同 Dashboard、Worker 或
CLI 进程分别计数；并行启动多个进程时，账户总频率可能高于单个进程的设置值。
内置默认值始终是 480；`.env` 或进程环境可以覆盖为 1–10000。配置值只是本地发送上限，
不能提高 Tushare 账户或代理服务本身的额度，设置过高可能收到上游限频错误。

代理 URL 也可在设置页动态修改。系统仅接受不含凭据、查询参数或片段的 HTTP/HTTPS
入口，保存时移除末尾斜杠，并在下一次真实请求前重建 Tushare Pro 客户端。清除数据根
设置后会回退到进程环境变量；两处都没有配置时使用官方入口。代理设置不改变端点设计，
仍禁止 `pro_bar` 和除基准指数外的逐证券采集。

首次构建最近五年数据：

```powershell
uv run quant data bootstrap --years 5
```

日常自动增量更新：

```powershell
uv run quant data update
```

开发或排障时，可以分阶段运行：

```powershell
uv run quant data localize stock_daily_bar --from 2026-08-01 --to 2026-08-25
uv run quant data curate stock_daily_bar --from 2026-08-01 --to 2026-08-25
uv run quant data validate stock_daily_bar
uv run quant data validate-all
```

`bootstrap` 和 `update` 会编排完整流程。分阶段命令主要用于理解、测试和定位错误。

生产假设是 Tushare 账户具有所需积分、VIP 和基金复权权限。Token 不得写入代码、配置文件、日志或测试数据。

## 15. 常见故障与处理

| 现象 | 常见原因 | 正确处理 |
|---|---|---|
| 提示旧 Canonical 布局 | 数据根仍有 `canonical/dataset=...` | 更换空的数据根并重新 bootstrap，不做混合迁移 |
| Tushare 请求被拒绝 | Token 缺失、积分、接口权限或频率设置不匹配 | 检查设置页、账户权限，并把每分钟请求数调到该账户最低适用额度 |
| 端点返回行数达到上限 | 全市场结果可能被截断 | 让采集失败，调整合法的市场、日期或行业切片；不能静默接受 |
| Schema 漂移 | Tushare 输出字段发生变化 | 对照官方端点检查并同步 Schema、Mapper、测试和文档 |
| 单数据集 validate 通过但研究仍不能读 | 全局门禁尚未通过 | 运行 `validate-all` 并处理其他数据集问题 |
| 实验提示数据漂移 | 运行期间 `catalog_hash` 变化 | 在数据更新完成并重新验证后重新提交运行 |
| 某日没有股票行情 | 休市、停牌、抓取缺失或数据错误 | 结合交易日历和停牌事件判断，不能直接填零 |
| 行业或财务数据不可用于历史日期 | 缺少可靠可用时间证据 | 保持 PIT 限制，不用当前状态回填历史 |

## 16. 新增或修改数据集时的开发清单

数据契约变化必须一次性同步，不能只改下载代码。通常需要检查：

1. 在 `DatasetKind` 中确定最终数据集名称；
2. 在 Canonical Schema 中声明完整列、类型、主键和排序键；
3. 在数据集目录中声明端点、分区、抓取粒度、频率、重用和新鲜度；
4. 在 Tushare 客户端中显式声明官方输出字段和合法请求；
5. 在 Mapper 中实现字段、日期、单位、代码和审计列转换；
6. 确认请求不违反“除 `index_daily` 外禁止 `ts_code`”；
7. 增加质量规则和 Repository 读取接口；
8. 更新股票池、因子、策略、回测或 Dashboard 消费者；
9. 增加字面量测试，覆盖字段映射、单位、PIT、哈希和路径；
10. 同步本文及相关架构文档。

项目未发布，不保留旧数据集名称或旧接口兼容层。设计改变时应直接迁移到唯一最终语义。

## 17. 必须始终成立的不变量

- 唯一生产数据源是 Tushare。
- 数据流水线固定为 `LOCALIZE → CURATE → VALIDATE`。
- 除 `index_daily` 外，采集请求不得携带证券代码。
- 股票池和 ETF 池不得控制上游采集范围。
- 股票和基金使用 `InstrumentId`，指数使用 `IndexId`。
- Canonical 只接受 `source=tushare` 命名空间。
- 路径不参与内容、输入或目录哈希。
- 供应商百分数进入 Canonical 时转换为小数。
- 研究收益由 `close` 和 `preclose` 计算。
- 股票和基金复权在本地使用各自复权因子完成。
- 财务和行业研究必须遵守 PIT 可用时间。
- 单数据集校验不开放研究读取；只有 `validate-all` 可以。
- 研究代码只能通过 `CanonicalResearchRepository` 读取。
- 实验运行期间 `catalog_hash` 不得漂移。
- 组合、订单和持仓不得接受 `IndexId`。
