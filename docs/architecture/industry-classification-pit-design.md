# 行业分类 PIT 整体设计

## 1. 文档定位

本文定义 `industry_classification` 从 BaoStock 采集、Raw 审计、Canonical 事件化、质量门禁、研究读取到业务消费的完整设计。本文描述当前已实施结构，不是迁移方案、待办清单或兼容性说明。

通用数据流水线、目录身份和研究门禁以[数据层设计](data-layer-design.md)为准；因子研究阶段与产物以[因子研究与分析总体设计](factor-analysis-design.md)为准；本文集中规定行业分类特有的供应商语义和跨业务约束。代码层的可执行权威定义位于：

- `src/quant_research/data/catalog.py` 的 `DATASET_CATALOG`；
- `src/quant_research/data/schemas.py` 的 `CANONICAL_SCHEMAS`；
- `src/quant_research/data/repository.py` 的 `CanonicalResearchRepository`。

设计目标是让每日市场全景、因子、策略、组合、回测和归因共享同一套严格 as-of 行业状态，同时避免把当前分类回填到历史日期。系统不维护可选择的数据集版本或发布 Snapshot；所有读取仍受当前 `catalog_hash` 和 `validate-all` 门禁约束。

## 2. PIT 语义与证据边界

### 2.1 接受的语义

唯一数据源是：

```text
BaoStock query_stock_industry(code="", date=D)
```

`date=D` 表示供应商按历史查询日期重建的 as-of 状态。系统据此把请求日期 `D` 作为状态可见边界，但不声称该响应是本系统在 D 当日实际观察并永久封存的内容。

以下三个日期必须分开理解：

| 字段 | 含义 | 是否决定 PIT 可见性 |
|---|---|---|
| `as_of_date` | 请求参数 `date=D`，即供应商重建快照边界 | 是 |
| `supplier_update_date` | 响应字段 `updateDate`，仅表示供应商状态元数据 | 否 |
| `ingested_at` | 本机实际抓取该响应的时间 | 否，仅用于血缘和重述审计 |

`supplier_update_date` 不等同于供应商对外发布时间，也不能用于把后来抓取的状态回写到更早日期。`available_at` 固定取 `as_of_date` 的上海日终，`availability_source` 固定为 `BAOSTOCK_AS_OF_DATE_RECONSTRUCTED`。

### 2.2 来源语义证据

2026-08-16 的只读来源门禁检查了三个已知状态变化窗口：

- 2023-12-22、2023-12-25、2023-12-26；
- 2024-12-27、2024-12-30、2024-12-31；
- 2025-12-26、2025-12-29、2025-12-30。

每组响应均表现为变化日前旧状态、变化日新状态、后一交易日维持新状态，且满足 `updateDate <= 请求日期`。2024 年窗口的响应行数为 5,405、5,407、5,407，后两日响应内容哈希一致。

该证据支持按请求日期重建状态，但不证明供应商永不重述历史响应。因此数据集使用 `APPEND_WITH_RESTATEMENT`：同一历史请求的 Raw 当前头发生变化时，系统必须重建受影响年度、切换 Canonical 和全局目录身份，并关闭研究读取门禁。

### 2.3 不允许的推断

系统不得：

- 将 `updateDate` 解释为公告时间或历史可得时间；
- 将当前最新行业投影到历史信号日；
- 使用未来日期或尚未完整结束交易日的响应作为 PIT 证据；
- 因供应商某次响应缺失证券而自动推断其行业已被取消；
- 把“供应商重建 PIT”描述为“本系统当日实盘观察 PIT”。

## 3. 端到端结构

行业分类遵循固定数据流水线：

```text
交易日历
   ↓
逐交易日全市场请求
   ↓
不可变 Raw 快照与当前头
   ↓
年度基线 + 状态变化事件
   ↓
validate-all / catalog_hash 门禁
   ↓
CanonicalResearchRepository 单日或批量重建
   ↓
市场全景 / 因子 / 策略 / 组合 / 回测 / 归因
```

业务能力不得直接扫描 Raw、Canonical Parquet 或连接 BaoStock。单日期和批量日期读取只是同一 Repository 的两个查询形态，不构成第二套行业数据入口。

## 4. Localize 与 Raw

### 4.1 请求生成

Localize 复用交易日历，为窗口内每个已完整结束的交易日 `D` 生成一个独立请求：

```json
{
  "api": "query_stock_industry",
  "scope": "ALL",
  "date": "2026-08-04",
  "as_of": "2026-08-04"
}
```

请求按日期稳定排序并去重。显式未来日期或尚未完整结束的交易日必须拒绝，不能静默截断后假装完成。每个交易日请求是独立的断点续抓单元；普通 Localize 复用 Schema 兼容的 Raw 当前头，`--full` 则重新请求选定窗口内全部业务单元。

### 4.2 Raw 内容

Raw 不可变保存供应商完整响应，并在行业端点的每一行附加合成列：

```text
as_of_date=D
```

`as_of_date` 是行业端点特有的业务字段，不是所有 Raw 数据集的通用审计列。其他端点继续使用各自的供应商日期字段和 Raw request/manifest 上下文。

Raw 文件继续按 request hash 和 content hash 内容寻址。同一请求内容被供应商重述时，旧 Raw 对象保留审计，新对象成为 `raw_request.current_content_hash` 指向的当前头。

历史遗留未来 Raw 不做手工删除；在对应日期完整结束前，它们必须被排除在 Curate 候选记录、分区 `input_hash` 和 Canonical 输出之外，但仍参与 Raw 当前头漂移检查，避免 Curate 期间的并发变化被漏检。

## 5. Canonical 模型

### 5.1 Schema

Canonical 列为：

| 字段 | 类型 | 定义 |
|---|---|---|
| `as_of_date` | `Date` | 供应商历史请求日期，也是事件可见日期 |
| `supplier_update_date` | `Date` | BaoStock `updateDate` 原值，仅供审计 |
| `instrument_id` | `String` | Canonical 证券代码 |
| `taxonomy` | `String` | 供应商分类体系；当前为“证监会行业分类” |
| `industry_code` | `String`，可空 | 供应商行业原值；未分类时为空 |
| `industry_name` | `String`，可空 | 展示名称；未分类时为空 |
| `is_classified` | `Boolean` | 是否具有有效行业分类 |
| `source` | `String` | 固定为 `baostock` |
| `available_at` | `Datetime(us, UTC)` | `as_of_date` 的上海日终 |
| `availability_source` | `String` | `BAOSTOCK_AS_OF_DATE_RECONSTRUCTED` |
| `pit_usable` | `Boolean` | 是否满足 PIT 使用条件 |
| `ingested_at` | `Datetime(us, UTC)` | 本机抓取时间，仅作血缘审计 |

主键和排序键均为：

```text
(as_of_date, instrument_id, taxonomy)
```

物理分区为：

```text
year=<as_of_date.year>
```

目录契约固定为交易日粒度抓取、每日更新、最近 5 个自然日增量重抓窗口以及 `APPEND_WITH_RESTATEMENT`。Freshness 使用 `TRADING_SESSION`，以 Canonical `as_of_date`/数据集 `end_date` 对齐最近完整交易日，容忍度为 0；供应商可能呈周度变化的 `updateDate` 不决定 freshness。

### 5.2 年度基线与状态事件

Curate 按请求年份独立重建：

1. 按 `as_of_date` 排序，同一请求日期只读取 Raw 当前头。
2. 每年第一份通过完整性门禁的快照保留全市场基线，包括未分类记录。
3. 后续快照只保留新增证券、行业代码或名称变化、已分类/未分类状态变化。
4. 新状态从首次出现它的 `as_of_date` 起生效，不得回写到更早的 `supplier_update_date`。
5. 供应商明确返回空行业时生成 tombstone：`is_classified=false`，行业代码和名称为空。
6. 某证券整行缺失时沿用此前状态，同时由覆盖质量规则诊断；缺失不自动生成 tombstone。
7. Raw 当前头重述时完整重建请求年份；不保留旧 Canonical 兼容入口。

候选分区只由请求中的 `as_of_date.year` 决定。1 月请求即使返回上一年 `supplier_update_date`，也必须进入新年的请求分区。这样新增当年 Raw 只改变当年分区的 `input_hash`，不会持续重建上一年。

### 5.3 状态压缩示例

以下示例均使用“证监会行业分类”。“空”表示供应商明确返回空行业，“缺失”表示证券整行没有出现在响应中。

Raw 快照：

| `as_of_date` | `supplier_update_date` | A | B | C | D |
|---|---|---|---|---|---|
| 2026-01-05 | 2025-12-29 | J66 | C39 | 空 | 缺失 |
| 2026-01-06 | 2025-12-29 | J66 | 缺失 | 空 | I65 |
| 2026-01-12 | 2026-01-12 | C39 | C39 | J66 | I65 |
| 2026-01-13 | 2026-01-12 | C39 | 空 | J66 | I65 |

`year=2026` 保存的事件：

| `as_of_date` | 证券 | 状态 | 原因 |
|---|---|---|---|
| 2026-01-05 | A | J66 | 年度基线 |
| 2026-01-05 | B | C39 | 年度基线 |
| 2026-01-05 | C | tombstone | 基线中的显式未分类 |
| 2026-01-06 | D | I65 | 首次出现 |
| 2026-01-12 | A | C39 | 行业变化 |
| 2026-01-12 | C | J66 | 从未分类变为已分类 |
| 2026-01-13 | B | tombstone | 从已分类变为显式未分类 |

B 在 2026-01-06 的整行缺失不产生事件，仍沿用 2026-01-05 的 C39；B 在 2026-01-13 的明确空行业则必须生成 tombstone，之后任何消费者都不能继续继承 C39。

对应查询 oracle：

| 查询日 | A | B | C | D |
|---|---|---|---|---|
| 2026-01-05 | J66 | C39 | tombstone | 无状态 |
| 2026-01-06 | J66 | C39 | tombstone | I65 |
| 2026-01-11 | J66 | C39 | tombstone | I65 |
| 2026-01-12 | C39 | C39 | J66 | I65 |
| 2026-01-13 | C39 | tombstone | J66 | I65 |

## 6. Repository 查询契约

### 6.1 单日期查询

```python
industry_classifications_as_of(instruments, as_of)
```

查询仅考虑：

```text
as_of_date <= 查询日
available_at <= 查询日上海日终
pit_usable = true
```

随后按 `(instrument_id, taxonomy)` 选择 `as_of_date`、`available_at` 最新的事件。结果保留 tombstone，并按 Canonical 排序键确定性排序。消费者必须在状态排名完成后再应用自己的未分类策略。

### 6.2 批量日期查询

```python
industry_classifications_on_dates(instruments, dates)
```

批量入口返回 `query_date`、命中的 Canonical 事件和全部审计列，按 `(query_date, instrument_id, taxonomy)` 排序。日期会排序去重；空日期集合返回固定 Schema 的空结果。

批量入口与单日期入口共享同一内部 SQL 状态重建内核，并在一次 Repository 读取中处理全部日期，避免研究、回测或归因按日形成 N+1 Parquet 扫描。对任一日期，单日期和批量入口必须产生相同状态。

## 7. 消费者契约

行业分类是显式依赖，不会自动注入所有研究和策略。未声明 `DatasetKind.INDUSTRY_CLASSIFICATION` 依赖的现有公式和运行结果保持不变。

| 消费者 | 日期边界 | 约束 |
|---|---|---|
| 每日市场全景 | 所选复盘日 | 区分无快照、零覆盖和有效分类 |
| 因子与因子研究 | 信号日 | 仅显式依赖时用于行业分组、中性化或派生特征 |
| 策略与组合 | 决策日或调仓日 | 不得使用成交日之后出现的分类 |
| 回测 | 产生信号或目标权重时的状态 | 执行阶段不得用后续分类改写历史决策 |
| 归因 | 每个归因日 | 不得用实验结束日或当前分类回填整个区间 |

所有行业相关配置必须显式提供：

```yaml
industry:
  taxonomy: 证监会行业分类
  unclassified_policy: EXCLUDE
```

`unclassified_policy` 只允许：

- `EXCLUDE`：tombstone、无覆盖和缺失状态不参加行业分组、中性化或约束；
- `UNCLASSIFIED`：将其放入稳定的未分类组。

未分类策略进入配置哈希。使用行业输入的 manifest 和质量披露至少记录 dataset、taxonomy、日期口径、未分类策略、覆盖率和 `BAOSTOCK_AS_OF_DATE_RECONSTRUCTED`。

独立因子研究已经实现该显式消费契约：配置行业块后，同一运行发布 `DIRECTION_ADJUSTED` 与 `INDUSTRY_NEUTRALIZED` 两个版本。中性化固定使用信号日 `industry_code` 的等权组内去均值，状态通过一次 `industry_classifications_on_dates` 批量读取；逐日覆盖写入 `industry_coverage.parquet`，聚合覆盖和可见性语义写入 Manifest。股票策略、组合约束和回测归因仍未启用行业消费，不能仅凭实验 Manifest 中存在行业输入就推断策略使用了行业信号。

Dashboard 需要先读取全市场状态，再与复盘证券集合求交：

- 全市场状态为空：显示所选日期之前没有可用的供应商重建快照；
- 有快照但所选证券无覆盖或全部为 tombstone：保留快照语义，显示 `coverage_rate=0` 和空行业列表；
- 存在有效分类：过滤 tombstone 后统计行业，未分类证券仍进入覆盖率分母。

## 8. 质量、身份与重述

### 8.1 质量门禁

行业数据至少满足以下约束：

- 每份 Raw 响应的 `updateDate` 非空且唯一，taxonomy 非空且唯一；
- `supplier_update_date <= as_of_date <= latest_complete_session`；
- 同一响应内 `(instrument_id, taxonomy)` 不得存在冲突状态；
- 每个请求年份必须存在通过覆盖门禁的年度基线；
- Canonical 主键唯一，年度分区 Schema 完全一致；
- `is_classified=true` 时行业代码和名称非空；tombstone 时两者为空；
- `available_at` 与 `as_of_date` 上海日终一致；
- 最新 Canonical `as_of_date` 达到最近完整交易日；
- 已上市股票的行业覆盖率进入质量结果，异常缺失不能被旧状态静默掩盖；
- 未来或未完成交易日 Raw 不进入 Canonical 或分区输入身份。

`validate industry_classification` 只生成诊断；只有 `validate-all` 能对精确的当前 `catalog_hash` 开放研究读取。

### 8.2 重述与运行隔离

同一历史请求强制重抓后可能产生新 Raw 当前头。Curate 必须：

1. 重新计算受影响年份的 `input_hash`；
2. 从该年份全部合格 Raw 当前头重建事件；
3. 内容变化时切换分区、数据集和全局目录身份；
4. 清空 `validated_catalog_hash` 并关闭门禁；
5. 使已捕获旧 `catalog_hash` 的在途实验在阶段边界以数据漂移失败。

既有实验、回测和研究产物保持不可变，重述不能覆盖旧产物。系统也不提供选择旧 Canonical 状态的兼容读取。

## 9. CLI 与本地数据基线

### 9.1 固定初始重建窗口

当前本地基线使用闭区间 `[2024-08-12, 2026-08-14]`：

```powershell
uv run quant data localize industry_classification --from 2024-08-12 --to 2026-08-14 --full
uv run quant data curate industry_classification --from 2024-08-12 --to 2026-08-14 --full
uv run quant data validate-all
```

两个日期选项必须同时传入。Localize 对区间内每个完整交易日请求一次；Curate 不访问供应商，而是读取 SQLite 登记的合格 Raw 当前头，按请求年份完整重建选中分区。必须先完成全部 Localize，再由 Curate 原子切换 Canonical，最后通过 `validate-all` 重开研究门禁。

省略日期时，`localize --full` 使用动态 bootstrap 窗口，不能替代上述固定基线。普通后续更新继续使用每日增量语义和最近 5 个自然日重抓窗口。

不得手工修改 Raw、Canonical Parquet 或 SQLite 当前指针。

### 9.2 已验收基线

2026-08-16 完成的本地基线结果为：

| 项目 | 结果 |
|---|---:|
| 行业交易日 Raw 请求 | 487 |
| `year=2024` Canonical 事件 | 5,454 |
| `year=2025` Canonical 事件 | 5,820 |
| `year=2026` Canonical 事件 | 5,572 |
| Canonical 事件合计 | 16,846 |
| `validate-all` | 55 PASS，0 issue |

对 2026-08-04 的生产 Repository 字面量验收重建出 5,538 个状态，其中 5,204 个有效分类、334 个 tombstone；最新命中事件日期为 2026-08-03，证明查询会沿用最近状态事件，而不是要求查询日必须存在一份 Canonical 全市场快照。

## 10. 责任边界与非目标

主要职责归属：

| 模块 | 职责 |
|---|---|
| `infrastructure/baostock/client.py` | 逐交易日请求与 Raw 行合成 `as_of_date` |
| `infrastructure/baostock/mapper.py` | Canonical 字段、tombstone 和可见性审计映射 |
| `data/pipelines/dataset.py` | 窗口、未来 Raw 隔离、年度输入身份和事件压缩 |
| `data/quality/` | Schema、日期关系、基线、状态和覆盖门禁 |
| `data/repository.py` | 单日与批量 as-of 状态重建 |
| `dashboard/market_review.py` | 无快照、零覆盖和有效行业展示 |
| 因子、策略、组合、回测、归因 | 仅在显式依赖时消费 Repository 状态 |
| 实验配置与产物登记 | 校验行业配置并记录输入语义和身份 |

本设计不包含：

- 默认行业因子、默认行业中性化、默认行业轮动策略或默认行业权重约束；
- 把行业自动加入股票池筛选；
- 申万、中信等额外分类体系或第二供应商；
- 绕过 Repository 的文件扫描或供应商直连；
- 旧 Schema、旧 `all` 分区或旧可见性语义的兼容层；
- 对其他 Raw 端点统一增加 `as_of_date`；
- 回写或覆盖既有不可变研究产物。

新增行业能力必须复用本文的 as-of 重建、tombstone、配置身份和重述门禁，不得在消费者内部复制近似 PIT 算法。
