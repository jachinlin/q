# 数据层设计

文档状态：设计草案，待用户书面审阅　·　日期：2026-08-20

本文是数据目录、Schema、LOCALIZE → CURATE → VALIDATE 流水线、PIT 研究读取和质量门的权威设计。平台定位、整体依赖方向与跨层约束见[总体设计](design.md)；行业分类特有的 PIT 语义见[行业分类 PIT 设计](industry-classification-pit-design.md)。

## 目录

1. 设计取向
2. 边界
3. 三层与流水线
4. Canonical 数据集
5. 复权语义
6. PIT 与研究读取
7. 质量门
8. 数据身份
9. 存储与性能
10. 数据源隔离
11. 测试契约
12. 明确不做

## 1. 设计取向

- **不为跨运行复现付费**：无数据快照版本、无历史 catalog 回放。数据层只维护“当前一份”
  Canonical，重性能与去重（不重算），不为跨运行复现付费。Raw/Canonical 仍**内容寻址**
  （`content_hash` / `request_hash` / `schema_fingerprint`），用于去重、增量和完整性，
  不是为了回放旧数据；哈希边界见[总体设计](design.md) §3.4.2。
- **PIT 是唯一不让步的红线**：时点正确与防未来函数保持全部严格性。
- **公司行为一等公民**：分红、送转、除权除息作为 Canonical 事件数据集，服务复权与回测账务。
- **数据源可插拔**：`SourceClient` + `CanonicalMapper` 隔离，上层零改动切换 TuShare。

## 2. 边界

### 2.1 输入

- 供应商原始响应（BaoStock/TuShare），经 `SourceClient.fetch(request)` 返回 `RawBatch`
  （`source, endpoint, request: Mapping, retrieved_at, schema: tuple[str], rows: list[dict[str, str]]`，全 String）。
- CLI 可以省略采集日期并调用计划器推导窗口；进入 Pipeline 的每个 LOCALIZE 调用必须携带明确的
  `(start, end)`，底层不得把空日期解释为自动窗口。
- 交易规则和费率与本层无关；数据层不消费策略配置。

### 2.2 输出

下游唯一读取契约是总体设计中的 `ResearchDataRepository`，接口定义见
[总体设计](design.md) §11.2：

- 九个 Canonical 数据集（Parquet，强类型 + 五个审计列）：
  `instrument / trade_calendar / daily_bar / daily_basic / security_status / corporate_action /
  financial_observation / industry_classification / index_bar`。
- 只读 PIT 接口返回 `pl.LazyFrame`，物理截断到 `available_at ≤ 截止 & pit_usable`：
  `bars / adjusted_bars / log_returns / daily_basics / security_status_range / corporate_actions /
  financials_as_of / financial_history / industry_as_of / trade_calendar / instruments`。
- `catalog().require_validated_catalog()` 返回包含当前 `catalog_hash` 的只读目录状态，用于运行内一致性；
  质量门关闭时 Repository 抛出 `DATA_NOT_VALIDATED`。
- 副产物包括 SQLite 元数据（`raw_request / raw_object / canonical_dataset /
  canonical_partition / data_catalog_state / quality_run …`）和质量运行报告。

数据层不输出持久化复权价；复权因子在读取侧计算。数据层也不输出任何跨运行数据快照。

## 3. 三层与流水线

“三层”描述数据从证据到研究输入的稳定形态；“三阶段”描述生成和开放这些数据的执行过程。二者正交：

```text
数据形态：供应商响应 → Raw 证据层 → Canonical 事实层 → 研究视图与 Feature 派生层
执行阶段：             LOCALIZE   →     CURATE      →       VALIDATE
```

### 3.1 三层职责与禁止依赖

| 层 | 保存内容 | 身份与存储 | 允许的消费者 | 禁止行为 |
| --- | --- | --- | --- | --- |
| Raw 证据层 | 供应商原始字段、请求和抓取时间；业务列保持供应商字符串口径 | `request_hash` 定位请求，`content_hash` 定位不可变 Parquet；SQLite 保存当前头和历史对象元数据 | `CanonicalMapper`、完整性校验和采集诊断 | 研究代码直接读取；在 Raw 中改名、补值或做业务类型转换 |
| Canonical 事实层 | 统一证券标识、强类型业务列、主键、排序键和 PIT 审计列 | 按数据集和业务分区保存内容寻址 Parquet；SQLite 只维护唯一当前指针 | `CanonicalResearchRepository`、质量规则和读取侧复权服务 | 暴露供应商字段；绕过 Mapper 写入；保存无法解释的隐式派生值 |
| 研究视图与 Feature 派生层 | PIT 截断后的 LazyFrame、复权行情、收益、因子输入和运行内特征 | 绑定当前已验证 `catalog_hash`；默认按需计算，不形成第二套行情事实库 | Universe、Feature、Signal、Risk、回测和分析组件 | 绕过质量门；把未来可见数据返回给较早决策日；跨运行持久化未绑定目录身份的缓存 |

第三层是逻辑层，不等于数据包内必须存在名为 `FeatureBuilder` 的固定实现。通用研究读取由
`CanonicalResearchRepository` 提供，策略或研究组件在其上派生 Feature；Feature 不是 Raw 或
Canonical 的替代品。

依赖方向固定为：

```text
研究组件 → CanonicalResearchRepository → Canonical 当前指针 → Canonical Parquet
                                            ▲
SourceClient → RawPartitionStore → CanonicalMapper → CuratedPartitionStore
      ▲                                      │
供应商适配器（infrastructure）                └→ QualityRunner
```

- `SourceClient` 和 `CanonicalMapper` 是数据层定义的消费者侧协议；BaoStock、TuShare 适配器位于
  `infrastructure`。
- `pipeline` 可以依赖数据目录、存储协议、Mapper、质量规则和元数据端口；研究组件不能依赖 Pipeline。
- SQLite 是 Raw 当前头、Canonical 当前指针、质量运行和门禁状态的元数据权威；Parquet 是大表内容载体。

### 3.2 可执行目录是流水线控制面

`DATASET_CATALOG` 是所有阶段共同使用的唯一可执行数据目录。每个 `DatasetSpec` 同时声明：

- Canonical Schema、主键、排序键和 PIT 字段；
- 物理分区方式：`year`、`report_year` 或 `all`；
- 抓取粒度与 `FetchPlan`；
- 增量回看天数、刷新频率和复用语义；
- 供应商端点、字段映射以及一个 Raw 响应可 fan-out 的 Canonical 数据集；
- 新鲜度判定依据和容忍范围。

`RoutingTable` 只负责为数据集选择目录已声明的可用供应商。Pipeline 不以数据集名称编写第二套
Schema、端点或分区分支；新增数据集必须先进入目录和 Canonical Schema，再由对应抓取计划及 Mapper
实现能力。

### 3.3 更新计划

数据更新在入队前生成不可变 `DataUpdatePlan`，Worker 只执行持久化计划，不在执行时重新解释“更新到
哪里”。计划按数据集保存 `DataUpdateWindow`：

| `basis` | 生成条件 | 窗口语义 |
| --- | --- | --- |
| `EXPLICIT` | 用户同时提供开始日和结束日 | 使用显式闭区间，并按数据集日历边界修正 |
| `INCREMENTAL` | 数据集已有当前水位 | 从 `min(current_watermark, latest_complete_day) - overlap_days` 开始，到目标业务日 |
| `SNAPSHOT_REFRESH` | 数据集采用无历史窗口的全量快照 | 以计划日执行完整快照；不读取 Canonical 水位，不应用重叠窗口 |
| `DISCLOSURE_TRIGGER` | 最近已结束财务报告期的披露截止日已被严格越过 | 仅包含该披露批次的报告期末范围；记录 `trigger_date`，不记录水位和重叠窗口 |

`trade_calendar` 是日历前瞻覆盖数据集。自动计划的目标结束日固定扩展到计划业务日之后 90 个自然日，
以便调度、开市日推算和研究区间校验；`canonical_dataset.end_date` 表示“日历覆盖末日”，不是市场数据
已经更新到的业务水位。其 `overlap_days=30` 表示计划覆盖最近 30 个自然日，不是更新滞后；相同
`request_hash` 的有效 Raw 当前头仍直接复用，不主动轮询同一请求的供应商修订。
`dataset_operational_state.localized_through` 记录最近一次计划已检查到的业务日，不能用未来的
日历覆盖末日替代。其他数据集不得借日历前瞻读取未来业务数据。计划中的数据集按 `dataset.value` 稳定
排序，`plan_hash` 对不含生成时间的规范 JSON 求 SHA-256，用于提交幂等和“预览内容就是执行内容”的
一致性校验。

`instrument` 固定使用 `SNAPSHOT_REFRESH`。其 `list_date` 和 `delist_date` 是证券生命周期字段，
不能作为数据更新水位；即使供应商返回未来上市证券，也不得把最大的 `list_date` 显示为当前水位。
Canonical 的 `start_date/end_date` 对该数据集记录快照抓取业务日，自动计划和 Dashboard 均把
`current_watermark`、`overlap_days` 展示为“不适用”。最近成功刷新证据来自
`dataset_operational_state.last_localized_at/localized_through`。

### 3.3.1 首次初始化

`quant data bootstrap --years N` 是唯一首次初始化入口，`N` 为必填正整数。首次调用冻结
`years/start/end` 并在 SQLite `data_initialization_state` 中登记为 `IN_PROGRESS`；中断后只能使用相同
年数续跑原窗口，全部 Canonical 发布且 `validate-all` 成功后才转为 `COMPLETED`。完整目录禁止再次
bootstrap。update 在空库、初始化中或任一可执行 Canonical 数据集缺失时返回
`DATA_UPDATE_REQUIRES_BOOTSTRAP`，不再自动生成 BOOTSTRAP 数据集窗口。

CLI 的 `localize` 和 `localize-all` 可以省略日期，此时先使用同一计划器推导窗口；Pipeline 的
`localize(dataset, start, end)` 与 `localize_all(windows)` 只执行已解析日期，不读取水位或隐式扩大窗口。

#### 3.3.2 `financial_observation` 的季度触发规则

`financial_observation` 不使用 `canonical_dataset.end_date`、报告期最大值或 `overlap_days` 作为自动更新
水位。财务报告不是逐日连续数据，自动更新只由季度最晚披露日触发。

保守最晚披露日固定为：

| 报告期 | 最晚披露日 |
| --- | --- |
| Q1（3 月 31 日） | 当年 4 月 30 日 |
| Q2（6 月 30 日） | 当年 8 月 31 日 |
| Q3（9 月 30 日） | 当年 10 月 31 日 |
| Q4（12 月 31 日） | 次年 4 月 30 日 |

自动计划以上海时区的 `planning_date` 判断：只有
`planning_date > financial_disclosure_deadline(report_year, report_quarter)` 时，该报告期才进入抓取集合；
披露截止日当天仍视为尚未完整结束，不抓取。

```text
planning_date <= 最近已结束报告期的最晚披露日
    → financial_observation 不进入本次自动更新计划
    → skipped_datasets 记录 DISCLOSURE_DEADLINE_PENDING 和 trigger_date

planning_date > 最近已结束报告期的最晚披露日
    → 生成该披露批次的完整财务请求
    → 每个有效证券 × 报告年 × 报告季度 × 财务端点
```

同一个最晚披露日可以对应多个报告期。例如 4 月 30 日同时对应“上一年度 Q4”和“本年度 Q1”，
因此 5 月 1 日起的首个自动更新必须同时刷新这两个报告期，不能只取其中一个。

该规则的具体含义是：

- 自动计划不根据财务 Canonical 水位向前回看，也不生成连续日期窗口；
- 未超过当前披露批次最晚发布日期时，不调用财务供应商端点；
- 超过后只刷新最近到期披露批次，不因历史报告期较早而重复展开全部历史季度；
- 同一季度再次运行时，规范请求身份不变，LOCALIZE 通过 Raw request 幂等检查直接复用；
- 首次建库仍可按 bootstrap 范围补齐所有已经超过最晚披露日的历史报告期；显式更新可以按用户指定
  报告期执行，不受自动触发规则限制；
- 财务重述由同一请求后续取得的新 `content_hash` 和 CURATE revision 语义处理，不把重述检测伪装成
  日期水位推进。

Dashboard 必须直接展示后端计划证据，不在浏览器内重复推算截止日：

- 数据资产表将财务证据显示为“季度披露 · `trigger_date` · 无需更新/待更新”，不得放入“实际/目标
  水位”口径；
- 自动计划预览把 `DISCLOSURE_DEADLINE_PENDING` 显示为跳过项，并在只有跳过项时禁用提交；
- 越过截止日后，执行窗口依据显示为“季度披露”，当前水位和重叠天数显示“不适用”；
- 运行中心按任务固化的 `trigger_date` 和跳过证据展示历史计划，不用当前日期重算历史结论。
- `instrument` 显示“全量快照 · 最近刷新时间”；计划与运行详情显示“快照日期”，不得显示最大上市日
  或数字 `0` 形式的重叠天数。
- `trade_calendar` 的资产证据显示“日历覆盖至 / 已检查至”；计划与运行详情显示“覆盖至 / 修订回看
  30 天 / 抓取开始日至结束日”，不得把未来覆盖末日标为“当前水位”。

```text
Dashboard / CLI
      │ 请求预览或提交
      ▼
DataUpdatePlanner ──读取当前水位和供应商日历──> DataUpdatePlan
      │                                               │
      └──────────────规范 payload + plan_hash─────────┘
                                                      ▼
                                             DATA_UPDATE 任务
                                                      ▼
                                                   Worker
```

### 3.4 阶段总契约

流水线阶段固定为 `LOCALIZE → CURATE → VALIDATE`，不允许跳过前置阶段后直接开放研究读取门。

| 阶段 | 主要输入 | 成功输出 | 可持久化检查点 | 是否允许开放研究门 |
| --- | --- | --- | --- | --- |
| `LOCALIZE` | 数据集窗口、路由、供应商会话、已有 Raw 当前头 | `PublishedPartition` 和 `LocalizeResult` | 每个规范请求完成后立即登记 Raw 对象和当前头 | 否 |
| `CURATE` | 当前 Raw 头快照、Mapper、Canonical Schema、已有 Canonical 指针 | `DatasetCurateResult` 和新的数据集当前指针 | 每个 Canonical 数据集原子替换完成后登记 | 否；内容变化会使旧门禁失效 |
| `VALIDATE` | 本次开始时的 `catalog_hash`、全部当前 Canonical 分区、质量规则 | 完整质量运行、规则结果、问题清单 | 质量运行整体登记 | 只有成功的 `validate-all` 可以开放 |

完整更新返回 `PipelineResult(run_id, quality_run_id, data_hash)`；任务层另外保存各数据集的抓取数、跳过
数、重建分区数、复用分区数、Raw 读取数和最终目录身份。

### 3.5 LOCALIZE：供应商响应本地化

LOCALIZE 的职责仅限供应商 I/O、Raw 保真和请求级幂等：

1. Pipeline 为本次 `localize-all` 建立一次供应商会话，按计划中的稳定数据集顺序执行。
2. `FetchPlan` 把数据集窗口展开为规范请求单元，例如交易日、日期区间、完整证券快照或
   “证券 × 报告年 × 季度 × 财务端点”。
3. 每个请求先按规范 JSON 计算 `request_hash`，依次检查 SQLite 当前头和可信 Raw manifest。
4. 已有、完整且允许复用的请求直接返回当前 `PublishedPartition`；否则调用供应商并取得 `RawBatch`。
5. `RawPartitionStore` 把响应转换为全字符串 Arrow 表，计算内容身份，先写同目录临时文件，再原子安装
   Parquet 和 `manifest.json`。
6. 文件发布成功后登记 `raw_object`，并把 `raw_request.current_content_hash` 切换到该对象。

物理布局为：

```text
raw/
└── source=<source>/
    └── endpoint=<endpoint>/
        └── <request_hash>/
            ├── <content_hash>.parquet
            └── manifest.json
```

`manifest.json` 保存规范请求、当前内容哈希及该请求的内容历史。相同请求、相同内容重复发布必须幂等；
相同请求返回不同内容时追加不可变 Raw 对象并切换当前头，不覆盖旧 Parquet。LOCALIZE 不执行
Canonical 类型转换，也不修改研究读取门。

### 3.6 CURATE：规范化、增量判定与发布

CURATE 对计划选中的数据集检查全部当前业务分区，但只读取和重建生产输入身份变化的分区：

1. 按数据集路由列出相关端点的 Raw 当前头，并冻结 `RawHeadSnapshot`。
2. Mapper 根据请求业务范围计算候选 Canonical 分区键；Schema 不兼容的 Raw 不能静默进入转换。
3. 为每个分区计算转换身份和 `input_hash`：

   ```text
   transform_hash = hash(数据集 + Mapper/Schema + 分区及复用规则 + 发布语义)
   input_hash     = hash(数据集 + 分区键 + transform_hash + 排序后的 Raw 输入身份)
   ```

4. 旧分区满足“文件存在且旧 `input_hash == 新 input_hash`”时直接复用，不读取 Raw；以下情况进入重建：
   `new_partition / canonical_file_missing / raw_input_changed`。Raw 输入消失且不是
   Schema 阻断时，删除对应当前分区。
5. 需要读取的 Raw 对象按稳定身份去重；一个 Raw 响应 fan-out 到多个 Canonical 数据集时只读取和
   normalize 一次。
6. Mapper 完成字段映射、强类型转换、证券标识统一、PIT 审计列生成和无效原因保留；随后按数据集
   主键去重、按排序键稳定排序并形成完整分区。
7. 发布前校验 Schema、主键唯一性和分区范围。Canonical Parquet 先写同文件系统临时文件，再按
   `content_hash` 原子安装。
8. 元数据事务在切换指针前再次比较当前 Raw 头与步骤 1 的快照；发生漂移时抛出
   `DATA_CURATE_INPUT_CHANGED`，不得发布混合输入结果。

物理布局为：

```text
canonical/
└── dataset=<dataset>/
    └── <partition_key>/            # year=2026 / report_year=2025 / all
        └── <content_hash>.parquet
```

Canonical 数据集指针一次包含全部当前分区。替换某个分区时，未变化分区沿用旧指针，变化分区切换到
新内容；只有 Canonical 内容真的变化才更新数据集身份和全局 `catalog_hash`，并使此前的全局质量门
失效。失去引用的文件属于孤儿清理对象，不参与当前研究读取。

`input_hash` 描述“由哪些 Raw 输入和哪套转换语义生产”，`content_hash` 描述“最终分区内容是什么”；
两者不能互相替代。输入身份变化但输出恰好相同时，可以重新计算后复用同一个内容寻址文件。

### 3.7 VALIDATE：质量运行与研究门

VALIDATE 分为两种范围：

- `validate <dataset>`：诊断单个数据集并登记质量运行，不开放全局研究门。
- `validate-all`：绑定全部可执行数据集的当前身份，只有它通过后才能开放研究读取门。

`validate-all` 在开始时捕获 `catalog_hash`，解析并校验所有当前 Canonical 路径、Schema 和内容元数据，
再执行“规则 × 数据集”矩阵。每条规则必须形成 `PASS / FAIL / SKIPPED` 结果；失败规则按严重级生成
可执行 `QualityIssue`。质量运行完整登记后，元数据事务再次比较运行的 `input_hash` 与当前
`catalog_hash`：

```text
质量通过且 catalog_hash 未漂移 → validated_catalog_hash = catalog_hash → 开门
存在 SEVERE/FATAL 问题          → 记录 FAILED                  → 保持关门
校验期间 catalog_hash 改变      → DATA_VALIDATE_INPUT_CHANGED  → 保持关门
```

研究读取门的判定不是“最近有一个通过的质量运行”，而是
`validated_catalog_hash == current catalog_hash`。因此任何 Canonical 内容变化都会自动关闭旧门禁，
直到新的 `validate-all` 对当前目录通过。

### 3.8 研究读取与 Feature 派生

`CanonicalResearchRepository` 是研究侧唯一入口：

1. 查询前要求当前目录已经通过 `validate-all`，并绑定该次读取使用的 `catalog_hash`。
2. 只接受元数据当前指针中的受信任 Canonical 路径，不接受任意用户文件路径。
3. 打开文件时验证路径仍位于可信根、文件类型和大小受限、Schema 与登记信息一致。
4. 通过 Polars/DuckDB 下推数据集、日期、证券和字段过滤，返回 `pl.LazyFrame`。
5. 在数据离开仓库前执行 `available_at` 和 `pit_usable` 物理截断；读取侧复权也只能使用截止日可见事件。

Feature、Signal 和研究组件可以组合这些 LazyFrame，但必须携带或检查提交时捕获的 `catalog_hash`；
运行阶段发现目录漂移立即失败，不能把两个目录状态的数据拼进同一个研究结果。

### 3.9 原子性、恢复与并发

- 同一数据根由进程级执行锁保护，同时只能有一个 LOCALIZE/CURATE/VALIDATE 写流程。
- Raw 请求是最小恢复单元：任务失败或取消后，已经完整发布的 Raw 对象继续有效，重试按请求身份复用。
- Canonical 数据集是 CURATE 的提交单元：单个数据集的当前指针只在全部目标分区写完并通过 Raw 头
  快照检查后切换，不存在指向半文件的状态。
- 完整更新不是跨所有数据集的大事务。若后续数据集失败，已成功切换的数据集可以保留，但目录门保持
  关闭；重试利用 Raw 当前头和分区 `input_hash` 继续推进。
- 取消在供应商请求、数据集和 Canonical 分区等安全边界生效；不删除已经发布的不可变证据。
- `VALIDATE` 失败只登记诊断证据并保持关门，不回滚正确的 Raw 或 Canonical 内容。
- 重试创建新的任务 attempt；不得覆盖旧 attempt 的状态、日志或结果。

### 3.10 进度、日志与阶段结果

Pipeline 通过 `PipelineObserver` 在阶段开始、数据集完成、请求完成和分区完成边界报告进度并检查取消。
结构化日志至少覆盖：

- LOCALIZE：供应商、端点、规范请求、请求/内容哈希、抓取或复用、行数和 Raw 路径；
- CURATE：数据集、分区键、重建原因、Raw 输入数、输入/内容哈希、行数和指针是否变化；
- VALIDATE：绑定的 `catalog_hash`、每条规则结果、问题严重级、质量运行 ID 和门禁结果；
- Task/Worker：`task_id`、`attempt_id`、Worker、阶段进度、取消、失败或成功结果。

端到端时序如下：

```text
提交端       Planner       Task/Worker       LOCALIZE        CURATE          VALIDATE       Repository
  │             │               │                │               │                │               │
  ├─更新请求───>│               │                │               │                │               │
  │<─计划预览───┤               │                │               │                │               │
  ├─提交 plan_hash─────────────>│                │               │                │               │
  │             │               ├─执行固化计划──>│               │                │               │
  │             │               │                ├─发布/复用 Raw─┤                │               │
  │             │               │                │               ├─原子切换指针───>│               │
  │             │               │                │               │                ├─登记质量运行──>│
  │             │               │                │               │                ├─无漂移则开门──>│
  │             │               │<────────────── PipelineResult ──────────────────┤               │
  │<────────────任务状态与阶段结果───────────────┤               │                │               │
```

上述时序中的每次指针切换和门禁开放都由 SQLite 事务完成；Parquet 发布使用同文件系统临时文件加原子
重命名。数据库不得登记尚未完成写入和完整性校验的文件。

## 4. Canonical 数据集

| 数据集 | 主键 | 说明 |
| --- | --- | --- |
| `instrument` | `instrument_id` | 证券主数据、板块、上市生命周期 |
| `trade_calendar` | `trade_date` | 开市和休市状态 |
| `daily_bar` | `instrument_id, trade_date` | 未复权 OHLCV 和成交额 |
| `daily_basic` | `instrument_id, trade_date` | 估值和换手数据 |
| `security_status` | `instrument_id, trade_date` | 上市、停牌、ST 和可交易状态 |
| `corporate_action` | `instrument_id, ex_date, action_type` | 分红、送转和除权除息事件 |
| `financial_observation` | `instrument_id, report_period, metric, revision` | PIT 财务及供应商重述 |
| `industry_classification` | `as_of_date, instrument_id, taxonomy` | 按请求日期重建的行业状态 |
| `index_bar` | `index_id, trade_date` | 指数行情，包括基准和全收益基准 |

### 4.1 通用 PIT 审计列

每个 Canonical 数据集尾部包含：

```text
source                # 供应商
available_at          # 业务上最早可用于决策的时间，PIT 截断依据
availability_source   # available_at 的确定依据
pit_usable            # 是否足以支持 PIT；false 时保留供审计但不参与 PIT
ingested_at           # 本地抓取时间，仅用于血缘，不替代 available_at
```

### 4.2 公司行为

`corporate_action` 至少包含：

```text
instrument_id, ex_date, action_type(CASH_DIVIDEND|STOCK_DIVIDEND|SPLIT|...),
cash_per_share, share_ratio, announced_at, available_at, pit_usable
```

它有两个消费方向：

1. **复权计算**：Canonical 提供前复权、后复权和不复权入口，前复权因子由公司行为事件推导。
2. **回测账务**：回测引擎按 `ex_date` 派发现金红利（`DIVIDEND` ledger 事件）并调整送转股数，
   避免 NAV 在除权除息日因未复权价格跳变而失真。数据层首版即产出该数据集，回测侧消费属于 P3b-1。

## 5. 复权语义

- Canonical `daily_bar` 存储**未复权**价格，保证撮合使用真实成交价格。
- `AdjustmentService` 由 `corporate_action` 推导前复权因子，供因子和信号计算使用前复权序列。
- **口径分离铁律**：回测撮合使用未复权价格和公司行为账务；因子信号使用前复权价格，二者不可混用。

## 6. PIT 与研究读取

- 研究代码只能通过 `CanonicalResearchRepository` 读取，禁止旁路扫描 Raw 或 Canonical 路径。
- 读取按 `as_of / signal_date` 截断，物理上不返回 `available_at > 截断` 或 `pit_usable = false` 的行。
- 财务：`financials_as_of` 选择信号日已知的最新 revision；`financial_history` 保留截止时点全部 revision，
  不使用最终修订回填早期信号日。
- 行业：按请求日期重建 as-of 状态，从首次出现该状态的 `as_of_date` 起可见，
  不回写更早的 `supplier_update_date`。

## 7. 质量门（VALIDATE）

阻断规则如下：

```text
FATAL : 必需数据集缺失或为空、交易日历无开市日、Canonical Schema 不符、
        跨分区 Schema 不一致、主键重复
SEVERE: 必填值为空、OHLC 关系错误、成交量为负、交易日覆盖缺口、证券未知、
        财务可用时间缺失或倒置、公司行为除权日与行情跳变不一致
```

- 质量规则必须符合真实市场语义，例如停牌日 turnover 为空不能误报。
- 结果按“规则 × 数据集”记录 `PASS / FAIL / SKIPPED`；只有全目录通过才能开放研究读取门。

## 8. 数据身份

- 维护由全部当前 Canonical 数据集身份确定性汇总得到的 `catalog_hash`，仅表示当前 Canonical 状态。
- `validated_catalog_hash` 记录最近一次通过 `validate-all` 的目录身份；它必须与当前 `catalog_hash`
  相等，研究读取门才开放。
- 目录身份只服务于实验运行的**运行内一致性门**：运行中数据变化时当前运行失败。
- 不保存历史版本，不提供回放，也不把 `data_hash` 纳入跨运行实验复现身份。

## 9. 存储与性能

- Raw 和 Canonical 使用 Parquet；日频数据按 `year=` 分区，日更只重建输入发生变化的年度分区。
- LOCALIZE 按 request 去重并支持断点续抓；CURATE 按分区输入身份变化增量重建。
- 使用 DuckDB/Polars 向量化执行，进行投影下推并尽早过滤。
- 年分区使日更成本随年内进度增长并在跨年后清零；如果年底日更成为瓶颈，可调整为月分区。
  暂不实现复杂的 append-only 物理追加。

## 10. 数据源隔离

```text
SourceClient(Protocol): login/logout/fetch_* → RawBatch
CanonicalMapper(Protocol): RawBatch → CanonicalBatch（供应商字段 → Canonical Schema）
```

一期实现 BaoStock；后续增加 TuShare 适配器时不修改上层。ETF 行情、指数、行业和财务端点的
供应商差异全部吸收在 Mapper 层。

## 11. 测试契约

- **PIT**：财务和行业修订不回填旧信号日；`CanonicalResearchRepository` 物理截断有效；
  公司行为可见性时点正确。
- **质量**：每条规则具有字面量 oracle；合法市场异常不误报。
- **复权和公司行为**：前复权因子由事件正确推导；除权日行情跳变与事件能够对账。
- **确定性**：排序稳定，不依赖集合、文件系统或数据库未声明的顺序。
- **隔离**：模拟 TuShare Mapper 产出与 BaoStock 一致的 Canonical Schema。

## 12. 明确不做

- 数据快照版本和历史 catalog 回放。
- 跨运行数据复现，包括 source/env 指纹和旧数据回放；Raw/Canonical 内容寻址哈希继续保留，
  因为它们服务于去重、增量和完整性。
- 分钟、Tick、盘口和实时行情。
- 北交所以外范围限制；市场范围按统一证券目录和能力契约控制，不再人为设限。
