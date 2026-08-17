# 因子研究与分析总体设计

## 1. 文档定位

本文定义独立因子研究子系统的整体架构、领域模型、计算语义、运行身份、产物契约和验收标准。它是因子研究的规范性设计，不是对既有系统的补丁清单或功能路线图。

本文使用以下约束词：

- **必须**：生产实现和研究产物不可违反的契约。
- **不得**：会破坏 PIT、可复现性、数据身份或审计闭环的行为。
- **可以**：在不改变核心身份和统计语义的前提下允许选择的实现方式。

因子研究由项目原生实现。Polars、Arrow 和 NumPy 可以进入生产计算路径；Qlib、SciPy 等第三方研究库只用于开发期数值交叉验证，不进入 Worker 组合根、数据身份或正式产物链。

## 2. 目标与边界

### 2.1 目标

因子研究子系统回答以下问题：

1. 因子在什么证券和日期上可计算，覆盖率是否稳定？
2. 因子值与未来收益是否存在稳定、显著且方向一致的截面关系？
3. 分层收益是否单调，多空收益是否稳定？
4. 预测能力是否依赖特定时期、市值、波动或流动性环境？
5. 信号经过策略实际变换后，预测能力、换手和容量如何变化？
6. 任一结论能否追溯到数据、股票池、代码、配置和不可变产物？

### 2.2 非目标

因子研究不负责：

- 生成真实持仓或订单；
- 模拟撮合、滑点、佣金和完整交易规则；
- 取代组合构建与策略回测；
- 发布跨运行共享的因子缓存；
- 绕过 Canonical 数据门禁读取 Raw 或磁盘分区；
- 根据样本内结果自动修改因子方向、参数或策略权重。

因子研究诊断预测关系；策略回测验证从信号到组合、交易和净值的完整实现。两者可以共享因子和信号变换实现，但产物身份与结论必须分开。

## 3. 系统上下文与依赖方向

```text
CLI / Dashboard
      │
      ▼
ResearchApplicationService
      ├── FactorStudyStore
      └── TaskQueue
               │
               ▼
        FACTOR_ANALYSIS task
               │
               ▼
FactorAnalysisHandler
      ├── CanonicalResearchRepository
      ├── UniverseBuilder
      ├── FactorRegistry / FactorEngine
      ├── FactorStudyAnalyzer
      ├── ArtifactPublisher
      └── FactorStudyStore
```

模块职责如下：

| 模块 | 职责 |
|---|---|
| `factor_studies.models` | 研究配置、状态和稳定枚举。 |
| `factor_studies.contracts` | 消费者侧持久化 Protocol。 |
| `factor_studies.analysis` | 未来收益标签组装与一次研究的分析编排。 |
| `factors` | 因子定义、注册、分区计算和通用统计内核。 |
| `application.research` | 创建研究、捕获运行身份并提交任务。 |
| `application.factor_studies` | Worker 用例、阶段推进、漂移检查和发布。 |
| `infrastructure.persistence.factor_studies` | SQLite 研究与运行仓储。 |
| `bootstrap` | 装配真实 Repository、Store、Queue 和 Worker。 |

应用层只依赖能力包的模型和 Protocol；SQLite、SQLAlchemy、文件系统和供应商实现由基础设施层提供，并在组合根中装配。

## 4. 核心不变量

1. 研究数据只能通过 `CanonicalResearchRepository` 读取。
2. 提交运行前必须通过全局数据门禁，并捕获当前 `catalog_hash`。
3. 运行期间任何数据身份漂移都必须使运行失败，不能自动切换到新数据。
4. 因子由唯一 `factor_id` 标识，不使用 `id@version`。
5. 因子每次运行重新计算，不持久化跨运行缓存。
6. 因子结果必须绑定 `catalog_hash`、`universe_hash`、因子代码哈希和日期范围。
7. 财务、状态和分类数据必须遵守 `available_at`；无法满足 PIT 的输入必须显式披露。
8. 未来收益窗口必须严格位于信号日之后。
9. 缺失、不可交易和窗口不完整的样本不得静默丢弃。
10. 同一输入重复运行必须得到相同排序、schema、内容哈希和统计结果。
11. 只有通过最终产物验证的运行才能进入 `SUCCEEDED`。

## 5. 领域模型与身份

### 5.1 FactorStudy

`FactorStudy` 是一个命名研究定义，保存严格校验、确定性序列化的 `FactorStudyConfig`。研究本身不代表一次计算结果；同一研究可以产生多个不可变运行，用于比较不同数据时点或源码状态。

研究身份包含：

- `study_id`；
- 名称；
- 规范化配置 JSON；
- `config_hash = SHA256(canonical_json(config))`；
- 创建时间。

配置创建后不得原地修改。改变日期、因子、标签、变换或分析语义时必须创建新的研究定义。

### 5.2 FactorRun

`FactorRun` 是一次不可变执行，创建时复制研究配置并捕获：

```text
run_id
study_id
task_id
config_hash
catalog_hash
source_hash
status
manifest_path
manifest_hash
error
created_at / started_at / completed_at
```

`source_hash` 来自提交时捕获的源码环境身份。因子执行描述还必须记录每个因子的逻辑 ID、参数、依赖和代码哈希。

运行的完整可复现身份为：

```text
run_identity = SHA256(canonical_json({
  config_hash,
  catalog_hash,
  universe_hash,
  source_hash,
  execution_descriptor_hash,
  label_definition_hash,
  transform_definition_hash,
  analyzer_definition_hash
}))
```

`run_id` 是记录标识；`run_identity` 才是研究输入与语义的内容身份。两个不同 `run_id` 可以拥有相同 `run_identity`，用于验证确定性，但不得共享或覆盖彼此的发布目录。

### 5.3 状态机

```text
CREATED → QUEUED → RUNNING → SUCCEEDED
                         ├→ FAILED
                         └→ CANCELLED
```

- 创建运行后，只有成功绑定任务才能进入 `QUEUED`。
- 状态迁移必须带期望前态，以乐观并发方式更新。
- `SUCCEEDED` 必须同时保存 Manifest 路径和哈希。
- 失败只保存结构化错误码和安全上下文，不把敏感路径或完整异常对象写入公开响应。
- 重试创建新运行和新任务，不复写旧运行与旧产物。

## 6. 生产研究 Profile

当前股票因子研究 Profile 固定为：

| 项目 | 定义 |
|---|---|
| 股票池 | `CN_STOCK_STANDARD`，按每个信号日独立构建。 |
| 因子 | 七个股票 Alpha，可选择非空子集。 |
| 持有期 | 1、5、20 个交易所会话。 |
| 标签 | `T1_OPEN_TO_TH_CLOSE`。 |
| 分位组 | 5 组。 |
| 最小有效截面 | 30。 |
| 信号形态 | `DIRECTION_ADJUSTED`。 |
| IC 滚动窗口 | 最近 20 个信号日。 |
| IC 滚动最小有效数 | 10。 |
| 描述分位数 | P05、P25、P50、P75、P95，线性插值。 |
| 可选行业研究 | `证监会行业分类`；同一运行并列生成方向调整基线和 PIT 行业中性化版本。 |

行业研究通过严格可选配置启用：

```yaml
industry:
  taxonomy: 证监会行业分类
  unclassified_policy: EXCLUDE
```

未配置时不得读取行业数据。配置后，研究日期必须完全位于 Canonical 行业覆盖范围内；taxonomy 当前只允许“证监会行业分类”，未分类策略只允许 `EXCLUDE` 或 `UNCLASSIFIED`，全部语义进入 `config_hash`。

七个因子的计算口径为：

| `factor_id` | 计算口径 |
|---|---|
| `earnings_yield_ttm` | `1 / pe_ttm`；PE 有限且非零时有效，负 PE 保留负倒数。 |
| `book_to_price_mrq` | `1 / pb_mrq`；PB 有限且非零时有效，负 PB 保留负倒数。 |
| `roe_pit` | 信号日已知的最大报告期及该报告期最新修订；当前修订披露后 190 个自然日内有效。 |
| `momentum_120_20` | 交易所会话 `t-120` 至 `t-20` 的累计对数收益转简单收益。 |
| `volatility_60d` | 最近 60 个会话对数收益的样本标准差，`ddof=1`，乘 `sqrt(252)`。 |
| `downside_volatility_60d` | 最近 60 个会话 `min(r, 0)` 的均方根，乘 `sqrt(252)`。 |
| `max_drawdown_120d` | 最近 120 个会话价格路径的最大峰谷回撤。 |

市场窗口统一按交易所会话计数。Canonical 中明确存在且成交量、成交额均为空的停牌占位行记为零收益；证券在某交易日完全缺少 Canonical 行情时，该会话收益为空并使覆盖它的窗口失效。历史不足时直接无效，不得回退到不完整窗口。

`roe_pit` 的 190 日计龄从当前有效修订的 `available_at` 上海本地日期开始；当前最新报告值非有限时，不得回退到旧报告。

## 7. 信号模型

因子值、统一方向后的研究信号和策略实际使用的信号是不同对象，必须使用不同身份：

| 信号形态 | 定义 |
|---|---|
| `RAW` | 因子实现直接输出的原始值。 |
| `DIRECTION_ADJUSTED` | 原始值乘因子方向，使数值越大统一表示预期越优。 |
| `INDUSTRY_NEUTRALIZED` | 在方向调整后，按信号日、因子和 PIT `industry_code` 对有效样本执行等权组内去均值。 |
| `STRATEGY_TRANSFORMED` | 按策略真实顺序完成 MAD 去极值、截面标准化、方向调整和类别聚合。 |

信号变换定义必须是不可变 DTO，至少包含：

- 信号形态；
- 因子方向；
- 去极值方法与参数；
- 标准化方法；
- 变换顺序；
- 变换实现代码哈希。

`STRATEGY_TRANSFORMED` 必须调用策略共享的公共变换实现，禁止在分析模块复制近似算法。

生产策略的固定变换顺序为 `MAD 去极值 → 截面标准化 → 方向调整 → 类别聚合`，本次行业研究不会改变策略。因子研究显式启用行业时，Worker 对全部证券和信号日只调用一次 Repository 批量 as-of 入口，并同时保留 `DIRECTION_ADJUSTED` 和 `INDUSTRY_NEUTRALIZED`。`EXCLUDE` 排除 tombstone 和没有历史状态的样本；`UNCLASSIFIED` 将二者放入稳定未分类组。单成员行业组以 `SINGLE_MEMBER_INDUSTRY` 失效，禁止用后续状态回填早期信号日。来源语义、事件模型和重述门禁见[行业分类 PIT 整体设计](industry-classification-pit-design.md)。

## 8. 股票池与因子计算

### 8.1 日度股票池

股票池按每个信号日构建，结果主键为：

```text
signal_date, instrument_id
```

至少包含 `eligible` 和规则诊断。研究计算范围是研究区间内至少一天进入股票池的证券并集，但每个信号日只允许 `eligible=true` 的样本进入截面统计。

`universe_hash` 必须由以下内容确定性计算：

- `config_hash`；
- 排序后的全部 `(signal_date, instrument_id, eligible, rule evidence)`；
- 股票池规则内容哈希。

### 8.2 分区计算

证券并集按规范化 `instrument_id` 排序后确定性分区。每个分区：

- 共享一次 Daily Basics 输入读取；
- 共享一次 session-complete 市场输入读取；
- `roe_pit` 只读取一次财务修订历史，查询次数不得随信号日数量增长；
- 生成只属于本次运行的 `FactorArtifact`；
- 绑定同一个 `catalog_hash`、分区股票池哈希和代码哈希。

分区大小是性能参数，不得改变最终排序、内容或统计结果。

### 8.3 因子输出契约

每个因子输出统一为：

```text
trade_date
instrument_id
factor_id
value
available_at
is_valid
```

分析前将 `trade_date` 映射为 `signal_date`，再与日度股票池内连接。`is_valid` 必须同时满足因子自身有效和当日股票池有效。

## 9. 未来收益标签

### 9.1 标签定义

持有期 `h` 的基础标签固定为：

```text
entry  = signal_date 后第 1 个交易所会话的 open
exit   = signal_date 后第 h 个交易所会话的 close
return = exit / entry - 1
```

标签行主键为：

```text
signal_date, instrument_id, horizon, label_kind
```

至少记录：

```text
return_start
return_end
future_return
is_valid
invalid_reason
```

### 9.2 标签种类

| 标签种类 | 用途 |
|---|---|
| `THEORETICAL_FORWARD_RETURN` | 诊断价格序列上的纯预测关系，不模拟完整成交约束。 |
| `EXECUTABLE_FORWARD_RETURN` | 要求入场方向可成交且退出价格有效，用于诊断可实现关系。 |

同一次研究可以同时发布两种标签，但统计结果必须以 `label_kind` 分区，不能混合样本。

### 9.3 无效原因

无效标签必须保留行和稳定原因码：

```text
INCOMPLETE_FORWARD_WINDOW
NOT_LISTED_AT_ENTRY
ENTRY_SUSPENDED
ENTRY_LIMIT_UP
ENTRY_LIMIT_DOWN
MISSING_ENTRY_PRICE
MISSING_EXIT_PRICE
DELISTED_WITHOUT_EXIT_PRICE
NONFINITE_RETURN
```

原因判断采用固定优先级。不得仅用 `future_return=null` 表达全部失败，也不得在 join 或聚合中静默删除无效样本。

## 10. 分析模型

### 10.1 覆盖率

覆盖率按因子和信号日计算：

```text
coverage = valid_factor_count / eligible_count
```

输出必须包含 `eligible_count`、`valid_count`、`coverage`、`is_valid` 和 `invalid_reason`。有效因子数低于最小截面时，该日因子截面整体无效。

### 10.2 IC

每个因子、标签种类、持有期和信号日分别计算：

- Pearson IC；
- Spearman Rank IC，并列值使用平均秩；
- 最近 20 个信号日滚动均值；
- 累计 IC；
- 有效配对数量和无效原因。

日度 IC 的无效原因按以下优先级判断：

```text
INSUFFICIENT_CROSS_SECTION
INSUFFICIENT_FORWARD_PAIRS
ZERO_FACTOR_VARIANCE
ZERO_RETURN_VARIANCE
NONFINITE_IC
```

累计 IC 只累加有效日；无效日沿用此前累计值，首个有效日前为空。正值率只统计 `IC > 0`，零值和无效日中断连续正负区间。

摘要按因子、标签种类和持有期发布：

- 均值；
- 样本标准差，`ddof=1`；
- 未年化 ICIR；
- 正值率；
- P05、P25、P50、P75、P95；
- 有效日期数；
- 最长连续正值与负值区间。

5 日和 20 日标签存在重叠，显著性模块必须使用 Newey–West/HAC 或 block bootstrap 处理序列相关，并发布 t-stat、置信区间、p-value 和有效样本量。多因子同时检验时必须记录 Bonferroni 或 Benjamini–Hochberg FDR 校正结果。

### 10.3 分位数组

分位数分组必须在配置中记录并列策略：

| 策略 | 语义 |
|---|---|
| `STABLE_SPLIT` | 相同值按 `instrument_id` 稳定拆分。 |
| `KEEP_TIES` | 相同值留在同组，允许组大小不均。 |
| `PERCENTILE_BOUNDARY` | 使用明确分位边界划分。 |

每个因子、标签种类、持有期、信号日和分位组输出实际边界、样本数、均值收益及有效状态。终端分位组为空、配对数量不足或分组域不完整时必须给出原因码。

分层分析包括：

- 各组日收益和累计净值；
- 分组收益单调性；
- 分组序号与收益相关性；
- 最高组减最低组收益；
- 多空胜率、年化收益、波动率、Sharpe 和最大回撤；
- 多空收益显著性。

### 10.4 相关性与风险暴露

因子相关性使用同日同股票池的有效截面计算，至少发布：

- 全期平均秩相关矩阵；
- 日度相关分布；
- 高相关告警；
- 正交化后的增量 IC。

风险暴露诊断覆盖对数流通市值、Beta、波动率和流动性。暴露输入必须满足与信号相同的 PIT 截止；非 PIT 输入只能进入显式披露的敏感性分析。

### 10.5 稳定性、换手与容量

稳定性切片可以按年度、季度、市场状态、上市板块、市值、波动率和流动性划分。所有切片继续绑定同一个运行身份，并记录确定性切片规则。

交易属性分析包括：

- 因子秩自相关；
- 分位组与 Top-N 成员换手率；
- 分位组迁移矩阵；
- 成交额覆盖率和预计市场参与率；
- 参数化成本前后收益；
- 不同资金规模下的容量曲线。

这些结果是信号层诊断，不替代回测引擎的实际成交结果。

### 10.6 组合信号与研究治理

组合信号可以采用等权、类别权重、滚动 IC 权重或滚动 ICIR 权重。组合分析必须同时发布：

- 单因子结果；
- 组合结果；
- 单因子剔除实验；
- 因子边际贡献；
- 权重敏感性；
- 相对单因子的增量 IC。

样本外研究使用明确的 `TRAIN → VALIDATION → TEST` 区间。测试区间不得用于选择因子、修改方向、调整窗口或优化权重。研究身份还必须记录尝试过的因子数、参数组合数、多重检验方法和最终选择理由。

## 11. 端到端执行流程

完整运行固定为：

```text
VALIDATE
→ UNIVERSE
→ FACTORS
→ RETURNS
→ ANALYZE
→ PUBLISH
→ ARTIFACT_VERIFY
```

### 11.1 VALIDATE

- 读取运行记录并核对任务 `run_id`、`task_id` 和 `config_hash`。
- 要求当前 Catalog 已通过 `validate-all`。
- 核对当前 `catalog_hash` 与运行捕获值一致。
- 校验交易日历覆盖研究区间及最长未来窗口。
- 校验供应商能力满足所有因子依赖。
- 显式启用行业时，校验研究区间完全位于行业 Canonical 覆盖范围内。

### 11.2 UNIVERSE

- 为每个信号日构建股票池。
- 形成研究区间证券并集。
- 生成 `universe_hash` 和股票池质量摘要。

### 11.3 FACTORS

- 按证券稳定分区。
- 注册本次运行需要的因子。
- 每次运行重新计算，不读写跨运行因子缓存。
- 生成执行描述和每个 `FactorArtifact.content_hash`。
- 应用配置指定的信号形态。
- 行业研究通过一次批量查询完成信号日状态对齐，并生成基线和行业中性化版本。

### 11.4 RETURNS

- 将交易日历扩展到最大持有期结束日。
- 读取前复权价格和证券交易状态。
- 构建理论或可执行未来收益。
- 保留无效标签及原因，并计算标签表内容哈希。

### 11.5 ANALYZE

- 计算覆盖率、IC、分位数组、多空收益和相关性。
- 按 `signal_variant` 独立计算并排序，禁止在同一截面混合两个版本。
- 按配置执行显著性、稳定性、风险、换手、容量或组合模块。
- 所有表按稳定主键排序。

### 11.6 PUBLISH

- 写入同一文件系统中的临时目录。
- 使用 Zstandard 压缩 Parquet 和确定性 JSON。
- 生成 Manifest 后原子重命名为最终目录。
- 最终目录已存在时失败，禁止覆盖。

### 11.7 ARTIFACT_VERIFY

- 从最终目录重新读取 Manifest 和每个文件。
- 验证路径边界、SHA-256、字节数、schema、行数和主键唯一性。
- 验证运行、数据、股票池、因子、标签和分析器身份一致。
- 只有全部验证通过后，运行才能转换为 `SUCCEEDED`。

每个阶段边界都必须检查取消请求。取消和失败不得留下可见的半成品目录。发布前必须再次读取 Catalog；数据身份漂移立即失败。

## 12. 产物契约

目录结构为：

```text
<artifact_root>/factor-studies/<study_id>/<run_id>/
├── summary.parquet
├── coverage.parquet
├── ic.parquet
├── quantile_returns.parquet
├── long_short_returns.parquet
├── correlation.parquet
├── industry_coverage.parquet
├── study_config.json
├── environment.json
└── manifest.json
```

启用扩展分析模块时，可以增加：

```text
significance.parquet
stability.parquet
exposure.parquet
rank_autocorrelation.parquet
quantile_turnover.parquet
quantile_transition.parquet
cost_adjusted_returns.parquet
capacity.parquet
composite_signals.parquet
```

文件是否存在必须由配置和 Manifest 决定，读取端不得猜测。

### 12.1 核心表主键

| 文件 | 逻辑主键 |
|---|---|
| `summary.parquet` | `signal_variant, factor_ref, horizon` |
| `coverage.parquet` | `signal_variant, factor_ref, signal_date` |
| `ic.parquet` | `signal_variant, factor_ref, horizon, signal_date` |
| `quantile_returns.parquet` | `signal_variant, factor_ref, horizon, signal_date, quantile` |
| `long_short_returns.parquet` | `signal_variant, factor_ref, horizon, signal_date` |
| `correlation.parquet` | `signal_variant, factor_x, factor_y` |
| `industry_coverage.parquet` | `signal_date`；未启用行业时为固定 Schema 空表。 |

时序诊断表统一包含：

```text
sample_count
is_valid
invalid_reason
```

### 12.2 Manifest

`manifest.json` 至少包含：

```text
run_id
run_identity
study_id
config_hash
catalog_hash
universe_hash
source_hash
execution_descriptor
factor_artifact_hashes
label_definition
label_hashes
signal_transform_definition
analyzer_definition_hash
quality_disclosures
environment_identity
entries
```

每个 `entries` 项必须记录相对路径、SHA-256、字节数、schema 和行数。Manifest 自身使用确定性 JSON 序列化，其 SHA-256 保存到 `factor_run.manifest_hash`。

## 13. API 与用户交互

Dashboard 和 CLI 都通过应用层用例操作研究，不直接创建 ORM 对象或后台 Worker。

核心操作包括：

- 创建研究定义；
- 分页列出研究；
- 查看研究及运行历史；
- 基于当前数据和源码身份提交新运行；
- 查看运行状态、任务进度和结构化失败原因；
- 从运行中心按任务绑定的 ``run_id`` 深链到精确的不可变运行，不回退到所属研究的最新运行；
- 读取已验证产物并展示摘要、时序和矩阵。

创建运行时必须先持久化运行，再以 `factor-run-<run_id>` 作为任务幂等键入队，最后绑定 `task_id`。接口响应只返回标识、状态和安全摘要，不暴露可信根之外的文件路径。

Dashboard 任务列表和详情统一返回可空的 ``factor_run_id``：仅合法
``FACTOR_ANALYSIS`` payload 可以暴露其中的非空 ``run_id``，列表不得因此暴露完整
payload。因子分析页以 ``/factors?run=<run_id>`` 作为精确运行深链；成功、运行中、
失败和历史运行都必须保留任务与运行的一一对应关系。

Dashboard 展示必须明确标注：

- 信号形态；
- 行业研究启用状态、taxonomy、未分类策略及基线/行业中性化版本；
- 每个信号日的已分类、tombstone、无历史状态和策略后可用覆盖率；
- 标签种类和收益区间；
- Pearson 或 Rank IC；
- 持有期；
- 有效样本数与覆盖率；
- PIT 或非 PIT 质量披露；
- 样本内、验证集和测试集区间。

## 14. 性能与资源模型

全市场长区间分析不得按证券或日期执行 Python 级主循环。实现优先级为：

1. Polars 表达式、窗口函数和 LazyFrame 查询规划；
2. 投影下推和尽早过滤；
3. Arrow 批次传递；
4. 经过基准验证的 NumPy 数值内核。

Python 循环只允许用于有明确上界的配置、持有期或分区编排；不得按全量 `(date, instrument)` 行循环构造生产标签。

运行日志必须记录：

- 每阶段耗时；
- 峰值内存；
- 信号日、证券、因子、持有期和有效样本数量；
- 分区大小与分区数量；
- Canonical 查询次数；
- 产物总字节数。

性能验收覆盖 20 年全市场等规模数据，并分别报告 `FACTORS`、`RETURNS` 和 `ANALYZE`。改变分区大小后，产物内容哈希必须保持一致。

## 15. 失败、恢复与并发

- 同一运行只能绑定一个任务。
- Worker 领取任务后以心跳和任务尝试记录运行状态。
- 运行状态迁移冲突必须失败，不允许最后写入者覆盖。
- 失败任务可以重试，但必须创建新 `FactorRun`。
- 临时发布目录可在失败后安全清理；已发布目录不可原地修复。
- Manifest 存在但运行未成功时，恢复流程必须重新执行完整验证，不能直接登记成功。
- 数据漂移、源码漂移、产物校验失败和取消必须使用不同错误码。

## 16. 测试与验收

### 16.1 数值正确性

- 使用硬编码字面量 oracle 验证 Pearson IC、Rank IC、分位数组和多空收益。
- 覆盖并列排名、常数截面、单样本、NaN、Inf、空组和零方差。
- 验证滚动窗口按信号日而非自然日或有效 IC 日推进。
- 验证 HAC、block bootstrap 和多重检验校正。
- 第三方库 parity 测试只能作为额外证据，不能替代项目自身 oracle。

### 16.2 PIT 与标签

- 未来收益起点严格晚于信号日。
- 财务修订只能在 `available_at` 之后可见。
- 行业事件只能从首次出现该状态的 `as_of_date` 起可见；同一 `supplier_update_date` 不得导致回写。
- T+1 停牌、涨跌停、缺价、未上市和退市必须得到稳定原因码。
- 不完整未来窗口不得静默消失。
- 未声明行业依赖的因子和股票策略不得读取行业分类；显式依赖用例必须记录 taxonomy、未分类策略、覆盖率和 `BAOSTOCK_AS_OF_DATE_RECONSTRUCTED`。

### 16.3 身份与产物

- 改变数据、股票池、因子代码、配置、标签或变换必须改变运行身份。
- 任一文件内容、schema、行数或路径变化必须使验证失败。
- Manifest 路径越界、缺文件和未知文件必须失败。
- 未通过 `ARTIFACT_VERIFY` 的运行不得登记成功。
- 相同输入重复运行产生相同内容哈希。

### 16.4 架构与性能

- 单元测试禁止真实网络访问。
- 应用层测试使用 fake Protocol，不依赖 SQLite 或供应商 SDK。
- BaoStock 和持久化实现只在集成测试中装配。
- 性能测试验证查询次数不随信号日线性增长。
- 全市场验收记录固定机器环境、最慢阶段和峰值内存。

## 17. 完成定义

因子研究子系统只有同时满足以下条件才形成可信闭环：

- 配置、数据、股票池、源码、因子、标签、变换和分析器均具有稳定身份；
- 研究数据只来自通过门禁的 Canonical Repository；
- 原始信号、方向调整信号和策略信号不会混用；
- 不可交易和无效样本有完整原因码，不发生静默删除；
- 重叠收益的显著性处理序列相关；
- 研究切片和组合选择记录样本外边界与多重检验；
- 产物原子发布并在成功登记前完成独立验证；
- 相同输入的结果可复现，分区和并发参数不改变内容；
- 全市场长区间的运行时间、内存和查询次数达到记录在案的验收目标。
