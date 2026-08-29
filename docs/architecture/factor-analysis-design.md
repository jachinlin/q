# 独立因子研究设计

文档状态：当前有效设计　·　日期：2026-08-28

因子研究是独立的 `FactorStudy` 任务，不属于 Experiment，也不存在 Run、Baseline、派生 Run
或 Run 比较。实验中心只承载策略回测。一次成功研究的配置、数据身份和产物永久冻结；参数变化
必须创建新的研究。

## 配置与身份

研究配置使用严格、扁平的最终契约：

```yaml
name: 价值动量因子研究
description: 候选因子诊断
tags: [factor]
start_date: 2018-01-01
end_date: 2022-12-31
correction: BH_FDR
factor_ids: [book_to_price_mrq, momentum_120_20]
universe: {name: CN_STOCK_STANDARD}
horizons: [1, 5, 20]
quantiles: 5
industry:
  taxonomy: SW2021
  unclassified_policy: EXCLUDE
market_cap:
  exposure: LOG_TOTAL_MARKET_VALUE
cost_bps_scenarios: [5, 10, 20]
```

配置没有 `kind`、`initial_run`、`factor_study`、`sample_windows` 或 `governance` 包装层。
日期必须明确；因子、期限和成本情景唯一且确定性排序。规范 YAML 及其 SHA-256 在提交前由
同一个解析器生成。提交时捕获 `catalog_hash`，四个阶段前后均检查该身份：

```text
VALIDATE → PREPARE_INPUTS → ANALYZE_FACTORS → PUBLISH
```

失败、取消和失联恢复复用同一个 `FACTOR_STUDY` Task，并创建新的 attempt；成功研究不可重跑。

### 任务进度与诊断日志

任务 `progress.completed/progress.total` 始终表示上述四个公开阶段，阶段开始和完成分别写入
一次进度，成功终态固定为 `PUBLISH 4/4`。`ANALYZE_FACTORS` 内部进度不得把顶层计数替换为
交易日数量，而是在 `progress.context` 使用以下稳定字段：

- `substage`：`BUILD_UNIVERSE`、`COMPUTE_FACTORS`、`BUILD_SIGNALS`、
  `LOAD_LABEL_INPUTS`、`BUILD_FORWARD_RETURNS`、`ANALYZE_STATISTICS`、
  `BUILD_METRICS`、`PUBLISH_ARTIFACTS` 或 `REGISTER_OUTPUTS`；
- `substage_state`：`STARTED`、`PROGRESS` 或 `COMPLETED`；
- `item_completed`、`item_total` 和 `signal_date`：仅用于可计数的股票池准备进度；
- 行数、证券数、因子数、产物数、字节数和身份哈希等已脱敏规模证据；
- `last_completed_substage` 与 `last_completed_evidence`：供 Dashboard 稳定显示最近完成节点。

股票池逐日准备只记录首项、末项及跨越 5% 桶的确定性里程碑，长区间最多产生约 21 条
中间进度。Task API 直接返回上述 `progress.context`；任务 JSON Lines 的 `task.progress` 将其
保存于 `context.details`。失败事件 `task.handler_failed.context.last_progress` 保存最后一次安全
进度，Dashboard 诊断同时暴露公开阶段和可空的 `substage`。因子研究详情与运行中心通过同一
展示契约读取 `/api/v1/tasks/{task_id}`，终态后停止轮询。

`BUILD_UNIVERSE` 对研究区间内的停牌和风险警示各执行一次范围读取，再按每行交易日对应的
上海日终重新应用 `available_at` 截止，禁止把区间末日才可见的状态带回历史日期。交易日与股票
通过 Polars 按上述 5% 里程碑分批向量化连接；批次只承担进度与取消边界，拼接结果仍按
`signal_date, instrument_id` 稳定排序。股票池成员的 SHA-256 必须与完整 canonical JSON 成员
列表逐字节一致，不得因批量化改变数据身份。

股票池哈希流式吸收包含原因码的完整批次；哈希完成后，常驻股票池只保留
`signal_date, instrument_id, eligible`，原因码不得继续占用分析阶段内存。

## PIT 输入与分析口径

行情、证券状态、行业和财务数据只经 `CanonicalResearchRepository` 读取。每次研究重新计算
因子，因子值先乘 `FactorSpec.direction`，之后统一以最高分位为多头、最低分位为空头。

- `THEORETICAL_FORWARD_RETURN`：T+1 开盘到 T+h 收盘，两个端点价格有效即可。
- `EXECUTABLE_FORWARD_RETURN`：在理论口径上，T+1 入场还必须已上市、未停牌且不是一字涨停。
- 始终发布 `DIRECTION_ADJUSTED`。只配置行业时增加 `INDUSTRY_NEUTRALIZED`；只配置
  `market_cap` 时增加 `MARKET_CAP_NEUTRALIZED`；两者都配置时只增加
  `INDUSTRY_MARKET_CAP_NEUTRALIZED`，不同时发布两个单项版本。
- 行业中性化保持等权组内去均值；`EXCLUDE` 排除无法分类样本，`UNCLASSIFIED` 将其放入
  固定未分类组。市值中性化以当日 PIT 可见的正数 `total_market_value` 的自然对数为暴露，
  计算带截距的一元等权截面回归残差。联合版本先将因子和对数市值行业内中心化，再回归
  市值暴露，等价于行业固定效应加市值暴露。市值和联合中性化使用 Polars 窗口表达式一次
  向量化完成有效性筛选、行业中心化、缩放回归与原因赋值，禁止转为逐行 Python 列表。
- 缺失、非有限或非正市值、行业缺失、单成员行业、截面不足和市值暴露零方差都生成稳定
  无效原因；不得用原因子值回填。计算按证券稳定排序并使用确定性求和。

Pearson IC、Rank IC、毛多空 spread 和成本情景净 spread 使用 Bartlett kernel 的
Newey–West/HAC 推断。多重检验按 Rank IC 与毛多空 spread 两个 family 分别应用
`BONFERRONI` 或 `BH_FDR`。研究不包含 TRAIN/VALIDATION/TEST、test budget、分段稳定性或
自动候选评分。

统计内核对每个“信号版本 × 因子”只执行一次稳定分位分配，同一分位结果复用于换手和全部
远期收益标签。日度 IC 先完成一次键对齐，再按有序日期分区调用相同的 NumPy Pearson 与平均
秩算法；分层收益和标签质量使用 Polars 分组聚合。各标签保持确定性顺序逐张处理并释放连接
中间表，禁止拼接为统一超大标签表。向量化不得改变输出 Schema、主键排序、空分位、有效性
原因优先级或统计公式；浮点归约仅允许机器精度范围内的末位差异。

Worker 使用数据根下的 `tmp/factor-studies/<factor_study_id>` 作为一次任务独占的临时边界。
`COMPUTE_FACTORS` 按确定性因子顺序逐个调用 `FactorEngine.compute` 并立即写入内部编号
Parquet；`BUILD_SIGNALS` 逐因子生成方向统一和可选行业、市值或联合中性化信号，行业与市值
PIT 状态各只读取并对齐一次。流式统计直接复用已过滤的有效因子和最小截面日期执行分位分配，
不得再次扫描完整信号做重复唯一性校验或有效样本过滤。
每个期限只构建一张共享收益边界、同时包含理论与可执行列的紧凑宽标签。统计按
“期限 → 标签 → 信号版本 × 因子”扫描；分位和换手先逐信号准备并落盘，单元内的因子—标签
连接同时供 IC 和分层收益使用，完成即释放。最终内存中只保留固定的 11 张小型结果表。

同一期限的理论与可执行列在一个配对表中共享键连接；若两种标签的收益、有效性和原因逐行
完全相同，可以复用统计结果并只替换标签主键。同一因子的两个信号版本也只有在全部分析必需列
逐行完全相同时才复用分位、换手和统计结果；任一值、有效性或原因不同即分别计算。

多因子相关矩阵按 20 个交易日的确定性批次加载信号，批次间检查取消，再以有效日期数和配对
数恢复双向矩阵。临时文件不进入 Manifest、不可跨任务复用；开始重试前先清理同研究残留，
成功、失败和取消退出时均删除整个可信临时目录。文件名由内部序号生成，不接受配置或用户路径。
`label_quality.count` 与 `eligible_count` 固定为 `Int64`。

## 持久化与可信产物

独立表为 `factor_study`、`factor_study_tag`、`factor_study_metric`、
`factor_study_artifact` 和 `factor_study_decision`。决策主键由研究 ID 加信号版本、收益标签、
因子和期限组成；`CANDIDATE`、`DISCARDED` 都是人工结论，`UNREVIEWED` 表示删除已有结论。
只有已发布 summary 中真实存在的决策行才能写入。

固定 Parquet 产物为：

- `summary`、`coverage`、`label_quality`、`industry_coverage`；
- `ic`、`quantile_returns`、`long_short_returns`、`monotonicity`；
- `turnover`、`cost_scenarios`、`correlation`。

不发布 `stability`。同时发布规范配置、指标和 Manifest。目录固定为
`artifacts/factor-studies/<factor_study_id>/`；发布必须经过同文件系统 staging、原子重命名和
最终目录哈希复核，Manifest 使用 `factor_study_id`。

## 接口与 Dashboard

CLI 使用 `quant factor-studies validate|submit|show|list`。Dashboard API 位于
`/api/v1/factor-studies`，提供 catalog、校验、原子提交、稳定分页、详情、终态删除、决策矩阵、
人工结论和可信产物读取。

前端一级导航“因子研究”包含工作台、表单/YAML 双模式创建页和独立详情页。详情页围绕四维
决策单元展示研究指标与曲线、人工结论和配置/产物；指标区域按主题持续展开 ``summary`` 的全部
非主键字段，未知新增字段也不得静默隐藏，并同时展开 IC、分层、多空、单调性、换手、成本、
覆盖率、标签质量、行业覆盖和相关性曲线。全局选择器同步 URL query。
工作台只汇总研究数量和人工评审进度，不生成跨研究排行榜。

创建页的“近 3 月、近 1 年、近 3 年、近 10 年”快捷项使用当前已验证数据目录的最新交易日
作为结束日期，并按自然月或自然年回推开始日期；月末或闰日取目标月最后一天，不自动改写为
交易日。日期服务不可用时只禁用快捷项，手工输入和 YAML 编辑仍可使用。
