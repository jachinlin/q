# 独立因子研究设计

文档状态：当前有效设计　·　日期：2026-08-24

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

## PIT 输入与分析口径

行情、证券状态、行业和财务数据只经 `CanonicalResearchRepository` 读取。每次研究重新计算
因子，因子值先乘 `FactorSpec.direction`，之后统一以最高分位为多头、最低分位为空头。

- `THEORETICAL_FORWARD_RETURN`：T+1 开盘到 T+h 收盘，两个端点价格有效即可。
- `EXECUTABLE_FORWARD_RETURN`：在理论口径上，T+1 入场还必须已上市、未停牌且不是一字涨停。
- 配置行业块时同时发布方向调整和行业中性化信号；`EXCLUDE` 排除无法分类样本，
  `UNCLASSIFIED` 将其放入固定未分类组。

Pearson IC、Rank IC、毛多空 spread 和成本情景净 spread 使用 Bartlett kernel 的
Newey–West/HAC 推断。多重检验按 Rank IC 与毛多空 spread 两个 family 分别应用
`BONFERRONI` 或 `BH_FDR`。研究不包含 TRAIN/VALIDATION/TEST、test budget、分段稳定性或
自动候选评分。

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
