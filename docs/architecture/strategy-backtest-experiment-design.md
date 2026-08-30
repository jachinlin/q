# 策略研究与回测设计

文档状态：当前有效设计　·　日期：2026-08-30

## 目标

策略研究采用“一次提交、一次执行、一个结果”的 `StrategyStudy`。它面向单用户本地快速
迭代：修改参数或再次执行时复制定义并创建另一项研究，不在一个研究内维护多个 Run，
也不提供 baseline、研究标记、Run 比较或 TRAIN/VALIDATION/TEST 样本治理。

因子研究是独立 `FactorStudy`，保留其统计校正、人工结论和既有产物契约。

## 冻结定义

`StrategyStudyDefinition` 直接包含：

```yaml
name: 沪深 300 ETF 双均线趋势
description: 前复权 20/60 日均线 LONG/FLAT 趋势回测
tags: [etf, trend]
start_date: 2018-01-01
end_date: 2024-12-31
strategy:
  strategy_id: dual_ma_trend
  parameters:
    instrument_id: 510300.SH
    short_window: 20
    long_window: 60
benchmark: 000300.SH
initial_cash_fen: 100000000
execution:
  reference_price: OPEN
  slippage_bps: 5.0
  max_volume_participation: 0.1
  limit_order_policy: REJECT
```

日期必须是明确的 `YYYY-MM-DD`；标签唯一且确定性排序；策略 ID 与参数必须能由策略注册表
真实构造。规范 JSON 的 SHA-256 是 `config_hash`。提交时通过全局研究门捕获
`catalog_hash`，配置和数据身份随后不可变。

## 生命周期与阶段

状态固定为：

```text
QUEUED → RUNNING → SUCCEEDED
                 ↘ FAILED
                 ↘ CANCELLED
```

阶段固定为：

```text
VALIDATE → BACKTEST → ANALYTICS → PUBLISH
```

- `VALIDATE`：重新解析冻结配置，真实构造策略和只读 Canonical 数据适配器。
- `BACKTEST`：执行订单驱动回测；请求与结果只携带 `strategy_study_id`。
- `ANALYTICS`：计算绩效、风险、成交质量和归因，不产生策略 p-value。
- `PUBLISH`：在可信根内暂存、写 Manifest、原子重命名并从最终目录重新验证。

每阶段前后检查 `catalog_hash`，阶段边界检查协作取消。失败和取消调用执行会话的清理逻辑，
删除未登记输出；只有最终 Manifest 完整性验证成功后才能转为 `SUCCEEDED`。

### 进度与任务日志

`progress.completed/progress.total` 始终表示四个公开阶段，成功终态为 `PUBLISH 4/4`。
内部进度只写入 `progress.context`，口径与因子研究一致：

- `substage`：`BUILD_STRATEGY`、`RUN_BACKTEST`、`CALCULATE_ANALYTICS`、
  `PUBLISH_ARTIFACTS` 或 `REGISTER_OUTPUTS`；
- `substage_state`：`STARTED`、`PROGRESS` 或 `COMPLETED`；
- `item_completed`、`item_total`、`trade_date`：只用于回测交易日进度；
- 已脱敏的交易日数、表行数、指标数、产物数、字节数和身份哈希；
- `last_completed_substage` 与 `last_completed_evidence`。

逐日回测只报告首项、末项和跨越 5% 桶的里程碑，长区间最多约 21 条中间进度，
不得为每个交易日写一条任务日志。Task API 返回 `progress.context`；JSON Lines 将其放在
`context.details`；失败状态与失败日志同时保留当前 `substage` 和最后安全进度。

## 持久化与任务

SQLite 主脊为：

```text
strategy_study
strategy_study_tag
strategy_study_metric
strategy_study_artifact
task (task_type/subject_kind = STRATEGY_STUDY)
audit_event (通用 subject_kind/subject_id，无专用 run_id)
```

研究和唯一任务在一个事务中创建。`StrategyStudyRecord` 直接保存定义、配置身份、数据身份、
状态、阶段、任务、错误、指标和产物。策略指标只包含 `name/value/unit`。

迁移直接删除旧 Experiment/Run/baseline/mark 表和旧 `EXPERIMENT_RUN` 任务，不保留兼容层。
旧 `artifacts/experiments` 不迁移、不读取，也不由数据库迁移删除。

## 产物

最终目录固定为：

```text
artifacts/strategy-studies/<strategy_study_id>/
```

Manifest 记录 `strategy_study_id`、配置身份、数据身份、交易规则身份，以及每个产物的相对
路径、SHA-256、字节数、Schema、行数、主键和排序键。发布目录不可覆盖。

## 公开接口

CLI：

```console
qlab strategy-studies validate <yaml>
qlab strategy-studies submit <yaml>
qlab strategy-studies list
qlab strategy-studies show <study-id>
```

HTTP：

```text
GET    /api/v1/strategies
POST   /api/v1/strategy-studies/validate
POST   /api/v1/strategy-studies
GET    /api/v1/strategy-studies
GET    /api/v1/strategy-studies/{study_id}
DELETE /api/v1/strategy-studies/{study_id}
GET    /api/v1/strategy-studies/{study_id}/artifacts/{artifact_type}
```

Dashboard 提供策略研究列表、创建页和单项详情。详情展示配置、状态、指标、图表与产物；
创建页默认使用覆盖三个内置策略全部参数的结构化表单，并保留可双向同步的高级 YAML 模式。
活动研究可取消，终态研究可删除。“复制研究”跳转到
`/strategy-studies/new?from=<study_id>`，用原冻结定义同时预填表单和 YAML；任何修改都必须重新校验。
任务中心将 `STRATEGY_STUDY` 链接到研究详情，不提供原任务重试。

示例：

- [股票多因子](../../configs/strategy_studies/examples/multifactor.yaml)
- [ETF 轮动](../../configs/strategy_studies/examples/etf_rotation.yaml)
- [双均线趋势](../../configs/strategy_studies/examples/dual_ma_trend.yaml)
