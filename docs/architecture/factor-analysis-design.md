# 因子研究设计

文档状态：当前有效设计　·　日期：2026-08-22

因子研究是 `ExperimentKind.FACTOR_STUDY`，与策略回测共享 Experiment、Run、任务、指标、产物、
审计、CLI 和 Dashboard；不再拥有独立 Study/Run 表、Worker handler 或页面。

## 输入与 PIT

`FactorStudyRunConfig` 只包含日期和：

```yaml
factor_study:
  factor_ids: [book_to_price_mrq, momentum_120_20]
  universe: {name: CN_STOCK_STANDARD}
  horizons: [1, 5, 20]
  quantiles: 5
  industry_neutral: false
```

因子计算每个 Run 重新执行，不建跨 Run 缓存。所有行情、状态、行业和财务输入只经
`CanonicalResearchRepository`，并绑定提交时 `catalog_hash`。未来收益固定为 T+1 开盘到 T+h 收盘；
分析因子不得读取收益窗口中的未来信息。

## 阶段与产物

```text
VALIDATE → PREPARE_INPUTS → ANALYZE_FACTORS → PERSIST
```

固定产物为 `summary`、`coverage`、`ic`、`quantile_returns`、`long_short_returns`、`correlation`、
`config`、`metrics` 和 `manifest`。目录为
`artifacts/experiments/<experiment_id>/<run_id>/`。发布经同文件系统 staging、原子重命名及最终目录
复核；Manifest 记录每个文件的哈希、字节数、行数、Schema、主键、排序和输入身份。

## 统计与治理

- 覆盖率分母来自当日 PIT 股票池，缺失和无效样本保留稳定原因。
- IC 同时输出 Pearson 和 Spearman Rank IC；分层和多空收益使用相同有效截面。
- 重叠持有期、最小截面、常数截面、并列秩、空组和零方差均有明确无效语义。
- 每个因子×信号版本×期限的 Rank IC 均值产生原始 p-value；按 Experiment 治理配置应用
  Bonferroni 或 BH-FDR，原始值与校正值同时登记到 `run_metric`。
- Run 与 TEST 区间相交即 `uses_test_region=true`；超过预算只告警和审计，不阻止执行。

Dashboard `/experiments/:experimentId` 对因子 Run 展示摘要、覆盖率、IC、分层、多空、相关性、
显著性和可信 Manifest，并与策略 Run 使用同一比较与 baseline/mark 流程。
