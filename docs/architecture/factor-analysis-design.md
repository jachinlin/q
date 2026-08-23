# 因子研究设计

文档状态：当前有效设计　·　日期：2026-08-23

因子研究是 `ExperimentKind.FACTOR_STUDY`，与策略回测共享 Experiment、Run、任务、通用
`run_metric`、可信产物、CLI 和 Dashboard。它用于候选因子筛选，不模拟资金、持仓和撮合；净值、
Sharpe、最大回撤和真实交易成本只属于策略回测。

## 冻结输入与身份

`FactorStudyRunConfig` 使用最终配置契约：

```yaml
factor_study:
  factor_ids: [book_to_price_mrq, momentum_120_20]
  universe: {name: CN_STOCK_STANDARD}
  horizons: [1, 5, 20]
  quantiles: 5
  industry:
    taxonomy: 证监会行业分类
    unclassified_policy: EXCLUDE  # 或 UNCLASSIFIED
  cost_bps_scenarios: [5, 10, 20]
```

`industry: null` 只发布 `DIRECTION_ADJUSTED`；配置行业块时同时发布
`DIRECTION_ADJUSTED` 和 `INDUSTRY_NEUTRALIZED`。不存在 `industry_neutral` 旧入口。
成本情景必须是严格升序、唯一的非负整数。理论和可执行收益标签固定同时发布，不能关闭。

每次 Run 重新计算因子。因子值先乘 `FactorSpec.direction`，之后统一以最高分位为多头、最低分位
为空头。Manifest 的 `analysis_identity` 绑定 `catalog_hash`、逐日 PIT 股票池哈希、因子执行描述及
代码哈希、A 股规则文件哈希、双标签定义、行业策略，以及 HAC、换手和成本公式参数。

## PIT 输入与双收益标签

行情、证券状态、行业和财务数据只经 `CanonicalResearchRepository` 读取。收益数值使用前复权价格：

- `THEORETICAL_FORWARD_RETURN`：T+1 开盘到 T+h 收盘，两个端点价格有效即可。
- `EXECUTABLE_FORWARD_RETURN`：在理论口径上，T+1 入场还必须已上市、未停牌，且不是依据未复权
  行情和 `configs/rules/a_share.yaml` 判定的一字涨停；退出价格必须有效。

每个股票池样本都保留 `is_valid` 和固定优先级 `invalid_reason`，不得以空收益静默删除。理论标签
不受停牌和涨停状态污染。一字涨停只约束多头入场，不模拟排队成交、冲击或退出跌停。

行业中性化使用信号日 PIT `industry_code` 等权组内去均值。`EXCLUDE` 排除 tombstone 和缺失状态；
`UNCLASSIFIED` 将两者放入固定未分类组。单成员行业仍以明确原因失效。逐日覆盖区分已分类、
tombstone、缺失状态和可用数量。

## 固定产物

因子 Run 固定发布以下 Parquet；所有相关表使用确定性排序和声明主键：

- `summary`：版本 × 标签 × 因子 × 期限的候选决策摘要。
- `coverage`、`label_quality`、`industry_coverage`：因子、标签和行业输入质量；未启用行业时
  `industry_coverage` 是固定 Schema 空表。
- `ic`：日度 Pearson/Rank IC，以及 `eligible_count`、`label_valid_count`、`sample_count`、
  `pair_coverage`。
- `quantile_returns`、`long_short_returns`：独立分位收益、实际因子上下边界和毛多空 spread。
- `monotonicity`：分位序号与收益的 Spearman 相关、OLS 斜率、相邻倒序数和终端 spread。
- `turnover`：日度秩自相关、高低分位等权成员换手和总换手。
- `stability`：TRAIN/VALIDATION/TEST、自然年和自然月的 Rank IC 稳定性。
- `cost_scenarios`：毛 spread、总换手、成本拖累、净 spread、净 spread HAC 和盈亏平衡成本。
- `correlation`：因子两两 Pearson 与 Spearman 相关。

同时发布 `config.json`、`metrics.json` 和 `manifest.json`。目录为
`artifacts/experiments/<experiment_id>/<run_id>/`，通过同文件系统 staging、原子重命名和最终目录
复核后登记；旧 Run 产物不迁移。

## 统计、换手与治理

Pearson IC、Rank IC、毛多空 spread 和每个成本情景净 spread 都使用 Bartlett kernel 的
Newey–West/HAC 推断。滞后阶数固定为 `min(horizon - 1, valid_count - 1)`，输出均值、HAC 标准误、
t-stat、双侧正态近似 p-value 和 95% CI。样本不足或长期方差非正时只输出明确无效原因。

单调性至少需要三个有效分位组。换手在相邻信号日计算，每条腿为
`0.5 × Σ|w_t-w_(t-1)|`，总换手为两腿之和；首日记为无效。成本代理为：

```text
net_spread = gross_spread - total_turnover × bps / 10000
```

盈亏平衡成本只使用毛 spread 与换手都有效的对齐日期；毛收益非正或总换手为零时输出原因码。
稳定性只使用各冻结区间与 Run 日期交集内的样本，年月按信号日切分。

多重检验分为 Rank IC 与毛多空 spread 两个独立 family。每个 family 覆盖全部版本、标签、因子和
期限，分别应用 Experiment 治理配置的 Bonferroni 或 BH-FDR。Run 级指标只保留平均因子覆盖率、
平均配对覆盖率、有效 Rank IC 假设数和校正后显著 Rank IC 数，不发布跨因子收益或 IC 均值。

## Dashboard

Dashboard 以“版本 × 标签 × 因子 × 期限”为决策单元，矩阵展示 Rank IC、HAC t-stat、校正
p-value、单调性、毛 spread、盈亏平衡成本和换手。全部下钻请求传递 `label_kind`，并提供标签失败
原因堆叠图、独立分位序列、样本区间与年月稳定性、秩自相关和换手、成本 bps—净 spread 图。
Run 比较继续使用维度化的通用指标名称与统一 Experiment/Run DTO。
