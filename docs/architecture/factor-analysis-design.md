# 因子、特征与信号研究设计

## 1. 定位

因子不再拥有独立的 `FactorStudy`/`FactorRun` 生命周期。因子分析是统一研究平台中
`SIGNAL_STUDY` 的一种分析能力；研究身份、样本切分、候选选择、任务、指标和产物均使用
`research_family`、`research_variant` 与 `research_run`。

因子、策略信号和回测仍是不同产物：因子是可复用输入变换，信号表达决策语义，组合与
成交结果不能反向充当因子证据。

## 2. 数据与 PIT

- 所有研究读取必须经过 `CanonicalResearchRepository`。
- 每次 execution 捕获 `catalog_hash`，每阶段发现漂移立即失败。
- `available_at <= decision_time` 且 `pit_usable=true` 才能参与计算。
- 动态股票池按决策日调用 PIT `UniverseBuilder`，发布每只证券的 `eligible` 和稳定
  `reason_codes`。
- 因子输出固定为 `trade_date`、`instrument_id`、`factor_id`、`value`、
  `available_at`、`is_valid`，无效值必须保留原因语义。
- 每次运行重新计算，不使用跨运行因子缓存。

## 3. 三类信号

| 产物 | 关键字段 | 消费者 |
|---|---|---|
| `CrossSectionalScoreArtifact` | `signal_date`、`instrument_id`、`score`、`rank` | 截面优化器 |
| `DirectionalSignalArtifact` | `direction`、`desired_exposure`、`state_changed` | 暴露映射器 |
| `AllocationSignalArtifact` | `raw_weight`、`strength`、`rank` | 配置投影器 |

三种类型不可通过多义 `value` 混用。信号产物绑定 run、组件源码、`catalog_hash`、
`universe_hash` 和日期区间，并按日期、证券稳定排序。

## 4. 首版研究实现

- 股票多因子：估值与 120 日动量形成截面因子，经有效性过滤和稳定截面标准化后加权。
- 双 MA：调整后收盘价计算短、长均线，输出 LONG/FLAT、目标暴露和状态变化日。
- ETF 轮动：20/60/120 日收益、趋势过滤和波动率惩罚形成 Allocation 排名与权重。
- 风险输入包括波动率、Beta、行业、流动性和收缩协方差。
- 事前成本使用固定费率、ADV、参与率平方根冲击；事后成本来自实际成交与唯一规则文件。

## 5. 研究方式

`SIGNAL_STUDY` 停止于信号分析，验证覆盖率、排序/方向行为、稳定性和 p-value；
`PORTFOLIO_STUDY` 进一步发布理论目标组合、风险与事前成本，但不宣称可成交收益；
`BACKTEST_EXPERIMENT` 执行完整订单、成交、实际成本、账户、净值和归因。

所有候选运行 TRAIN 与 VALIDATION。TRAIN 只做诊断，选型只读取 VALIDATION；锁定的
`selection.json` 发布后才创建唯一 TEST 运行，TEST 指标永不参与候选排序。

## 6. 产物与验收

研究产物位于 `artifacts/research/<family_id>/<execution_id>/`，包含股票池、信号、目标
组合、成交、分析和 Manifest。Manifest 必须记录相对路径、SHA-256、字节数、Schema、
行数、主键、排序和完整输入身份，并从最终目录重新验证。

数值测试必须覆盖并列与常数截面、缺失原因、MA 交叉日、ETF 排名、PIT 截止时点、
多重检验、TEST 隔离和分区无关的确定性排序。
