# 因子研究与分析设计：从信号到可信结论

文档状态：当前有效设计　·　日期：2026-08-30

本文既是本项目独立因子研究的权威设计，也是写给第一次接触量化研究的后端工程师的入门说明。
读者不需要预先理解金融统计，但应熟悉 Python、命令行、数据库和 API 等一般开发概念。

读完本文后，你应该能回答以下问题：

- 因子是什么，它与策略回测有什么区别？
- 一次研究究竟比较哪些股票、日期和未来收益？
- PIT、方向调整、行业中性化和市值中性化分别解决什么问题？
- IC、分层收益、多空收益、换手和成本应该怎样阅读？
- 系统如何让配置、数据身份和最终产物可以相互追溯？

第 1～8 节建立量化研究主线；第 9～12 节说明接口、实现不变量、常见误读和术语。

## 1. 因子研究解决什么问题

### 1.1 什么是因子

**因子（factor）**是一个可以按“日期 × 股票”计算的数值特征。例如：

- 账面市值比：尝试描述股票相对账面价值是否便宜；
- 过去 120 日到 20 日的动量：尝试描述一段时间内的价格趋势；
- ROE：尝试描述公司使用净资产创造利润的能力。

因子值本身不是收益，也不是买卖指令，而是一个可检验的排序信号。因子研究想回答：

> 同一天因子分数更高的股票，之后是否倾向于获得更高收益？这种关系是否覆盖充分、相对稳定、
> 具有一定可执行性，并且足以抵扣换手成本？

### 1.2 六个基础概念

| 概念 | 含义 | 本项目中的例子 |
|---|---|---|
| 截面 | 同一时点上多只股票组成的一组观测 | 2022-06-30 日终可研究的全部 A 股 |
| 信号日 | 计算股票池和因子、形成排序的日期 | `signal_date = 2022-06-30` |
| 股票池 | 某个信号日允许进入比较的股票集合 | `CN_STOCK_STANDARD` |
| 未来收益 | 信号形成后某个区间内的收益标签 | T+1 开盘到 T+5 收盘 |
| 分位组 | 按因子排序后切成数量尽量相近的组 | 五分位时为 Q1～Q5 |
| PIT | Point-in-Time，只使用历史当时已经知道的信息 | 6 月 30 日看不到 7 月才公告的财报 |

时间序列比较同一只股票在不同日期的变化；截面研究比较同一天许多股票之间的差异。因子研究先逐日
得到截面结论，再把日度结论汇总。

### 1.3 因子研究不是策略回测

| 因子研究 `FactorStudy` | 策略回测 `Experiment → Run` |
|---|---|
| 检查因子与未来收益的统计关系 | 模拟策略如何产生订单、成交和持仓 |
| 输出 IC、分层、多空、换手、成本代理和相关性 | 输出订单、成交、净值、绩效和归因 |
| 不管理现金、仓位、成交量限制或账户账本 | 包含组合构建、交易规则、撮合和账户状态 |
| 参数变化必须提交一项新研究 | 一个策略实验可以产生多个不可变 Run |
| 由研究者人工标记候选或淘汰 | 可以比较 Run，但仍需人工作结论 |

因子研究中的“多头”“空头”和“成本”都是诊断口径，不表示系统真的建立账户并成交。因子研究结果是
后续策略开发的输入证据，不是可以直接上线的策略表现。

## 2. 用价值与动量提出一项研究

贯穿本文的例子来自[示例配置](../../configs/factor_studies/examples/factor_study.yaml)：

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

它要回答的不是“价值动量策略能赚多少钱”，而是：

1. 账面市值比和中期动量能否预测之后 1、5、20 个交易日的收益？
2. 控制行业和公司规模后，这种关系是否仍然存在？
3. 排除明显无法买入的样本后，理论关系还剩多少？
4. 高分组减低分组的收益能否覆盖 5、10、20 bps 的换手成本代理？
5. 两个因子的排序信息是否高度重复？

### 2.1 配置字段

| 字段 | 本例取值 | 含义与约束 |
|---|---|---|
| `name` | `价值动量因子研究` | 人类可读名称，1～128 个字符 |
| `description` | `候选因子诊断` | 可选说明，最长 4,000 个字符 |
| `tags` | `[factor]` | 可选标签；必须唯一并按字典序排列 |
| `start_date` / `end_date` | 2018～2022 | 明确的 `YYYY-MM-DD`，开始日不得晚于结束日 |
| `correction` | `BH_FDR` | `BH_FDR` 或 `BONFERRONI` 多重检验校正 |
| `factor_ids` | 两个因子 ID | 至少一个，非空且不得重复，必须能在因子目录中解析 |
| `universe.name` | `CN_STOCK_STANDARD` | 当前唯一允许的 PIT 股票池 |
| `horizons` | `[1, 5, 20]` | 正整数、唯一、严格升序，单位为交易日 |
| `quantiles` | `5` | 分位组数，至少为 2 |
| `industry` | SW2021 + EXCLUDE | 可选行业中性化及未分类处理方式 |
| `market_cap` | 对数总市值 | 可选市值中性化，当前只支持 `LOG_TOTAL_MARKET_VALUE` |
| `cost_bps_scenarios` | `[5, 10, 20]` | 非负整数、唯一、严格升序；1 bps = 0.01% |

这是严格、扁平的最终契约，不接受未知字段，也没有 `kind`、`initial_run`、`factor_study`、
`sample_windows` 或 `governance` 包装层。日期必须明确，不接受“近一年”之类 selector，也没有格式
版本字段。

解析器将配置转换为规范 JSON 字节并计算 SHA-256 `config_hash`。相同规范字节得到相同身份；字段值
或有序列表的顺序变化可能得到不同身份。参数变化不能覆盖已有成功结果。

### 2.2 校验与提交

```console
uv run quant factor-studies validate configs/factor_studies/examples/factor_study.yaml
uv run quant factor-studies submit configs/factor_studies/examples/factor_study.yaml
uv run quant worker once
uv run quant factor-studies show <factor-study-id>
uv run quant factor-studies list
```

`validate` 解析配置并确认因子 ID 存在，不创建研究。`submit` 还要求整个 Canonical 数据目录已经通过
全局校验，随后原子创建独立 `FactorStudy` 和一个 `FACTOR_STUDY` 后台任务。Worker 领取任务后执行
研究，CLI 不在提交进程中同步做大规模计算。

## 3. 从历史事实到研究样本

```text
已通过全局校验的 Canonical 数据
              │
              ▼
按信号日重建 PIT 股票池 ──────→ eligible
              │
              ├──→ 逐因子重新计算 ─→ 方向调整 / 可选中性化 ─→ signal
              │
              └──→ T+1 入场与 T+h 退出 ────────────────→ forward return
                                                        │
signal × forward return 按 signal_date + instrument_id 对齐
                                                        │
                                                        ▼
                                     IC、分层、多空、换手、成本和相关性
```

行情、证券状态、行业和财务数据只能经 `CanonicalResearchRepository` 读取，不得直接扫描 Raw 或
Canonical 路径。否则可能读到未校验文件、更新中间态或不满足 PIT 的历史数据。完整数据约束见
[数据层设计](data-layer-design.md)。

### 3.1 每个信号日有哪些股票

`CN_STOCK_STANDARD` 复用 `UniverseRules` 的统一口径：

- 至少已经经历 120 个真实上市交易日；
- 只允许主板、创业板和科创板；
- 排除尚未上市、已经退市、元数据缺失、风险警示和停牌证券；
- 按固定优先级保留排除原因，以便审计。

“120 个交易日”从真实 `list_date` 开始沿完整 Canonical 交易日历计数，不能从研究 `start_date`
重新编号，否则改变研究起点就会改变股票上市年龄。

停牌和风险警示可以按区间批量读取，但每行仍按信号日对应的上海日终 `available_at` 截止判断。
区间末日才公开的状态不能倒灌到历史日期。

### 3.2 重新计算因子并统一方向

因子每次研究都重新计算，不使用跨研究持久化缓存。固定输出列为：

```text
trade_date, instrument_id, factor_id, value, available_at, is_valid
```

系统先把因子值乘以 `FactorSpec.direction`，使后续分析统一遵循：调整后分数越高，越被当作多头端；
分数越低，越被当作空头端。方向调整不会自动让因子有效，也不会把缺失或无效值补成零。

内置股票因子目录固定为以下 18 项，其中 `avg_amount_20d` 只用于流动性辅助，其余 17 项可进入
Alpha 研究。窗口、源字段、口径、方向和有效值约束均写入对应 `FactorSpec.parameters`：

| `factor_id` | 公式或 Canonical 源值 | 方向 | 关键约束 |
|---|---|---:|---|
| `earnings_yield_ttm` | `1 / pe_ttm` | +1 | 分母有限且非零 |
| `book_to_price_mrq` | `1 / pb` | +1 | 分母有限且非零 |
| `sales_yield` | `1 / ps_ttm` | +1 | TTM；分母有限且非零 |
| `dividend_yield` | `dividend_yield_ttm` | +1 | TTM；非负且有限 |
| `log_total_market_cap` | `ln(total_market_value)` | -1 | 输入为正且有限 |
| `roe` | 最新可见 `roe` | +1 | 最新报告期及修订；190 日时效 |
| `revenue_growth` | 最新可见 `tr_yoy` | +1 | YoY；允许合法负值；190 日时效 |
| `profit_growth` | 最新可见 `netprofit_yoy` | +1 | YoY；允许合法负值；190 日时效 |
| `roa` | 最新可见 `roa` | +1 | 允许合法负值；190 日时效 |
| `gross_margin` | 最新可见 `grossprofit_margin` | +1 | 允许合法负值；190 日时效 |
| `cash_quality` | 最新可见 `ocf_to_opincome` | +1 | 允许合法负值；190 日时效 |
| `leverage` | 最新可见 `debt_to_assets` | -1 | 非负且有限；190 日时效 |
| `momentum_120_20` | T-120 至 T-20 的前复权累计收益 | +1 | 完整交易会话窗口 |
| `volatility_60d` | 60 日对数收益年化样本波动率 | -1 | 完整交易会话窗口 |
| `downside_volatility_60d` | 60 日负对数收益年化均方根 | -1 | 完整交易会话窗口 |
| `max_drawdown_120d` | 120 日前复权价格最大回撤 | -1 | 完整交易会话窗口 |
| `turnover_20d` | 20 个自由流通换手率观测的算术均值 | -1 | 全部非负、有限且在信号日可见 |
| `avg_amount_20d` | 20 日成交额算术均值 | +1 | 流动性辅助，不允许进入 Alpha 组合 |

财务类因子在一个 `FactorContext` 内共享一次交易日历和一次完整修订历史读取。信号日选择最新可见
报告期的最新修订；若该最新记录的目标字段无效，不回退到旧报告。

### 3.3 构造两类未来收益

对信号日 T 和持有期 h，两类标签都使用前复权价格：

```text
future_return = adjusted_close(T+h) / adjusted_open(T+1) - 1
```

| 标签 | 条件 | 回答的问题 |
|---|---|---|
| `THEORETICAL_FORWARD_RETURN` | T+1 开盘和 T+h 收盘价格有效 | 只看价格端点时，因子与未来收益有什么关系？ |
| `EXECUTABLE_FORWARD_RETURN` | 理论标签有效，且 T+1 已上市、未停牌、不是一字涨停 | 排除明显买不到的入场后，关系还剩多少？ |

`horizon: 1` 表示 T+1 开盘到 T+1 收盘，`horizon: 5` 表示 T+1 开盘到 T+5 收盘。区间尾部
不足、价格缺失、退市后无退出价、收益非有限等情况不会被静默删除，而是保留稳定无效原因。

可执行标签仍不是完整撮合：它不模拟资金、订单量、冲击成本、排队成交、持仓约束或卖出限制。

## 4. 多个信号版本与中性化

因子可能间接表达行业或公司规模。例如价值因子高分股票可能集中在银行业；银行整体上涨时，研究者
容易把行业行情误认为因子能力。**中性化**移除这些已知截面暴露，再检查剩余部分。

系统始终发布 `DIRECTION_ADJUSTED`，并按配置增加一个版本：

| 配置 | 额外信号版本 | 计算直觉 |
|---|---|---|
| 无行业、无市值 | 无 | 只做方向统一 |
| 只配置行业 | `INDUSTRY_NEUTRALIZED` | 行业内等权去均值 |
| 只配置市值 | `MARKET_CAP_NEUTRALIZED` | 对对数总市值做带截距等权截面回归，取残差 |
| 同时配置两者 | `INDUSTRY_MARKET_CAP_NEUTRALIZED` | 行业内中心化因子和市值，再回归市值暴露 |

两者同时配置时只增加联合版本，不同时发布两个单项版本。因此贯穿示例得到方向调整版和行业＋市值
联合中性化版。

行业分类必须是信号日 PIT 可见的 SW2021 状态：`EXCLUDE` 排除无法分类股票，`UNCLASSIFIED`
将其放入固定未分类组。市值暴露使用 PIT 可见、有限且为正的 `total_market_value` 自然对数。

行业缺失、单成员行业、市值缺失或非正、截面不足、市值暴露零方差等情况产生稳定无效原因，不用
原因子值回填。中性化后的值表示相对同行业、相近规模股票的剩余高低，没有直接经济单位。

## 5. 用小截面理解 IC、分位和多空

假设一个信号日只有五只股票，已经完成方向调整：

| 股票 | 因子值 | 因子排名 | T+1 收益 | 收益排名 | 五分位 |
|---|---:|---:|---:|---:|---:|
| A | 10 | 1 | -2% | 1 | Q1 |
| B | 20 | 2 | -1% | 2 | Q2 |
| C | 30 | 3 | 0% | 3 | Q3 |
| D | 40 | 4 | 1% | 4 | Q4 |
| E | 50 | 5 | 2% | 5 | Q5 |

这个示意截面中，Pearson IC 与 Rank IC 都是 1，各分位收益单调上升，毛多空 spread 为
`Q5 - Q1 = 4%`。但这只是教学示例：生产环境默认每个日度截面至少需要 30 个有效样本，五只股票
会以 `INSUFFICIENT_CROSS_SECTION` 失效。

### 5.1 Pearson IC 与 Rank IC

```text
Pearson IC = corr(factor_value, future_return)
Rank IC    = corr(rank(factor_value), rank(future_return))
```

- 正值：高分股票之后倾向于收益更高；
- 负值：关系与统一方向相反；
- 接近零：截面上没有明显线性或排序关系。

Pearson IC 更关注数值线性关系，Rank IC 只关注顺序，通常更贴近排名选股。单日 IC 很噪声，应结合
跨日期均值、分布、正值比例、置信区间和有效覆盖阅读。

### 5.2 分层收益与单调性

系统在每个有效截面稳定排序并分配 Q1～Qn。`quantile_returns` 保存各分位收益；
`monotonicity` 用分位编号与收益的秩相关、趋势斜率、相邻反转次数和首尾差描述形状。只看 Q1 与
Qn 可能漏掉中间组混乱，所以多空 spread 与单调性必须一起看。

### 5.3 毛多空、换手和成本

```text
gross_spread = highest_quantile_return - lowest_quantile_return
leg_turnover = 0.5 × Σ |weight_t - weight_t-1|
total_turnover = low_quantile_turnover + high_quantile_turnover
net_spread = gross_spread - total_turnover × cost_bps / 10,000
```

这些都是等权诊断口径，不是账户收益。`break_even_cost_bps` 用整个对齐样本的累计毛 spread 与累计
换手估算盈亏平衡成本；毛 spread 非正或总换手为零时无定义。成本情景不包含冲击函数、成交量限制
或订单簿，不能替代回测成本模型。

## 6. 怎样阅读研究结果

可靠顺序是“样本是否可信 → 关系是否存在 → 形状是否合理 → 是否可能执行 → 是否提供新增信息”。

### 6.1 覆盖率和标签质量

`coverage` 比较股票池 `eligible_count` 与因子 `valid_count`。覆盖低可能来自财务数据缺失、上市
历史不足、中性化失败或因子只覆盖部分股票；低覆盖上的漂亮 IC 可能只描述有偏子集。

`label_quality` 按原因统计未来收益的有效和无效数量。理论与可执行覆盖差可以揭示停牌、一字涨停等
入场障碍。配置行业时还要看 `industry_coverage`；未配置行业时该表保留固定 Schema，但可以为空。

### 6.2 IC、分层和多空应讲同一个故事

较完整的正向证据通常包括：

- Rank IC 均值方向正确，并非由极少数日期贡献；
- Q1～Qn 大致单调，不是只有一个异常组；
- 最高分位减最低分位的毛 spread 方向一致；
- 理论与可执行标签没有无法解释的反转；
- 原始版与中性化版的差异能由行业或规模暴露解释。

这是阅读框架，不是自动通过规则；系统不会按阈值自动选因子。

### 6.3 HAC 与多重检验

多日收益会重叠，市场状态也会让相邻日期的 IC 或 spread 相关。Pearson IC、Rank IC、毛多空 spread
和成本情景净 spread 的均值都使用 Bartlett kernel 的 Newey–West/HAC 标准误、双侧 p 值和 95%
置信区间。

HAC 描述均值不确定性，不提高均值，也不证明因果。实现保留完整信号交易会话轴，无效日以空值占位，
不会把原本隔日的有效样本压缩成相邻样本。滞后阶数为：

```text
min(horizon - 1, signal_date_count - 1)
```

同时检验很多组合容易偶然得到小 p 值。本项目分别对 Rank IC 和毛多空 spread 两个假设族应用
`BONFERRONI` 或 `BH_FDR`，登记校正后 p 值。显著性仍不等于经济收益足够大或能够交易。

### 6.4 换手、成本和相关性

- 排名自相关高通常意味着排序稳定、换手较低；
- 毛 spread 为正但净 spread 为负，说明关系可能太弱或换手太高；
- 盈亏平衡成本很低，说明结果对成本假设敏感；
- 因子 Rank 相关接近 1 或 -1，说明排序信息可能重复。

`correlation` 输出完整“因子 × 因子”双向矩阵。没有共同有效样本的组合也保留，配对数为零、相关值
为空且状态无效；缺失或空值不能误解为“相关性为零”。

## 7. 生命周期与数据身份

因子研究是独立 `FactorStudy`，不属于 Experiment，也不存在 Run、Baseline、派生 Run 或 Run 比较。
提交时冻结规范配置及 `config_hash`、当前已验证目录的 `catalog_hash`、研究 ID 和任务 ID。

Worker 执行固定四阶段：

```text
VALIDATE → PREPARE_INPUTS → ANALYZE_FACTORS → PUBLISH
```

| 阶段 | 职责 | 成功证据 |
|---|---|---|
| `VALIDATE` | 再检查配置、因子目录和数据身份 | 配置与目录仍合法 |
| `PREPARE_INPUTS` | 冻结交易日、PIT 股票池和输入边界 | 股票池哈希、日期和规模证据 |
| `ANALYZE_FACTORS` | 计算因子、信号版本、双标签和统计 | 11 张固定结果表与分析身份 |
| `PUBLISH` | staging、复核、原子发布和登记 | 最终目录、Manifest 哈希与登记记录 |

每个阶段前后检查 `catalog_hash`。运行中数据目录变化时立即失败，避免混用两批数据。这是运行内一致
性门，不是历史版本回放：项目没有可按旧 `catalog_hash` 读取的 Snapshot。

状态为 `QUEUED → RUNNING → SUCCEEDED`，运行中也可进入 `FAILED` 或 `CANCELLED`。失败、取消和失联
恢复复用同一个 `FACTOR_STUDY` Task 和冻结配置，并创建新 attempt。重试前清理残留临时目录，不能
借重试改参数。

成功研究不可重跑；参数变化必须提交新研究。终态研究可通过 Dashboard API 删除，运行中不能删除。

## 8. 可信产物与人工结论

### 8.1 十一张 Parquet 表

| 产物 | 粒度或主键要点 | 用途 |
|---|---|---|
| `summary` | 信号版本 × 标签 × 因子 × 期限 | 汇总 IC/HAC、多空、单调性、换手和成本 |
| `coverage` | 信号版本 × 因子 × 信号日 | 因子有效数量和覆盖率 |
| `label_quality` | 标签 × 期限 × 信号日 × 原因 | 收益标签质量 |
| `industry_coverage` | 信号日 × 分类法 × 未分类策略 | 行业状态覆盖 |
| `ic` | 四维决策键 × 信号日 | 日度 Pearson/Rank IC 及诊断 |
| `quantile_returns` | 四维决策键 × 信号日 × 分位 | 各分位收益 |
| `long_short_returns` | 四维决策键 × 信号日 | 毛多空 spread |
| `monotonicity` | 四维决策键 × 信号日 | 分层形状 |
| `turnover` | 信号版本 × 因子 × 信号日 | 排名稳定性与两端换手 |
| `cost_scenarios` | 四维决策键 × 成本 bps | 净 spread 与盈亏平衡成本 |
| `correlation` | 信号版本 × 因子 X × 因子 Y | 因子相关性与配对证据 |

四维决策键固定为：

```text
(signal_variant, label_kind, factor_ref, horizon)
```

`turnover` 和 `correlation` 不依赖标签或期限，因此不重复存储。

### 8.2 配置、指标和 Manifest

最终目录还包含 `config.json`、`metrics.json` 和 `manifest.json`，路径固定为：

```text
artifacts/factor-studies/<factor_study_id>/
```

Manifest 使用 `factor_study_id`，记录相对路径、SHA-256 和字节数；Parquet 还记录 Schema、行数、
主键与排序键。`analysis_identity` 绑定股票池哈希、因子执行描述及哈希、交易规则哈希、标签定义、
中性化设置、HAC、换手和成本口径。临时文件不进入 Manifest。

### 8.3 人工结论

研究者可对已发布 summary 中真实存在的四维行写入：

| 标记 | 含义 |
|---|---|
| `UNREVIEWED` | 尚无结论；写入它等同于删除已有结论 |
| `CANDIDATE` | 值得进入策略设计和回测 |
| `DISCARDED` | 当前证据下不再推进 |

结论保存备注、操作者和更新时间。持久化主键为 `factor_study_id` 加四维键，因此不同研究不会互相
覆盖。系统不自动评分，也不生成跨研究排行榜。

## 9. CLI、HTTP 与 Dashboard

### 9.1 CLI

```text
quant factor-studies validate <yaml>
quant factor-studies submit <yaml>
quant factor-studies show <factor-study-id>
quant factor-studies list
```

取消与失败重试走通用任务命令。成功 stdout 只输出 JSON，受控错误写 stderr。

### 9.2 HTTP API

| 方法与路径 | 职责 |
|---|---|
| `GET /api/v1/factor-studies/catalog` | 因子目录 |
| `POST /api/v1/factor-studies/validate` | 校验 YAML |
| `POST /api/v1/factor-studies` | 原子提交并返回 202 |
| `GET /api/v1/factor-studies` | 稳定分页与筛选 |
| `GET /api/v1/factor-studies/{id}` | 详情 |
| `DELETE /api/v1/factor-studies/{id}` | 删除终态研究 |
| `GET /api/v1/factor-studies/{id}/matrix` | 四维决策矩阵 |
| `PUT /api/v1/factor-studies/{id}/decisions` | 人工结论 |
| `GET /api/v1/factor-studies/{id}/artifacts/{type}` | 可信产物读取 |

产物接口只接受白名单类型和声明过滤字段，不接受任意文件路径；读取前重新验证目录、Manifest 和文件
身份。

### 9.3 Dashboard

一级导航“因子研究”包含工作台、表单/YAML 双模式创建页和独立详情页。工作台只汇总研究数量、状态
和人工评审进度；详情页围绕四维决策单元展示指标、曲线、结论、配置和产物，选择器同步 URL query。
`summary` 的未知新增非主键字段也必须显示。运行中心通过 `/api/v1/tasks/{task_id}` 展示阶段与诊断，
终态后停止轮询。

“近 3 月、近 1 年、近 3 年、近 10 年”快捷项以已验证目录的最新交易日为结束日期，按自然月或
自然年回推；月末和闰日取目标月最后一天，不自动改成交易日。日期服务不可用时只禁用快捷项，手工
输入和 YAML 仍可使用。

## 10. 工程实现与不变量

### 10.1 公开阶段与内部进度

`progress.completed/progress.total` 始终表示四个公开阶段，成功终态为 `PUBLISH 4/4`。分析内部进度
只写入 `progress.context`：

- `substage`：`BUILD_UNIVERSE`、`COMPUTE_FACTORS`、`BUILD_SIGNALS`、
  `LOAD_LABEL_INPUTS`、`BUILD_FORWARD_RETURNS`、`ANALYZE_STATISTICS`、
  `BUILD_METRICS`、`PUBLISH_ARTIFACTS` 或 `REGISTER_OUTPUTS`；
- `substage_state`：`STARTED`、`PROGRESS` 或 `COMPLETED`；
- `item_completed`、`item_total`、`signal_date`：只用于股票池准备；
- 已脱敏的行数、证券数、因子数、产物数、字节数和身份哈希；
- `last_completed_substage` 与 `last_completed_evidence`。

股票池只报告首项、末项和跨越 5% 桶的里程碑，长区间最多约 21 条中间进度。Task API 返回
`progress.context`；JSON Lines 将其放在 `context.details`；失败事件在 `last_progress` 保存最后安全
进度。

### 10.2 股票池、涨停与 PIT

批量股票池由 `universe` 能力层实现，与逐日 `UniverseBuilder` 保持原因码和优先级一致。交易日与股票
使用 Polars 分批向量化连接，结果按 `signal_date, instrument_id` 稳定排序。

股票池 SHA-256 必须与包含原因码的完整 canonical JSON 成员列表逐字节一致。哈希流式吸收完整批次
后，常驻内存只保留 `signal_date, instrument_id, eligible`。

一字涨停使用未复权 `preclose`、真实全局上市交易日序号和 `configs/rules/a_share.yaml` 历史规则。
比例使用整数分数，价格使用最小十进制报价单位并执行 `ROUND_HALF_UP`；批量结果必须与
`AShareRuleBook.price_limits` 的 Decimal 口径一致，不能用 Float64 近似半分边界。

### 10.3 统计语义优先

中性化使用 Polars 窗口表达式向量化完成有效性筛选、行业中心化、缩放回归和原因赋值，不退化为逐行
Python 列表。计算按证券稳定排序并使用确定性求和。

每个“信号版本 × 因子”只做一次有效样本过滤、最小截面屏蔽和分位分配，同一结果复用于换手和所有
标签。日度 IC 先按键对齐，再按日期调用相同 NumPy Pearson 与平均秩算法；分层和标签质量使用
Polars 聚合。

向量化不得改变输出 Schema、主键排序、空分位、无效原因优先级、身份哈希、统计公式或完整信号
会话轴；浮点归约只允许机器精度范围的末位差异。

### 10.4 临时 Parquet 与内存边界

Worker 使用 `tmp/factor-studies/<factor_study_id>` 作为单任务独占临时边界：

1. 按确定性因子顺序计算并立即写内部编号 Parquet；
2. 逐因子生成方向调整和可选中性化版本，行业、市值 PIT 状态各读取并对齐一次；
3. 每个期限只构建一张同时含理论与可执行列的紧凑宽标签；
4. 按“期限 → 标签 → 信号版本 × 因子”扫描，完成连接即释放；
5. 最终内存只保留 11 张小型结果表。

理论与可执行标签只有收益、有效性和原因逐行完全一致时才能复用统计；两个信号版本也只有全部分析
列完全相同时才能复用。

相关矩阵按 20 个交易日确定性批次加载，批次间检查取消，再恢复全部输入因子的双向矩阵。临时文件
不进入 Manifest、不可跨任务复用；成功、失败和取消都删除临时目录。文件名只用内部序号。
`label_quality.count` 与 `eligible_count` 固定为 `Int64`。

### 10.5 持久化与原子发布

独立 SQLite 表为：

```text
factor_study
factor_study_tag
factor_study_metric
factor_study_artifact
factor_study_decision
```

发布固定为：

```text
同文件系统 staging
  → 写稳定排序的 11 张 Parquet、config.json、metrics.json
  → 校验主键并生成 manifest.json
  → 原子重命名到最终目录
  → 从最终目录复核哈希、字节数、行数、Schema 和排序
  → SQLite 登记指标与产物
  → FactorStudy 标记为 SUCCEEDED
```

最终目录已存在时禁止覆盖。发布、登记、取消或复核失败都不能留下登记成功的半成品；所有路径必须先
解析并验证位于可信根内。

## 11. 常见误读与设计边界

### 11.1 显著不等于可交易

p 值与置信区间不证明因果，也不保证收益量级、结构稳定性或成交可实现。必须同时检查覆盖、spread、
换手、成本和策略回测。

### 11.2 理论收益不等于可执行收益

理论标签只要求价格端点；可执行标签额外排除部分明显买不到的入场，但仍不是撮合。两者差异大说明
可交易性可能是主要风险。

### 11.3 中性化不等于风险消失

当前只控制 SW2021 行业和对数总市值。风格、流动性、波动率、极端行情和模型误差仍可能影响结果。

### 11.4 因子研究不等于回测

因子研究没有仓位、资金、撮合和账户，不能把多空 spread 当作策略净值。候选因子必须进入策略定义、
组合构建和回测后才能评价完整交易结果。

### 11.5 当前明确不包含

- TRAIN / VALIDATION / TEST 样本分段；
- test budget 或跨研究试验次数治理；
- `stability` 产物或分段稳定性评分；
- 自动候选评分和跨研究排行榜；
- 历史数据 Snapshot 回放；
- 真实组合、订单、成交和账户模拟。

## 12. 术语速查

| 术语 | 简明解释 |
|---|---|
| Factor | 按日期和股票计算、用于检验排序关系的数值特征 |
| FactorStudy | 配置、数据身份和成功产物不可变的独立研究 |
| PIT | 站在历史时点，只使用当时已经公开的信息 |
| Cross-section | 同一天许多股票组成的比较截面 |
| Signal date | 形成股票池和因子排序的日期 |
| Horizon | 从信号日向后观察收益的交易日跨度 |
| Label | 用于检验因子的未来收益结果 |
| Quantile | 按因子排序后划分的分位组 |
| IC / Rank IC | 因子值或排名与未来收益的日度截面相关性 |
| Long-short spread | 最高分位收益减最低分位收益 |
| Neutralization | 从因子值中移除行业或市值暴露 |
| Turnover | 相邻信号日分组成员或权重变化程度 |
| bps | 基点；1 bps = 0.01% = 0.0001 |
| HAC | 对异方差和时间自相关稳健的均值推断 |
| Multiple testing | 同时检验许多假设时控制偶然显著的方法 |
| `config_hash` | 规范研究配置的 SHA-256 身份 |
| `catalog_hash` | 当前全部 Canonical 数据集组合身份 |
| Manifest | 记录输入身份、文件哈希、Schema、行数和排序的清单 |
