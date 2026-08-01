# BaoStock 涨跌幅前复权设计

**日期：** 2026-08-01
**状态：** 待用户审阅
**范围：** `PriceAdjustmentService`、BaoStock 日线时点映射、ETF/股票行情因子及对应缓存身份

## 1. 背景与目标

当前研究价格使用公司行动数据计算后复权。BaoStock 生产采集链路目前只稳定提供证券、交易日历和不复权日线，尚未提供完整公司行动数据，因此真实快照上的 `BACKWARD` 请求会因缺少 `CORPORATE_ACTION` 数据集而失败。

本设计新增基于 BaoStock `close/preclose` 涨跌幅复权法的前复权模式。研究行情因子统一改用前复权价格，从现有不复权日线直接计算，不再依赖公司行动数据。Canonical 原始行情保持不可变，复权只发生在价格服务返回结果中。

目标：

- 新增 `AdjustmentMode.FORWARD`，使用本地原始日线计算前复权价格。
- ETF 与股票市场类因子统一使用 `FORWARD`。
- 只调整价格列，不改变真实成交量、成交额及估值字段。
- 保证逐信号日时点安全，延长实验结束日期不得改变已有历史信号。
- 修正 BaoStock 历史日线的业务可用时间，使今日下载的历史数据可用于历史 PIT 研究。
- 通过稳定、可审计的实现版本使旧复权算法缓存失效。

非目标：

- 本轮不删除 `RAW` 或现有 `BACKWARD` 接口。
- 本轮不把 BaoStock 公司行动或财务接口接入生产采集。
- 本轮不调整 `volume`、`amount`、`turnover` 或估值字段。
- 本轮不以逐证券 `query_history_k_data_plus(adjustflag=2)` 作为生产研究路径。

## 2. 方案选择

### 2.1 采用方案：从 raw bars 本地计算

使用已进入 Canonical 的不复权 `close` 和 `preclose` 计算相邻 session 的价格跳变，再以查询锚点向前累计前复权因子。

优点：

- 不新增网络请求和生产数据集依赖。
- 兼容现有按交易日批量下载全市场数据的 20 年 bootstrap 路径。
- 原始证据、计算逻辑和输出因子均可审计。
- 可在单元测试中使用独立公式验证。

### 2.2 暂不采用的方案

`query_daily_adjust_factor` 可在后续作为供应商交叉校验数据，但本轮不把它变成计算依赖，否则需要新增 Raw/Canonical schema、快照发布和质量规则。

逐证券请求 `query_history_k_data_plus(adjustflag=2)` 虽能直接得到前复权行情，但全市场 20 年请求量过大，也会破坏“原始数据先落 Raw，再由确定性服务派生研究价格”的边界。

## 3. 前复权公式

对每个证券按 `trade_date` 升序排列原始日线。设查询锚点之前最后一个有效观测为 `T`：

```text
factor[T] = 1

factor[t-1] =
    factor[t] × raw_preclose[t] / raw_close[t-1]
```

等价的对数域形式为：

```text
log_factor[T] = 0

log_factor[t-1] =
    log_factor[t]
    + log(raw_preclose[t])
    - log(raw_close[t-1])
```

最终价格：

```text
adjusted_open[t]     = raw_open[t]     × factor[t]
adjusted_high[t]     = raw_high[t]     × factor[t]
adjusted_low[t]      = raw_low[t]      × factor[t]
adjusted_close[t]    = raw_close[t]    × factor[t]
adjusted_preclose[t] = raw_preclose[t] × factor[t]
```

`preclose` 既是跳变因子的原始输入，也是复权输出的一部分。计算因子时只使用未修改的 raw `close/preclose`；因子完成后才生成复权列，因此不存在循环依赖。

正确的复权结果应在相邻有效 session 满足：

```text
adjusted_close[t-1] == adjusted_preclose[t]
```

允许正常浮点误差。

以下字段保持原始值：

```text
volume
amount
turnover
pct_change
pe_ttm
pb_mrq
ps_ttm
pcf_ncf_ttm
```

若未来需要连续成交量研究，应新增独立派生字段，不得覆盖真实 `volume`。

## 4. 服务接口与数据流

`PriceAdjustmentService.bars()` 保持现有参数：

```python
bars(
    snapshot_id,
    instruments,
    start,
    end,
    mode,
    as_of,
) -> pl.LazyFrame
```

新增 `AdjustmentMode.FORWARD`。

语义：

- `RAW`：返回原始价格和单位因子。
- `FORWARD`：以 `as_of` 当日或之前最后一个有效交易观测为锚点计算前复权；只返回 `[start, end]`。
- `BACKWARD`：暂时保留当前兼容接口，不再由内置研究因子使用。

为计算 `FORWARD`，底层读取每个证券 `[start, as_of]` 的原始日线。`as_of` 必须不早于 `end`。输出继续携带：

- `adjustment_mode = "FORWARD"`
- `adjustment_as_of`
- `adjustment_factor`

公司行动专用 lineage 字段只属于 `BACKWARD` 兼容路径。`FORWARD` 不得伪造公司行动；其 lineage 完全来自参与计算的原始 bars。

## 5. 逐信号日 PIT 语义

统一以 `ctx.end` 为锚点生成一条前复权序列，会让较早价格包含后续跳变。收益率和波动率通常对统一尺度不敏感，但当前趋势公式按 `mean(log(price))` 归一，并非尺度不变，因此市场因子必须使用信号日独立锚点。

为避免逐日重新读取数据，批量实现采用累计因子换基：

```text
global_adjusted_price[d] = raw_price[d] × factor[d, ctx.end]

signal_adjusted_price[d, s] =
    global_adjusted_price[d] / factor[s, ctx.end]
```

其中 `d <= s`。该换基等价于直接计算 `factor[d, s]`，并消除 `s` 之后的跳变影响。

每个信号窗口的 `available_at` 为该窗口实际使用的 raw bar 时间最大值。`FORWARD` 不读取公司行动，因此不附加公司行动公告时间。

必须通过以下不变性验证：

```text
固定 snapshot、signal_date 及 signal_date 之前的 raw bars，
仅延长 ctx.end 或追加 signal_date 之后的行情，
已有 signal_date 的因子值和 available_at 不变。
```

## 6. BaoStock 历史行情可用时间

当前 mapper 将历史日线和交易状态的 `available_at` 设为实际抓取时间。这会使今日 bootstrap 的历史数据在过去 `as_of` 下全部不可见。

修正为双时间语义：

- `available_at`：该交易日收盘时点，使用 `Asia/Shanghai` 市场收盘时间并转 UTC，表示完整日线最早成为公开事实的时间。
- `ingested_at`：BaoStock 实际 `retrieved_at`，表示本地何时获得该证据。
- `availability_source`：明确标识为从交易日收盘规则派生，而不是供应商公告时间。

采集任务只应发布已完成的日线。若目标日期尚未收盘，不得通过派生 `available_at` 将未完成数据标记为已知。

该修正同时适用于由 daily API 派生的 `DAILY_BAR` 和 `SECURITY_STATUS`，使历史股票池和流动性规则能在历史 `as_of` 正常工作。

## 7. 数据质量与错误处理

- 每个证券独立排序、累计；不得跨证券连接。
- `(instrument_id, trade_date)` 重复时立即拒绝。
- 除首个观测外，每个跳变所需的 `raw_close[t-1]` 与 `raw_preclose[t]` 必须非 null、有限且大于零。
- IPO 首行的 `preclose` 可以为空或零，因为它不用于更早观测的跳变。
- 日期缺口与停牌按实际观测 session 连接，不填造日历行。
- `open/high/low/close` 中参与输出的非 null 值必须有限；必要的 `close` 无效时整次请求 fail closed。
- 累计对数因子和还原后的价格必须有限；出现溢出、下溢为零或非法因子时拒绝结果。
- 空输入返回稳定、精确 schema 的空结果。
- 输入 frame 不得被原地修改；输出按 `(instrument_id, trade_date)` 稳定排序。

## 8. 因子、注册与缓存

- ETF 与股票行情因子统一声明 `adjustment_mode="FORWARD"`。
- ETF/股票共享的 `volatility_60d_v1@1.0.0` 必须由单一 canonical registrar 注册，注册顺序不得影响结果或 code hash。
- 前复权切换属于语义变更，相关因子的 implementation revision 必须升级。
- code hash 必须覆盖真实实现来源或明确的传递闭包版本，不能只依赖长期不变的手工常量。
- 已由旧公司行动算法产生的缓存 key 必须失效；无需删除旧文件，它们将因新 key 不再命中。

## 9. 验收测试

### 9.1 公式与边界

- 无跳变序列的全部因子为 1。
- 单次、多次跳变与独立 NumPy/Python 参考一致。
- 复权后相邻 `adjusted_close` 与 `adjusted_preclose` 连续。
- `open/high/low/close/preclose` 均按当日因子调整。
- `volume/amount` 逐值、dtype 完全不变。
- 多证券、乱序、重复键、停牌、日期缺口、IPO 首行、空表、非法价格。
- 极端有限价格使用对数差，不产生中间比值下溢。

### 9.2 PIT 与集成

- 延长 `ctx.end` 或追加未来行情不改变较早信号。
- raw BaoStock → mapper → immutable snapshot → `FORWARD` → 五个 ETF 因子的集成链路通过。
- 今日抓取的历史日线在对应历史交易日收盘后可见，但在收盘前不可见。
- 历史 universe 使用真实 mapper 输出，不再依赖手工伪造的历史 `available_at`。
- BaoStock 官方固定复权样本与本地计算一致；在线 API 不作为单元测试依赖。

### 9.3 缓存与注册

- 旧/new implementation revision 产生不同 cache key。
- ETF→股票和股票→ETF 两种注册顺序得到相同 factor ref 与 code hash，且不抛重复注册异常。
- 相同输入重复计算命中缓存；改变 snapshot、universe 或实现来源后重新物化。

## 10. 后续工作

完成本设计后，市场行情因子可由 BaoStock 原始日线独立运行。财务质量因子仍需要可靠公告时间的财务数据能力；在接入 Tushare 或其他正式数据源前，实验提交层必须通过 capability gate 明确拒绝，而不是在执行中延迟失败。

BaoStock `query_daily_adjust_factor` 可在后续作为供应商交叉校验源，但不属于本次生产依赖。
