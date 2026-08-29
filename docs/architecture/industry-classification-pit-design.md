# SW2021 行业成员 PIT 设计

行业目录来自 Tushare `index_classify(level=L1, src=SW2021)`，行业成员来自
`index_member_all`，按申万一级行业切片，不按股票请求。一级行业参数必须使用完整的
Tushare 指数代码（例如 `801010.SI`），不能省略 `.SI`。每个一级行业分别请求
`is_new=Y` 和 `is_new=N`：前者提供当前成员，后者提供已经退出的历史成员。两类记录
共同构成可按日期回看的 PIT 历史。

`industry_catalog` 保存一级行业代码和名称；`industry_membership` 保存一级至三级
代码、`instrument_id`、`in_date/out_date`、`is_current`，以及进入和退出事件各自的
可用时间。查询日状态要求进入事件当时已知，并仅在退出事件当时已知且已经生效时
排除该成员，避免用后来获取的信息回写历史。

行业成员的全量响应按请求保留全部 Raw 历史观测，不能只使用最新 Raw head 重建。
CURATE 按 `(level1_code, instrument_id, in_date)` 折叠一个成员生命周期：业务字段和
`is_current/out_date` 取最新观测，`in_available_at` 取该生命周期首次被观测的时间，
`out_available_at` 取当前 `out_date` 首次被观测的时间，`ingested_at` 保留最新观测
时间，通用审计列 `available_at` 与 `in_available_at` 对齐。历史响应中首次见到的已
退出成员，其进入和退出可用时间都只能从首次本地观测开始。这样同一一级行业切片中
其他成员发生变化或后续重复刷新时，既有关系的 PIT 事件时间不会向后漂移。

研究仓库提供 `industry_catalog()` 和 `industry_memberships_on_dates()`。股票池、因子
研究和 Dashboard 在内存中按查询日组合成员关系，不持久化跨数据集聚合视图。
仓库对每个“查询日 × 证券”只返回一个 PIT 行业状态。供应商全量刷新留下未闭合的旧
关系时，按最新 `in_date`、最新 `in_available_at`、最新记录 `available_at` 和
`ingested_at` 依次裁决；最终以行业代码稳定打破完全相同时间证据的冲突。该规则只在
查询日已可见的关系中选择，不使用未来退出信息回写历史。
