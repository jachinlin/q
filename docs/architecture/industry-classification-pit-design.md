# SW2021 行业成员 PIT 设计

行业目录来自 Tushare `index_classify(level=L1, src=SW2021)`，行业成员来自
`index_member_all`，按申万一级行业切片，不按股票请求。

`industry_catalog` 保存一级行业代码和名称；`industry_membership` 保存一级至三级
代码、`instrument_id`、`in_date/out_date`、`is_current`，以及进入和退出事件各自的
可用时间。查询日状态要求进入事件当时已知，并仅在退出事件当时已知且已经生效时
排除该成员，避免用后来获取的信息回写历史。

研究仓库提供 `industry_catalog()` 和 `industry_memberships_on_dates()`。股票池、因子
研究和 Dashboard 在内存中按查询日组合成员关系，不持久化跨数据集聚合视图。
