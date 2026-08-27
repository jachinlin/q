# 实现约束索引

当前实现不保留旧数据源、旧 DatasetKind 或旧 Canonical 路径的兼容入口。

数据目录当前包含 20 个 Tushare Canonical 数据集。其中三张主要财务报表使用
`income_vip`、`balancesheet_vip`、`cashflow_vip` 的全市场合并报表请求；股票和
场内基金分红分别使用 `dividend`、`fund_div` 的自然日事件切片。旧的 15 数据集数据
根必须更换目录并重新 bootstrap，不执行原地迁移。

- 数据与 Tushare 端点：[data-layer-design.md](data-layer-design.md)
- SW2021 PIT 行业：[industry-classification-pit-design.md](industry-classification-pit-design.md)
- 包和依赖边界：[package-layout.md](package-layout.md)

代码、测试和上述文档必须在同一变更中保持一致。
