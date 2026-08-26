# 当前架构

本地单用户 A 股研究平台采用 `bootstrap → interfaces → application → capabilities`
依赖方向，SQLite 保存任务和目录元数据，Raw/Canonical Parquet 保存市场数据。

数据层的权威设计见 [data-layer-design.md](data-layer-design.md)，包与组合根边界见
[package-layout.md](package-layout.md)。实验固定捕获 Canonical `catalog_hash`、规则、
配置和代码身份，运行中发现漂移立即失败；组合、订单和持仓只使用可交易的
`InstrumentId`，指数 benchmark 使用独立 `IndexId`。
