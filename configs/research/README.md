# 研究配置字段说明

`examples/stock_multifactor.yaml`、`dual_ma_trend.yaml` 和 `etf_rotation.yaml` 可直接传给
`quant research validate|submit`。配置严格校验，未知字段、隐式日期、重叠样本区间、无效
组件组合和超过 256 个候选均会在提交前失败。

- `name`、`hypothesis`：研究问题，不是策略实现名称。
- `research_mode`：信号、理论组合或完整回测深度。
- `strategy_id`：三个组件模板之一。
- `benchmark`、`initial_cash_fen`：规范基准证券和整数分初始现金。
- `research_protocol.train|validation|test`：互不重叠的明确日期闭区间。
- `parameter_search_space`：点分配置路径到离散列表；路径排序后确定性笛卡尔展开。
- `selection`：仅使用 VALIDATION 的主指标方向、约束、次指标和多重检验校正。
- `universe` 到 `analytics`：类型化组件配置；`component`、`estimator`、`constructor`、
  `policy` 或 `simulator` 是后端组件目录中的稳定 ID。

Dashboard 的研究编排器通过 `/api/v1/research/components` 获取同一组件 Schema，并通过
后端 parse/preview 接口取得唯一规范化结果，前端不自行定义第二套校验语义。
