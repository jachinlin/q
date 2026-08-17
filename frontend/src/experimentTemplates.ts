export type ExperimentTemplateId = 'etf_rotation' | 'stock_multifactor'

export const EXPERIMENT_TEMPLATES: Record<ExperimentTemplateId, { name: string; description: string; yaml: string }> = {
  etf_rotation: {
    name: 'ETF 轮动',
    description: '月频多周期动量、趋势过滤与波动率惩罚',
    yaml: `strategy_id: etf_rotation
start_date: 2026-07-08
end_date: 2026-08-10
benchmark: 000300.SH
initial_cash_fen: 100000000
strategy_config:
  etf_pool:
    - 510050.SH
    - 510300.SH
    - 513100.SH
    - 588000.SH
  return_factor_weights:
    return_20d: 0.2
    return_60d: 0.3
    return_120d: 0.5
  trend_factor_ref: trend_120d
  volatility_factor_ref: volatility_60d
  volatility_penalty: 0.5
  top_n: 3
  frequency: MONTHLY
  missing_signal_policy: EXCLUDE
  weighting: EQUAL
`,
  },
  stock_multifactor: {
    name: '股票多因子',
    description: '价值、质量、动量与风险四类因子的周频组合',
    yaml: `strategy_id: stock_multifactor
start_date: 2024-01-02
end_date: 2024-12-31
benchmark: 000300.SH
initial_cash_fen: 100000000
strategy_config:
  factor_definitions:
    earnings_yield_ttm: {category: VALUE, direction: 1}
    book_to_price_mrq: {category: VALUE, direction: 1}
    roe_pit: {category: QUALITY, direction: 1}
    momentum_120_20: {category: MOMENTUM, direction: 1}
    volatility_60d: {category: RISK, direction: -1}
    downside_volatility_60d: {category: RISK, direction: -1}
    max_drawdown_120d: {category: RISK, direction: -1}
  category_weights: {VALUE: 0.25, QUALITY: 0.25, MOMENTUM: 0.30, RISK: 0.20}
  min_valid_factors: 5
  mad_multiplier: 3.0
  frequency: WEEKLY
  constraints:
    max_position_weight: 0.10
    min_positions: 20
    max_positions: 50
    min_adv_amount: 10000000.0
    max_turnover: 0.50
`,
  },
}
