import { describe, expect, it } from 'vitest'

import {
  createDefaultStrategyStudyForm,
  defaultStrategy,
  definitionFromStrategyStudyForm,
  strategyStudyFormFromDefinition,
} from './strategy-study-form'

describe('strategy study form adapter', () => {
  it('serializes all direct dual MA parameters', () => {
    const form = createDefaultStrategyStudyForm()
    const result = definitionFromStrategyStudyForm(form)
    expect(result.strategy).toEqual({
      strategy_id: 'dual_ma_trend',
      parameters: { instrument_id: '510300.SH', short_window: 20, long_window: 60, long_weight: 1, flat_weight: 0, target_tolerance: 0.001 },
    })
    expect(result.initial_cash_fen).toBe(100_000_000)
  })

  it('serializes the complete ETF pipeline with stable paired momentum rows', () => {
    const form = createDefaultStrategyStudyForm()
    form.strategy = defaultStrategy('etf_rotation')
    if (form.strategy.strategy_id !== 'etf_rotation') throw new Error('unexpected strategy')
    form.strategy.alpha.momentum = [{ window: 120, weight: 0.5 }, { window: 20, weight: 0.2 }, { window: 60, weight: 0.3 }]
    const result = definitionFromStrategyStudyForm(form)
    if (result.strategy.strategy_id !== 'etf_rotation') throw new Error('unexpected strategy')
    expect(result.strategy.parameters.pipeline.frequency).toBe('MONTHLY')
    expect(result.strategy.parameters.pipeline.alpha.params).toMatchObject({
      momentum_windows: [20, 60, 120], momentum_weights: [0.2, 0.3, 0.5],
    })
    expect(result.strategy.parameters.pipeline.constraints.params).toMatchObject({ max_positions: 2, max_position_weight: 0.5 })
  })

  it('serializes and hydrates the complete stock pipeline', () => {
    const form = createDefaultStrategyStudyForm()
    form.strategy = defaultStrategy('stock_multifactor')
    const definition = definitionFromStrategyStudyForm(form)
    if (definition.strategy.strategy_id !== 'stock_multifactor') throw new Error('unexpected strategy')
    expect(definition.strategy.parameters.pipeline.frequency).toBe('WEEKLY')
    expect(definition.strategy.parameters.pipeline.alpha.params).toMatchObject({
      factor_weights: { book_to_price_mrq: 0.5, momentum_120_20: 0.5 }, min_valid_factors: 2,
    })
    expect(definitionFromStrategyStudyForm(strategyStudyFormFromDefinition(definition))).toEqual(definition)
  })

  it('rejects duplicate dynamic keys before backend validation', () => {
    const form = createDefaultStrategyStudyForm()
    form.strategy = defaultStrategy('stock_multifactor')
    if (form.strategy.strategy_id !== 'stock_multifactor') throw new Error('unexpected strategy')
    form.strategy.alpha.factor_weights.push({ factor_id: 'book_to_price_mrq', weight: 1 })
    expect(() => definitionFromStrategyStudyForm(form)).toThrow('因子不能重复')
  })

  it('serializes only fields belonging to the selected component models', () => {
    const form = createDefaultStrategyStudyForm()
    form.strategy = defaultStrategy('etf_rotation')
    if (form.strategy.strategy_id !== 'etf_rotation') throw new Error('unexpected strategy')
    form.strategy.pipeline.risk.model_id = 'none'
    form.strategy.pipeline.cost.model_id = 'fixed_bps'
    form.strategy.pipeline.construction.model_id = 'top_n_equal_weight'
    const definition = definitionFromStrategyStudyForm(form)
    if (definition.strategy.strategy_id !== 'etf_rotation') throw new Error('unexpected strategy')
    expect(definition.strategy.parameters.pipeline.risk.params).toEqual({})
    expect(definition.strategy.parameters.pipeline.cost.params).toEqual({})
    expect(definition.strategy.parameters.pipeline.construction.params).toEqual({ top_n: 2 })
  })
})
