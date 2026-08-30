import type {
  CrossSectionalPipelineParameters,
  DualMAStrategyParameters,
  StrategyComponentRef,
  StrategyStudyDefinition,
} from './types'

export type RiskForm = {
  model_id: 'none' | 'sample_cov' | 'shrinkage'
  lookback: number
  shrinkage: number
}

export type CostForm = {
  model_id: 'fixed_bps' | 'linear_impact' | 'sqrt_impact'
  impact_bps: number
  max_participation: number
}

export type ConstructionForm = {
  model_id: 'top_n_equal_weight' | 'mean_variance'
  top_n: number
  risk_aversion: number
  cost_aversion: number
  iterations: number
  learning_rate: number
}

export type ConstraintForm = {
  min_positions: number
  max_positions: number
  max_position_weight: number
  max_turnover: number
  max_industry_weight: number
  min_adv_amount: number
  long_exposure: number
}

type PipelineForm = {
  target_tolerance: number
  risk: RiskForm
  cost: CostForm
  construction: ConstructionForm
  constraints: ConstraintForm
}

export type EtfAlphaForm = {
  model_id: 'single_factor' | 'multi_factor_composite'
  etf_pool: string[]
  lookback: number
  direction: number
  momentum: Array<{ window: number; weight: number }>
  trend_window: number
  trend_weight: number
  volatility_window: number
  volatility_weight: number
}

export type StockAlphaForm = {
  model_id: 'single_factor' | 'multi_factor_composite'
  factor_id: string
  direction: number
  factor_weights: Array<{ factor_id: string; weight: number }>
  min_valid_factors: number
}

export type StrategyForm =
  | { strategy_id: 'dual_ma_trend'; parameters: DualMAStrategyParameters }
  | { strategy_id: 'etf_rotation'; pipeline: PipelineForm; alpha: EtfAlphaForm }
  | { strategy_id: 'stock_multifactor'; pipeline: PipelineForm; alpha: StockAlphaForm }

export type StrategyStudyFormState = {
  name: string
  description: string
  tags: string[]
  start_date: string
  end_date: string
  benchmark: string
  initial_cash_fen: number
  execution: StrategyStudyDefinition['execution']
  strategy: StrategyForm
}

const object = (value: unknown, field: string): Record<string, unknown> => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${field} 必须是对象`)
  return value as Record<string, unknown>
}

const text = (value: unknown, field: string): string => {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`${field} 不能为空`)
  return value
}

const number = (value: unknown, field: string): number => {
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`${field} 必须是有限数字`)
  return value
}

const integer = (value: unknown, field: string): number => {
  const result = number(value, field)
  if (!Number.isInteger(result)) throw new Error(`${field} 必须是整数`)
  return result
}

const optionalParams = (value: StrategyComponentRef, field: string): Record<string, unknown> => {
  if (value.params === undefined) return {}
  return object(value.params, `${field}.params`)
}

const stringList = (value: unknown, field: string): string[] => {
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string' || !item.trim())) {
    throw new Error(`${field} 必须是非空字符串列表`)
  }
  return value as string[]
}

const numericList = (value: unknown, field: string, integers = false): number[] => {
  if (!Array.isArray(value)) throw new Error(`${field} 必须是数字列表`)
  return value.map((item, index) => integers ? integer(item, `${field}[${index}]`) : number(item, `${field}[${index}]`))
}

const uniqueSorted = (values: string[], field: string): string[] => {
  const normalized = values.map((item) => item.trim()).filter(Boolean)
  if (new Set(normalized).size !== normalized.length) throw new Error(`${field} 不能重复`)
  return [...normalized].sort()
}

export function defaultRisk(model_id: RiskForm['model_id'] = 'none'): RiskForm {
  return { model_id, lookback: 60, shrinkage: 0.2 }
}

export function defaultCost(model_id: CostForm['model_id'] = 'fixed_bps'): CostForm {
  return { model_id, impact_bps: 0, max_participation: 0.1 }
}

export function defaultConstruction(model_id: ConstructionForm['model_id'] = 'top_n_equal_weight', top_n = 20): ConstructionForm {
  return { model_id, top_n, risk_aversion: 1, cost_aversion: 1, iterations: 200, learning_rate: 0.05 }
}

const defaultConstraints = (kind: 'etf' | 'stock'): ConstraintForm => kind === 'etf'
  ? { min_positions: 1, max_positions: 2, max_position_weight: 0.5, max_turnover: 1, max_industry_weight: 1, min_adv_amount: 0, long_exposure: 1 }
  : { min_positions: 10, max_positions: 20, max_position_weight: 0.05, max_turnover: 0.4, max_industry_weight: 0.3, min_adv_amount: 50_000_000, long_exposure: 1 }

const defaultPipeline = (kind: 'etf' | 'stock'): PipelineForm => ({
  target_tolerance: 0.001,
  risk: defaultRisk(),
  cost: defaultCost(),
  construction: defaultConstruction('top_n_equal_weight', kind === 'etf' ? 2 : 20),
  constraints: defaultConstraints(kind),
})

export function defaultStrategy(strategy_id: StrategyForm['strategy_id']): StrategyForm {
  if (strategy_id === 'dual_ma_trend') {
    return {
      strategy_id,
      parameters: { instrument_id: '510300.SH', short_window: 20, long_window: 60, long_weight: 1, flat_weight: 0, target_tolerance: 0.001 },
    }
  }
  if (strategy_id === 'etf_rotation') {
    return {
      strategy_id,
      pipeline: defaultPipeline('etf'),
      alpha: {
        model_id: 'multi_factor_composite', etf_pool: ['510050.SH', '510300.SH', '513100.SH', '588000.SH'],
        lookback: 60, direction: 1,
        momentum: [{ window: 20, weight: 0.2 }, { window: 60, weight: 0.3 }, { window: 120, weight: 0.5 }],
        trend_window: 60, trend_weight: 0.5, volatility_window: 20, volatility_weight: 0.5,
      },
    }
  }
  return {
    strategy_id,
    pipeline: defaultPipeline('stock'),
    alpha: {
      model_id: 'multi_factor_composite', factor_id: 'book_to_price_mrq', direction: 1,
      factor_weights: [{ factor_id: 'book_to_price_mrq', weight: 0.5 }, { factor_id: 'momentum_120_20', weight: 0.5 }],
      min_valid_factors: 2,
    },
  }
}

export function createDefaultStrategyStudyForm(): StrategyStudyFormState {
  return {
    name: '双均线趋势', description: '沪深300ETF 短长均线趋势', tags: ['trend'],
    start_date: '2018-01-01', end_date: '2024-12-31', benchmark: '000300.SH', initial_cash_fen: 100_000_000,
    execution: { reference_price: 'OPEN', slippage_bps: 5, max_volume_participation: 0.1, limit_order_policy: 'REJECT' },
    strategy: defaultStrategy('dual_ma_trend'),
  }
}

const component = (model_id: string, params: Record<string, unknown>): StrategyComponentRef => ({ model_id, params })

const pipelineDefinition = (
  form: Extract<StrategyForm, { strategy_id: 'etf_rotation' | 'stock_multifactor' }>,
): CrossSectionalPipelineParameters => {
  const riskParams: Record<string, unknown> = form.pipeline.risk.model_id === 'none' ? {} : { lookback: integer(form.pipeline.risk.lookback, '风险窗口') }
  if (form.pipeline.risk.model_id === 'shrinkage') riskParams.shrinkage = number(form.pipeline.risk.shrinkage, '收缩强度')
  const costParams = form.pipeline.cost.model_id === 'fixed_bps' ? {} : {
    impact_bps: number(form.pipeline.cost.impact_bps, '冲击成本'),
    max_participation: number(form.pipeline.cost.max_participation, '最大参与率'),
  }
  const constructionParams = form.pipeline.construction.model_id === 'top_n_equal_weight'
    ? { top_n: integer(form.pipeline.construction.top_n, '持仓数量') }
    : {
        risk_aversion: number(form.pipeline.construction.risk_aversion, '风险厌恶'),
        cost_aversion: number(form.pipeline.construction.cost_aversion, '成本厌恶'),
        iterations: integer(form.pipeline.construction.iterations, '迭代次数'),
        learning_rate: number(form.pipeline.construction.learning_rate, '学习率'),
      }
  let alpha: StrategyComponentRef
  if (form.strategy_id === 'etf_rotation') {
    const pool = uniqueSorted(form.alpha.etf_pool, 'ETF 池')
    if (!pool.length) throw new Error('ETF 池不能为空')
    if (form.alpha.model_id === 'single_factor') {
      alpha = component('single_factor', { etf_pool: pool, lookback: integer(form.alpha.lookback, '回看窗口'), direction: number(form.alpha.direction, '方向') })
    } else {
      if (!form.alpha.momentum.length) throw new Error('动量窗口不能为空')
      const rows = [...form.alpha.momentum].sort((left, right) => left.window - right.window)
      if (new Set(rows.map((item) => item.window)).size !== rows.length) throw new Error('动量窗口不能重复')
      alpha = component('multi_factor_composite', {
        etf_pool: pool,
        momentum_windows: rows.map((item) => integer(item.window, '动量窗口')),
        momentum_weights: rows.map((item) => number(item.weight, '动量权重')),
        trend_window: integer(form.alpha.trend_window, '趋势窗口'), trend_weight: number(form.alpha.trend_weight, '趋势权重'),
        volatility_window: integer(form.alpha.volatility_window, '波动率窗口'), volatility_weight: number(form.alpha.volatility_weight, '波动率权重'),
      })
    }
  } else if (form.alpha.model_id === 'single_factor') {
    alpha = component('single_factor', { factor_id: text(form.alpha.factor_id, '因子'), direction: number(form.alpha.direction, '方向') })
  } else {
    if (!form.alpha.factor_weights.length) throw new Error('因子权重不能为空')
    const rows = [...form.alpha.factor_weights].sort((left, right) => left.factor_id.localeCompare(right.factor_id))
    const ids = rows.map((item) => text(item.factor_id, '因子'))
    if (new Set(ids).size !== ids.length) throw new Error('因子不能重复')
    alpha = component('multi_factor_composite', {
      factor_weights: Object.fromEntries(rows.map((item) => [item.factor_id, number(item.weight, `因子 ${item.factor_id} 权重`)])),
      min_valid_factors: integer(form.alpha.min_valid_factors, '最少有效因子数'),
    })
  }
  const constraints = form.pipeline.constraints
  return {
    pipeline: {
      frequency: form.strategy_id === 'etf_rotation' ? 'MONTHLY' : 'WEEKLY',
      target_tolerance: number(form.pipeline.target_tolerance, '目标差额容差'), alpha,
      risk: component(form.pipeline.risk.model_id, riskParams), cost: component(form.pipeline.cost.model_id, costParams),
      construction: component(form.pipeline.construction.model_id, constructionParams),
      constraints: component('long_only', {
        min_positions: integer(constraints.min_positions, '最少持仓数'), max_positions: integer(constraints.max_positions, '最大持仓数'),
        max_position_weight: number(constraints.max_position_weight, '单标的权重上限'), max_turnover: number(constraints.max_turnover, '换手上限'),
        max_industry_weight: number(constraints.max_industry_weight, '行业权重上限'), min_adv_amount: number(constraints.min_adv_amount, '最小 ADV'),
        long_exposure: number(constraints.long_exposure, '多头敞口'),
      }),
    },
  }
}

export function definitionFromStrategyStudyForm(form: StrategyStudyFormState): StrategyStudyDefinition {
  const tags = [...new Set(form.tags.map((item) => item.trim()).filter(Boolean))].sort()
  let strategy: StrategyStudyDefinition['strategy']
  if (form.strategy.strategy_id === 'dual_ma_trend') {
    const parameters = form.strategy.parameters
    strategy = { strategy_id: 'dual_ma_trend', parameters: {
      instrument_id: text(parameters.instrument_id, '标的'), short_window: integer(parameters.short_window, '短均线窗口'),
      long_window: integer(parameters.long_window, '长均线窗口'), long_weight: number(parameters.long_weight, '多头权重'),
      flat_weight: number(parameters.flat_weight, '空仓权重'), target_tolerance: number(parameters.target_tolerance, '目标差额容差'),
    } }
  } else if (form.strategy.strategy_id === 'etf_rotation') {
    strategy = { strategy_id: 'etf_rotation', parameters: pipelineDefinition(form.strategy) }
  } else {
    strategy = { strategy_id: 'stock_multifactor', parameters: pipelineDefinition(form.strategy) }
  }
  return {
    name: text(form.name, '研究名称'), description: form.description, tags,
    start_date: text(form.start_date, '开始日期'), end_date: text(form.end_date, '结束日期'), strategy,
    benchmark: text(form.benchmark, '基准'), initial_cash_fen: integer(form.initial_cash_fen, '初始资金'),
    execution: {
      reference_price: form.execution.reference_price, slippage_bps: number(form.execution.slippage_bps, '滑点'),
      max_volume_participation: number(form.execution.max_volume_participation, '最大成交量参与率'), limit_order_policy: form.execution.limit_order_policy,
    },
  }
}

const ref = (value: unknown, field: string): StrategyComponentRef => {
  const raw = object(value, field)
  return { model_id: text(raw.model_id, `${field}.model_id`), params: raw.params === undefined ? {} : object(raw.params, `${field}.params`) }
}

const pipelineFormFromDefinition = (parameters: CrossSectionalPipelineParameters, kind: 'etf' | 'stock'): { pipeline: PipelineForm; alpha: EtfAlphaForm | StockAlphaForm } => {
  const raw = object(parameters, 'strategy.parameters')
  const pipelineRaw = object(raw.pipeline, 'pipeline')
  const expectedFrequency = kind === 'etf' ? 'MONTHLY' : 'WEEKLY'
  if (pipelineRaw.frequency !== expectedFrequency) throw new Error(`frequency 必须是 ${expectedFrequency}`)
  const riskRef = ref(pipelineRaw.risk, 'risk')
  const costRef = ref(pipelineRaw.cost, 'cost')
  const constructionRef = ref(pipelineRaw.construction, 'construction')
  const constraintRef = ref(pipelineRaw.constraints, 'constraints')
  const alphaRef = ref(pipelineRaw.alpha, 'alpha')
  if (!['none', 'sample_cov', 'shrinkage'].includes(riskRef.model_id)) throw new Error('不支持的 Risk 模型')
  if (!['fixed_bps', 'linear_impact', 'sqrt_impact'].includes(costRef.model_id)) throw new Error('不支持的 Cost 模型')
  if (!['top_n_equal_weight', 'mean_variance'].includes(constructionRef.model_id)) throw new Error('不支持的 Construction 模型')
  if (constraintRef.model_id !== 'long_only') throw new Error('只支持 long_only 约束')
  const defaults = defaultPipeline(kind)
  const riskParams = optionalParams(riskRef, 'risk')
  const costParams = optionalParams(costRef, 'cost')
  const constructionParams = optionalParams(constructionRef, 'construction')
  const constraintParams = optionalParams(constraintRef, 'constraints')
  const pipeline: PipelineForm = {
    target_tolerance: pipelineRaw.target_tolerance === undefined ? 0.001 : number(pipelineRaw.target_tolerance, 'target_tolerance'),
    risk: { ...defaults.risk, model_id: riskRef.model_id as RiskForm['model_id'], lookback: riskParams.lookback === undefined ? 60 : integer(riskParams.lookback, 'lookback'), shrinkage: riskParams.shrinkage === undefined ? 0.2 : number(riskParams.shrinkage, 'shrinkage') },
    cost: { ...defaults.cost, model_id: costRef.model_id as CostForm['model_id'], impact_bps: costParams.impact_bps === undefined ? 0 : number(costParams.impact_bps, 'impact_bps'), max_participation: costParams.max_participation === undefined ? 0.1 : number(costParams.max_participation, 'max_participation') },
    construction: {
      ...defaults.construction, model_id: constructionRef.model_id as ConstructionForm['model_id'],
      top_n: constructionParams.top_n === undefined ? defaults.construction.top_n : integer(constructionParams.top_n, 'top_n'),
      risk_aversion: constructionParams.risk_aversion === undefined ? 1 : number(constructionParams.risk_aversion, 'risk_aversion'),
      cost_aversion: constructionParams.cost_aversion === undefined ? 1 : number(constructionParams.cost_aversion, 'cost_aversion'),
      iterations: constructionParams.iterations === undefined ? 200 : integer(constructionParams.iterations, 'iterations'),
      learning_rate: constructionParams.learning_rate === undefined ? 0.05 : number(constructionParams.learning_rate, 'learning_rate'),
    },
    constraints: {
      min_positions: constraintParams.min_positions === undefined ? defaults.constraints.min_positions : integer(constraintParams.min_positions, 'min_positions'),
      max_positions: constraintParams.max_positions === undefined ? defaults.constraints.max_positions : integer(constraintParams.max_positions, 'max_positions'),
      max_position_weight: constraintParams.max_position_weight === undefined ? defaults.constraints.max_position_weight : number(constraintParams.max_position_weight, 'max_position_weight'),
      max_turnover: constraintParams.max_turnover === undefined ? defaults.constraints.max_turnover : number(constraintParams.max_turnover, 'max_turnover'),
      max_industry_weight: constraintParams.max_industry_weight === undefined ? defaults.constraints.max_industry_weight : number(constraintParams.max_industry_weight, 'max_industry_weight'),
      min_adv_amount: constraintParams.min_adv_amount === undefined ? defaults.constraints.min_adv_amount : number(constraintParams.min_adv_amount, 'min_adv_amount'),
      long_exposure: constraintParams.long_exposure === undefined ? defaults.constraints.long_exposure : number(constraintParams.long_exposure, 'long_exposure'),
    },
  }
  const alphaParams = optionalParams(alphaRef, 'alpha')
  if (!['single_factor', 'multi_factor_composite'].includes(alphaRef.model_id)) throw new Error('不支持的 Alpha 模型')
  if (kind === 'etf') {
    const defaultsAlpha = (defaultStrategy('etf_rotation') as Extract<StrategyForm, { strategy_id: 'etf_rotation' }>).alpha
    const etf_pool = stringList(alphaParams.etf_pool, 'etf_pool')
    const windows = alphaParams.momentum_windows === undefined ? defaultsAlpha.momentum.map((item) => item.window) : numericList(alphaParams.momentum_windows, 'momentum_windows', true)
    const weights = alphaParams.momentum_weights === undefined ? defaultsAlpha.momentum.map((item) => item.weight) : numericList(alphaParams.momentum_weights, 'momentum_weights')
    if (windows.length !== weights.length) throw new Error('动量窗口与权重数量必须一致')
    return { pipeline, alpha: {
      ...defaultsAlpha, model_id: alphaRef.model_id as EtfAlphaForm['model_id'], etf_pool,
      lookback: alphaParams.lookback === undefined ? 60 : integer(alphaParams.lookback, 'lookback'),
      direction: alphaParams.direction === undefined ? 1 : number(alphaParams.direction, 'direction'),
      momentum: windows.map((window, index) => ({ window, weight: weights[index] })),
      trend_window: alphaParams.trend_window === undefined ? 60 : integer(alphaParams.trend_window, 'trend_window'),
      trend_weight: alphaParams.trend_weight === undefined ? 1 : number(alphaParams.trend_weight, 'trend_weight'),
      volatility_window: alphaParams.volatility_window === undefined ? 20 : integer(alphaParams.volatility_window, 'volatility_window'),
      volatility_weight: alphaParams.volatility_weight === undefined ? 1 : number(alphaParams.volatility_weight, 'volatility_weight'),
    } }
  }
  const defaultsAlpha = (defaultStrategy('stock_multifactor') as Extract<StrategyForm, { strategy_id: 'stock_multifactor' }>).alpha
  const rawWeights = alphaParams.factor_weights === undefined ? Object.fromEntries(defaultsAlpha.factor_weights.map((item) => [item.factor_id, item.weight])) : object(alphaParams.factor_weights, 'factor_weights')
  const factorWeights = Object.entries(rawWeights).map(([factor_id, weight]) => ({ factor_id, weight: number(weight, `factor_weights.${factor_id}`) })).sort((left, right) => left.factor_id.localeCompare(right.factor_id))
  return { pipeline, alpha: {
    ...defaultsAlpha, model_id: alphaRef.model_id as StockAlphaForm['model_id'],
    factor_id: alphaParams.factor_id === undefined ? defaultsAlpha.factor_id : text(alphaParams.factor_id, 'factor_id'),
    direction: alphaParams.direction === undefined ? 1 : number(alphaParams.direction, 'direction'), factor_weights: factorWeights,
    min_valid_factors: alphaParams.min_valid_factors === undefined ? factorWeights.length : integer(alphaParams.min_valid_factors, 'min_valid_factors'),
  } }
}

export function strategyStudyFormFromDefinition(value: unknown): StrategyStudyFormState {
  const raw = object(value, 'definition')
  const strategyRaw = object(raw.strategy, 'strategy')
  const strategy_id = text(strategyRaw.strategy_id, 'strategy_id')
  const parameters = object(strategyRaw.parameters, 'strategy.parameters')
  let strategy: StrategyForm
  if (strategy_id === 'dual_ma_trend') {
    strategy = { strategy_id, parameters: {
      instrument_id: text(parameters.instrument_id, 'instrument_id'),
      short_window: parameters.short_window === undefined ? 20 : integer(parameters.short_window, 'short_window'),
      long_window: parameters.long_window === undefined ? 60 : integer(parameters.long_window, 'long_window'),
      long_weight: parameters.long_weight === undefined ? 1 : number(parameters.long_weight, 'long_weight'),
      flat_weight: parameters.flat_weight === undefined ? 0 : number(parameters.flat_weight, 'flat_weight'),
      target_tolerance: parameters.target_tolerance === undefined ? 0.001 : number(parameters.target_tolerance, 'target_tolerance'),
    } }
  } else if (strategy_id === 'etf_rotation') {
    const result = pipelineFormFromDefinition(parameters as CrossSectionalPipelineParameters, 'etf')
    strategy = { strategy_id, pipeline: result.pipeline, alpha: result.alpha as EtfAlphaForm }
  } else if (strategy_id === 'stock_multifactor') {
    const result = pipelineFormFromDefinition(parameters as CrossSectionalPipelineParameters, 'stock')
    strategy = { strategy_id, pipeline: result.pipeline, alpha: result.alpha as StockAlphaForm }
  } else {
    throw new Error(`表单暂不支持策略 ${strategy_id}`)
  }
  const execution = object(raw.execution, 'execution')
  if (!Array.isArray(raw.tags) || raw.tags.some((item) => typeof item !== 'string')) throw new Error('tags 必须是字符串列表')
  return {
    name: text(raw.name, 'name'), description: typeof raw.description === 'string' ? raw.description : '', tags: raw.tags as string[],
    start_date: text(raw.start_date, 'start_date'), end_date: text(raw.end_date, 'end_date'), strategy,
    benchmark: text(raw.benchmark, 'benchmark'), initial_cash_fen: integer(raw.initial_cash_fen, 'initial_cash_fen'),
    execution: {
      reference_price: text(execution.reference_price, 'reference_price'), slippage_bps: number(execution.slippage_bps, 'slippage_bps'),
      max_volume_participation: number(execution.max_volume_participation, 'max_volume_participation'), limit_order_policy: text(execution.limit_order_policy, 'limit_order_policy'),
    },
  }
}
