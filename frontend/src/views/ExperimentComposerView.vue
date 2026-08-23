<script setup lang="ts">
import { useMutation, useQuery } from '@tanstack/vue-query'
import { ElMessage } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'
import { parseDocument } from 'yaml'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import type { ExperimentAggregate, ExperimentValidation, StrategyCatalog } from '../types'

const router = useRouter()
const selected = ref('dual_ma_trend')
const validatedYaml = ref('')
const templates: Record<string, string> = {
  dual_ma_trend: `name: 双均线趋势\ndescription: 沪深300ETF 短长均线趋势\nkind: STRATEGY_BACKTEST\ntags: [trend]\nsample_windows:\n  train: {start: 2018-01-01, end: 2020-12-31}\n  validation: {start: 2021-01-01, end: 2022-12-31}\n  test: {start: 2023-01-01, end: 2024-12-31}\ngovernance: {test_budget: 1, correction: BONFERRONI}\ninitial_run:\n  kind: STRATEGY_BACKTEST\n  start_date: 2018-01-01\n  end_date: 2022-12-31\n  strategy:\n    strategy_id: dual_ma_trend\n    parameters: {instrument_id: 510300.SH, short_window: 20, long_window: 60, long_weight: 1.0}\n  benchmark: 000300.SH\n  initial_cash_fen: 100000000\n  execution: {reference_price: OPEN, slippage_bps: 5.0, max_volume_participation: 0.1, limit_order_policy: REJECT}\n`,
  stock_multifactor: `name: 股票多因子\ndescription: 价值与动量周频组合\nkind: STRATEGY_BACKTEST\ntags: [equity, multifactor]\nsample_windows:\n  train: {start: 2018-01-01, end: 2020-12-31}\n  validation: {start: 2021-01-01, end: 2022-12-31}\n  test: {start: 2023-01-01, end: 2024-12-31}\ngovernance: {test_budget: 1, correction: BH_FDR}\ninitial_run:\n  kind: STRATEGY_BACKTEST\n  start_date: 2018-01-01\n  end_date: 2022-12-31\n  strategy:\n    strategy_id: stock_multifactor\n    parameters:\n      pipeline:\n        frequency: WEEKLY\n        target_tolerance: 0.001\n        alpha: {model_id: multi_factor_composite, params: {factor_weights: {book_to_price_mrq: 0.5, momentum_120_20: 0.5}, min_valid_factors: 2}}\n        risk: {model_id: none}\n        cost: {model_id: fixed_bps}\n        construction: {model_id: top_n_equal_weight, params: {top_n: 10}}\n        constraints: {model_id: long_only, params: {min_positions: 5, max_positions: 10, max_position_weight: 0.1, max_turnover: 0.4, max_industry_weight: 0.3, min_adv_amount: 50000000.0, long_exposure: 1.0}}\n  benchmark: 000300.SH\n  initial_cash_fen: 100000000\n  execution: {reference_price: OPEN, slippage_bps: 5.0, max_volume_participation: 0.1, limit_order_policy: REJECT}\n`,
  etf_rotation: `name: ETF 轮动\ndescription: 固定 ETF 池月频动量、趋势和波动率轮动\nkind: STRATEGY_BACKTEST\ntags: [etf, rotation]\nsample_windows:\n  train: {start: 2018-01-01, end: 2020-12-31}\n  validation: {start: 2021-01-01, end: 2022-12-31}\n  test: {start: 2023-01-01, end: 2024-12-31}\ngovernance: {test_budget: 1, correction: BONFERRONI}\ninitial_run:\n  kind: STRATEGY_BACKTEST\n  start_date: 2018-01-01\n  end_date: 2022-12-31\n  strategy:\n    strategy_id: etf_rotation\n    parameters:\n      pipeline:\n        frequency: MONTHLY\n        target_tolerance: 0.001\n        alpha: {model_id: multi_factor_composite, params: {etf_pool: [510300.SH, 510500.SH], momentum_windows: [20, 60, 120], momentum_weights: [0.2, 0.3, 0.5], trend_window: 60, trend_weight: 0.5, volatility_window: 20, volatility_weight: 0.5}}\n        risk: {model_id: none}\n        cost: {model_id: fixed_bps}\n        construction: {model_id: top_n_equal_weight, params: {top_n: 1}}\n        constraints: {model_id: long_only, params: {min_positions: 1, max_positions: 1, max_position_weight: 1.0, max_turnover: 1.0, max_industry_weight: 1.0, min_adv_amount: 0.0, long_exposure: 1.0}}\n  benchmark: 000300.SH\n  initial_cash_fen: 100000000\n  execution: {reference_price: OPEN, slippage_bps: 5.0, max_volume_participation: 0.1, limit_order_policy: REJECT}\n`,
  factor_study: `name: 价值动量因子研究\ndescription: 统一 Experiment 下的因子诊断\nkind: FACTOR_STUDY\ntags: [factor]\nsample_windows:\n  train: {start: 2018-01-01, end: 2020-12-31}\n  validation: {start: 2021-01-01, end: 2022-12-31}\n  test: {start: 2023-01-01, end: 2024-12-31}\ngovernance: {test_budget: 1, correction: BH_FDR}\ninitial_run:\n  kind: FACTOR_STUDY\n  start_date: 2018-01-01\n  end_date: 2022-12-31\n  factor_study:\n    factor_ids: [book_to_price_mrq, momentum_120_20]\n    universe: {name: CN_STOCK_STANDARD}\n    horizons: [1, 5, 20]\n    quantiles: 5\n    industry:\n      taxonomy: 证监会行业分类\n      unclassified_policy: EXCLUDE\n    cost_bps_scenarios: [5, 10, 20]\n`,
}
const yaml = ref(templates[selected.value])
const catalog = useQuery({ queryKey: ['strategies'], queryFn: () => api.get<StrategyCatalog>('/api/v1/strategies') })
const validate = useMutation({ mutationFn: (candidate: string) => api.post<ExperimentValidation>('/api/v1/experiments/validate', { yaml: candidate }), onSuccess: (_, candidate) => { validatedYaml.value = candidate; ElMessage.success('实验配置有效') } })
const submit = useMutation({ mutationFn: () => api.post<ExperimentAggregate>('/api/v1/experiments', { yaml: validatedYaml.value }), onSuccess: async (value) => { ElMessage.success('实验和首个 Run 已入队'); await router.push(`/experiments/${value.experiment.id}`) } })
const error = computed(() => validate.error.value ?? submit.error.value)
const isCurrentYamlValidated = computed(() => Boolean(validate.data.value) && validatedYaml.value === yaml.value)
function invalidateValidation() { validatedYaml.value = ''; validate.reset() }
const pipelineModels = computed<Record<string, string>>(() => {
  try {
    const value = parseDocument(yaml.value).toJS() as any
    const pipeline = value?.initial_run?.strategy?.parameters?.pipeline
    if (!pipeline) return {}
    const result: Record<string, string> = {}
    for (const [category, field] of Object.entries({ alpha: 'alpha', risk: 'risk', cost: 'cost', construction: 'construction', constraint: 'constraints' })) {
      const modelId = pipeline[field]?.model_id
      if (typeof modelId === 'string') result[category] = modelId
    }
    return result
  } catch { return {} }
})
type SchemaProperty = { type?: string; minimum?: number; maximum?: number; exclusiveMinimum?: number }
const componentFields = computed(() => {
  const result: Array<{ category: string; name: string; schema: SchemaProperty; value: unknown }> = []
  try {
    const value = parseDocument(yaml.value).toJS() as any
    const pipeline = value?.initial_run?.strategy?.parameters?.pipeline
    if (!pipeline) return result
    for (const [category, modelId] of Object.entries(pipelineModels.value)) {
      const descriptor = catalog.data.value?.component_schemas[category]?.find((item) => item.model_id === modelId)
      const properties = (descriptor?.params_schema?.properties ?? {}) as Record<string, SchemaProperty>
      const field = category === 'constraint' ? 'constraints' : category
      for (const name of Object.keys(properties).sort()) {
        result.push({ category, name, schema: properties[name], value: pipeline[field]?.params?.[name] })
      }
    }
  } catch { return result }
  return result
})
function useTemplate(id: string) { selected.value = id; yaml.value = templates[id]; invalidateValidation() }
function setModel(category: string, modelId: string) {
  const document = parseDocument(yaml.value)
  const field = category === 'constraint' ? 'constraints' : category
  document.setIn(['initial_run', 'strategy', 'parameters', 'pipeline', field, 'model_id'], modelId)
  document.setIn(['initial_run', 'strategy', 'parameters', 'pipeline', field, 'params'], {})
  yaml.value = document.toString({ lineWidth: 0 })
  invalidateValidation()
}
function setParameter(category: string, name: string, value: unknown) {
  const document = parseDocument(yaml.value)
  const field = category === 'constraint' ? 'constraints' : category
  const path = ['initial_run', 'strategy', 'parameters', 'pipeline', field, 'params', name]
  if (value == null) document.deleteIn(path)
  else document.setIn(path, value)
  yaml.value = document.toString({ lineWidth: 0 })
  invalidateValidation()
}
</script>

<template>
  <div class="page-stack">
    <section class="panel composer-header"><div><span class="eyebrow">STRICT YAML</span><h2>新建实验</h2><p>后端是规范化与校验的唯一来源；提交只使用最近一次校验通过且未改动的 YAML。</p></div><div class="toolbar"><RouterLink to="/experiments"><el-button>返回</el-button></RouterLink><el-button :loading="validate.isPending.value" @click="validate.mutate(yaml)">校验</el-button><el-button type="primary" :loading="submit.isPending.value" :disabled="!isCurrentYamlValidated" @click="submit.mutate()">提交</el-button></div></section>
    <section class="template-strip"><button v-for="(_, id) in templates" :key="id" type="button" :class="{active:selected===id}" @click="useTemplate(id)"><strong>{{ id }}</strong><small>{{ id === 'factor_study' ? 'FACTOR_STUDY' : 'STRATEGY_BACKTEST' }}</small></button></section>
    <ErrorState v-if="error" :error="error instanceof DashboardApiError ? error : new Error(String(error))" />
    <section class="composer-grid">
      <div class="panel"><div class="panel-heading"><div><h2>能力目录</h2><p>字段由后端 Schema 驱动，修改会同步回 YAML。</p></div></div><dl><template v-for="(values,key) in catalog.data.value?.components ?? {}" :key="key"><dt>{{ key }}</dt><dd><el-select v-if="pipelineModels[key]" :model-value="pipelineModels[key]" size="small" @change="(value:string)=>setModel(key,value)"><el-option v-for="value in values" :key="value" :label="value" :value="value" /></el-select><span v-else>{{ values.join(' · ') }}</span></dd></template></dl><div v-if="componentFields.length" class="schema-fields"><label v-for="field in componentFields" :key="`${field.category}.${field.name}`"><span>{{ field.category }}.{{ field.name }}</span><el-input-number v-if="['number','integer'].includes(field.schema.type ?? '')" :model-value="typeof field.value === 'number' ? field.value : undefined" :min="field.schema.minimum ?? field.schema.exclusiveMinimum" :max="field.schema.maximum" :step="field.schema.type === 'integer' ? 1 : 0.01" controls-position="right" @change="(value:number|undefined)=>setParameter(field.category,field.name,value)" /><el-input v-else :model-value="String(field.value ?? '')" @update:model-value="(value:string)=>setParameter(field.category,field.name,value)" /></label></div><details v-if="catalog.data.value?.component_schemas"><summary>参数 JSON Schema</summary><pre>{{ JSON.stringify(catalog.data.value.component_schemas, null, 2) }}</pre></details></div>
      <div class="panel"><div class="panel-heading"><div><h2>配置</h2><p>日期只接受 YYYY-MM-DD，不支持旧格式字段。</p></div><span class="hash">{{ isCurrentYamlValidated ? validate.data.value?.config_hash.slice(0,12) : 'UNVALIDATED' }}</span></div><el-input v-model="yaml" type="textarea" :rows="35" class="yaml-editor" @input="invalidateValidation" /></div>
      <aside class="panel"><div class="panel-heading"><div><h2>协议预览</h2><p>TRAIN / VALIDATION / TEST</p></div></div><pre v-if="validate.data.value">{{ JSON.stringify(validate.data.value.normalized.sample_windows, null, 2) }}</pre><div v-else class="empty-state">等待后端校验</div></aside>
    </section>
  </div>
</template>

<style scoped>
.composer-header{display:flex;justify-content:space-between;align-items:center}.composer-header h2{margin:8px 0}.template-strip{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.template-strip button{padding:14px;border:1px solid var(--border);border-radius:9px;background:#fff;display:flex;flex-direction:column;gap:5px}.template-strip button.active{border-color:var(--blue)}.template-strip small,dd{color:var(--dim)}.composer-grid{display:grid;grid-template-columns:290px 1fr 300px;gap:14px;align-items:start}.yaml-editor :deep(textarea),pre{font:11px/1.55 ui-monospace,Consolas,monospace}dt{margin-top:12px;font-weight:700}dd{margin:4px 0;overflow-wrap:anywhere}.schema-fields{display:grid;gap:9px;margin:16px 0;padding-top:14px;border-top:1px solid var(--border)}.schema-fields label{display:grid;gap:4px;color:var(--dim);font-size:11px}.schema-fields :deep(.el-input-number){width:100%}@media(max-width:1300px){.composer-grid{grid-template-columns:1fr}.template-strip{grid-template-columns:repeat(2,1fr)}}
</style>
