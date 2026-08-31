<script setup lang="ts">
import { useMutation, useQuery } from '@tanstack/vue-query'
import { ElMessage } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import { computed, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { parse, stringify } from 'yaml'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import {
  createDefaultStrategyStudyForm,
  defaultConstruction,
  defaultCost,
  defaultRisk,
  defaultStrategy,
  definitionFromStrategyStudyForm,
  strategyStudyFormFromDefinition,
} from '../strategy-study-form'
import type {
  ConstructionForm,
  CostForm,
  RiskForm,
  StrategyForm,
  StrategyStudyFormState,
} from '../strategy-study-form'
import type { FactorStudyCatalog, StrategyCatalog, StrategyStudy, StrategyStudyValidation } from '../types'

const route = useRoute()
const router = useRouter()
const sourceId = computed(() => typeof route.query.from === 'string' ? route.query.from : '')
const mode = ref<'form' | 'yaml'>('form')
const syncing = ref(false)
const formError = ref('')
const validatedYaml = ref('')
const form = reactive<StrategyStudyFormState>(createDefaultStrategyStudyForm())
const yaml = ref(stringify(definitionFromStrategyStudyForm(form), { lineWidth: 0 }))
const supportedStrategies = new Set<StrategyForm['strategy_id']>(['dual_ma_trend', 'etf_rotation', 'stock_multifactor'])

const catalog = useQuery({ queryKey: ['strategies'], queryFn: () => api.get<StrategyCatalog>('/api/v1/strategies') })
const selectedStrategyProfile = computed(() => catalog.data.value?.strategies.find((item) => item.strategy_id === form.strategy.strategy_id))
const factorCatalog = useQuery({
  queryKey: ['factor-study-catalog'],
  queryFn: () => api.get<FactorStudyCatalog>('/api/v1/factor-studies/catalog'),
  enabled: computed(() => form.strategy.strategy_id === 'stock_multifactor'),
})
const source = useQuery({
  queryKey: computed(() => ['strategy-study-copy', sourceId.value]),
  queryFn: () => api.get<StrategyStudy>(`/api/v1/strategy-studies/${sourceId.value}`),
  enabled: computed(() => Boolean(sourceId.value)),
})
const validate = useMutation({
  mutationFn: (candidate: string) => api.post<StrategyStudyValidation>('/api/v1/strategy-studies/validate', { yaml: candidate }),
  onSuccess: (_, candidate) => { validatedYaml.value = candidate; ElMessage.success('策略研究配置有效') },
})
const submit = useMutation({
  mutationFn: () => api.post<StrategyStudy>('/api/v1/strategy-studies', { yaml: validatedYaml.value }),
  onSuccess: async (value) => { ElMessage.success('策略研究已入队'); await router.push(`/strategy-studies/${value.id}`) },
})

function replaceForm(value: StrategyStudyFormState) {
  syncing.value = true
  Object.assign(form, value)
  syncing.value = false
}

function invalidateValidation() {
  validatedYaml.value = ''
  validate.reset()
}

function serializeForm(): string {
  return stringify(definitionFromStrategyStudyForm(form), { lineWidth: 0 })
}

watch(form, () => {
  if (syncing.value) return
  invalidateValidation()
  try {
    yaml.value = serializeForm()
    formError.value = ''
  } catch (error) {
    formError.value = error instanceof Error ? error.message : String(error)
  }
}, { deep: true, flush: 'sync' })

watch(() => source.data.value, (value) => {
  if (!value) return
  const copied = { ...value.definition, name: `${value.definition.name}（副本）` }
  invalidateValidation()
  try {
    const next = strategyStudyFormFromDefinition(copied)
    replaceForm(next)
    yaml.value = stringify(definitionFromStrategyStudyForm(next), { lineWidth: 0 })
    mode.value = 'form'
    formError.value = ''
  } catch (error) {
    yaml.value = stringify(copied, { lineWidth: 0 })
    mode.value = 'yaml'
    formError.value = error instanceof Error ? error.message : String(error)
  }
}, { immediate: true })

function handleYamlInput() {
  invalidateValidation()
  formError.value = ''
  try {
    replaceForm(strategyStudyFormFromDefinition(parse(yaml.value)))
  } catch {
    // 编辑中的 YAML 可以暂时不完整，切回表单时再提示具体错误。
  }
}

function changeMode(value: string | number | boolean | undefined) {
  if (value !== 'form' && value !== 'yaml') return
  if (value === mode.value) return
  if (value === 'yaml') {
    try {
      yaml.value = serializeForm()
      formError.value = ''
      mode.value = 'yaml'
    } catch (error) {
      formError.value = error instanceof Error ? error.message : String(error)
      ElMessage.error(formError.value)
    }
    return
  }
  try {
    const next = strategyStudyFormFromDefinition(parse(yaml.value))
    replaceForm(next)
    yaml.value = stringify(definitionFromStrategyStudyForm(next), { lineWidth: 0 })
    formError.value = ''
    mode.value = 'form'
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error)
    formError.value = `无法切换到表单：${detail}`
    ElMessage.error(formError.value)
  }
}

const strategyId = computed<StrategyForm['strategy_id']>({
  get: () => form.strategy.strategy_id,
  set: (value) => {
    if (!supportedStrategies.has(value) || value === form.strategy.strategy_id) return
    form.strategy = defaultStrategy(value)
  },
})
const dual = computed(() => form.strategy.strategy_id === 'dual_ma_trend' ? form.strategy : null)
const etf = computed(() => form.strategy.strategy_id === 'etf_rotation' ? form.strategy : null)
const stock = computed(() => form.strategy.strategy_id === 'stock_multifactor' ? form.strategy : null)
const cross = computed(() => form.strategy.strategy_id === 'dual_ma_trend' ? null : form.strategy)

function changeRisk(value: RiskForm['model_id']) {
  if (cross.value) cross.value.pipeline.risk = defaultRisk(value)
}

function changeCost(value: CostForm['model_id']) {
  if (cross.value) cross.value.pipeline.cost = defaultCost(value)
}

function changeConstruction(value: ConstructionForm['model_id']) {
  if (!cross.value) return
  cross.value.pipeline.construction = defaultConstruction(value, cross.value.strategy_id === 'etf_rotation' ? 2 : 20)
  if (value === 'mean_variance' && cross.value.pipeline.risk.model_id === 'none') {
    cross.value.pipeline.risk = defaultRisk('sample_cov')
    ElMessage.info('均值方差构建需要风险模型，已切换为 sample_cov')
  }
}

function addMomentum() {
  etf.value?.alpha.momentum.push({ window: 20, weight: 1 })
}

function addFactor() {
  stock.value?.alpha.factor_weights.push({ factor_id: '', weight: 1 })
}

function validateCurrent() {
  try {
    const candidate = mode.value === 'form' ? serializeForm() : yaml.value
    if (mode.value === 'form') yaml.value = candidate
    formError.value = ''
    validate.mutate(candidate)
  } catch (error) {
    formError.value = error instanceof Error ? error.message : String(error)
    ElMessage.error(formError.value)
  }
}

const currentValidated = computed(() => Boolean(validate.data.value) && validatedYaml.value === yaml.value)
const error = computed(() => validate.error.value ?? submit.error.value ?? source.error.value ?? catalog.error.value ?? factorCatalog.error.value)
</script>

<template>
  <div class="page-stack">
    <section class="panel composer-header">
      <div><span class="eyebrow">IMMUTABLE STRATEGY STUDY</span><h2>{{ sourceId ? '复制策略研究' : '新建策略研究' }}</h2><p>表单与 YAML 双向同步；任何修改都会使旧校验失效，每次提交创建一次独立执行。</p></div>
      <div class="toolbar"><RouterLink to="/strategy-studies"><el-button>返回</el-button></RouterLink><el-button :loading="validate.isPending.value" @click="validateCurrent">校验</el-button><el-button type="primary" :disabled="!currentValidated" :loading="submit.isPending.value" @click="submit.mutate()">提交</el-button></div>
    </section>
    <ErrorState v-if="error" :error="error instanceof DashboardApiError ? error : new Error(String(error))" />
    <el-alert v-if="formError" class="client-error" type="warning" :closable="false" :title="formError" />

    <section class="panel mode-panel">
      <div class="panel-heading">
        <div><h2>研究配置</h2><p>单一区间、单一策略、单次执行。</p></div>
        <div class="mode-switch" role="group" aria-label="配置模式"><el-radio-group :model-value="mode" size="small" @change="changeMode"><el-radio-button value="form">表单</el-radio-button><el-radio-button value="yaml">YAML</el-radio-button></el-radio-group><span class="hash">{{ currentValidated ? validate.data.value?.config_hash.slice(0,12) : 'UNVALIDATED' }}</span></div>
      </div>

      <div v-if="mode === 'form'" class="study-form" data-mode="form">
        <section class="form-section">
          <h3>基本信息</h3>
          <div class="field-grid">
            <label class="wide"><span>研究名称</span><el-input v-model="form.name" data-field="name" /></label>
            <label class="wide"><span>描述</span><el-input v-model="form.description" type="textarea" :rows="2" /></label>
            <label><span>开始日期</span><el-date-picker v-model="form.start_date" type="date" value-format="YYYY-MM-DD" format="YYYY-MM-DD" aria-label="开始日期" /></label>
            <label><span>结束日期</span><el-date-picker v-model="form.end_date" type="date" value-format="YYYY-MM-DD" format="YYYY-MM-DD" aria-label="结束日期" /></label>
            <label class="wide"><span>标签</span><el-select v-model="form.tags" multiple filterable allow-create default-first-option aria-label="标签" /></label>
            <label><span>策略</span><el-select v-model="strategyId" data-field="strategy-id"><el-option v-for="item in catalog.data.value?.strategies ?? []" :key="item.strategy_id" :label="item.display_name" :value="item.strategy_id" :disabled="!supportedStrategies.has(item.strategy_id as StrategyForm['strategy_id'])" /></el-select></label>
            <label><span>基准</span><el-input v-model="form.benchmark" placeholder="000300.SH" /></label>
            <label><span>初始资金（分）</span><el-input-number v-model="form.initial_cash_fen" :min="1" :step="1000000" /></label>
          </div>
          <div v-if="selectedStrategyProfile" class="strategy-profile-summary" data-strategy-profile>
            <div><strong>{{ selectedStrategyProfile.display_name }}</strong><span>{{ selectedStrategyProfile.strategy_id }}</span></div>
            <p>{{ selectedStrategyProfile.summary }}</p>
            <RouterLink :to="`/strategies/${selectedStrategyProfile.strategy_id}`">查看完整策略说明</RouterLink>
          </div>
        </section>

        <section class="form-section">
          <h3>执行设置</h3>
          <div class="field-grid four">
            <label><span>参考价格</span><el-select v-model="form.execution.reference_price"><el-option label="OPEN" value="OPEN" /><el-option label="CLOSE" value="CLOSE" /></el-select></label>
            <label><span>滑点（bps）</span><el-input-number v-model="form.execution.slippage_bps" :min="0" :precision="2" /></label>
            <label><span>最大成交量参与率</span><el-input-number v-model="form.execution.max_volume_participation" :min="0.0001" :max="1" :step="0.01" :precision="4" /></label>
            <label><span>涨跌停订单</span><el-select v-model="form.execution.limit_order_policy"><el-option label="REJECT" value="REJECT" /><el-option label="PARTIAL" value="PARTIAL" /></el-select></label>
          </div>
        </section>

        <section v-if="dual" class="form-section" data-strategy-form="dual_ma_trend">
          <h3>双均线参数</h3>
          <div class="field-grid three">
            <label><span>标的</span><el-input v-model="dual.parameters.instrument_id" /></label>
            <label><span>短均线窗口</span><el-input-number v-model="dual.parameters.short_window" :min="1" /></label>
            <label><span>长均线窗口</span><el-input-number v-model="dual.parameters.long_window" :min="2" /></label>
            <label><span>多头权重</span><el-input-number v-model="dual.parameters.long_weight" :min="0.0001" :max="1" :step="0.1" /></label>
            <label><span>空仓权重</span><el-input-number v-model="dual.parameters.flat_weight" :min="0" :max="0" /></label>
            <label><span>目标差额容差</span><el-input-number v-model="dual.parameters.target_tolerance" :min="0" :max="0.1" :step="0.001" :precision="4" /></label>
          </div>
        </section>

        <template v-if="cross">
          <section class="form-section" :data-strategy-form="cross.strategy_id">
            <div class="section-heading"><h3>Alpha</h3><span class="fixed-value">频率：{{ cross.strategy_id === 'etf_rotation' ? 'MONTHLY' : 'WEEKLY' }}</span></div>
            <div class="field-grid">
              <label><span>Alpha 模型</span><el-select v-model="cross.alpha.model_id"><el-option label="single_factor" value="single_factor" /><el-option label="multi_factor_composite" value="multi_factor_composite" /></el-select></label>
              <label><span>目标差额容差</span><el-input-number v-model="cross.pipeline.target_tolerance" :min="0" :max="0.1" :step="0.001" :precision="4" /></label>
            </div>
            <template v-if="etf">
              <div class="field-grid">
                <label class="wide"><span>ETF 池</span><el-select v-model="etf.alpha.etf_pool" multiple filterable allow-create default-first-option aria-label="ETF 池" /></label>
                <template v-if="etf.alpha.model_id === 'single_factor'">
                  <label><span>回看窗口</span><el-input-number v-model="etf.alpha.lookback" :min="2" /></label>
                  <label><span>方向</span><el-input-number v-model="etf.alpha.direction" :step="0.1" /></label>
                </template>
                <template v-else>
                  <label><span>趋势窗口</span><el-input-number v-model="etf.alpha.trend_window" :min="2" /></label>
                  <label><span>趋势权重</span><el-input-number v-model="etf.alpha.trend_weight" :step="0.1" /></label>
                  <label><span>波动率窗口</span><el-input-number v-model="etf.alpha.volatility_window" :min="2" /></label>
                  <label><span>波动率权重</span><el-input-number v-model="etf.alpha.volatility_weight" :step="0.1" /></label>
                  <div class="dynamic-group wide"><div class="dynamic-title"><span>动量窗口与权重</span><el-button size="small" data-action="add-momentum" @click="addMomentum">添加</el-button></div><div v-for="(row, index) in etf.alpha.momentum" :key="index" class="dynamic-row"><el-input-number v-model="row.window" :min="2" aria-label="动量窗口" /><el-input-number v-model="row.weight" :step="0.1" aria-label="动量权重" /><el-button type="danger" plain size="small" :disabled="etf.alpha.momentum.length === 1" @click="etf.alpha.momentum.splice(index, 1)">删除</el-button></div></div>
                </template>
              </div>
            </template>
            <template v-if="stock">
              <div v-if="stock.alpha.model_id === 'single_factor'" class="field-grid">
                <label><span>因子</span><el-select v-model="stock.alpha.factor_id" filterable allow-create default-first-option><el-option v-for="item in factorCatalog.data.value?.factors ?? []" :key="item.factor_id" :label="item.factor_id" :value="item.factor_id" /></el-select></label>
                <label><span>方向</span><el-input-number v-model="stock.alpha.direction" :step="0.1" /></label>
              </div>
              <div v-else class="dynamic-group"><div class="dynamic-title"><span>因子权重</span><div><span class="inline-field">最少有效因子 <el-input-number v-model="stock.alpha.min_valid_factors" :min="1" :max="stock.alpha.factor_weights.length" size="small" /></span><el-button size="small" data-action="add-factor" @click="addFactor">添加</el-button></div></div><div v-for="(row, index) in stock.alpha.factor_weights" :key="index" class="dynamic-row"><el-select v-model="row.factor_id" filterable allow-create default-first-option aria-label="因子"><el-option v-for="item in factorCatalog.data.value?.factors ?? []" :key="item.factor_id" :label="item.factor_id" :value="item.factor_id" /></el-select><el-input-number v-model="row.weight" :step="0.1" aria-label="因子权重" /><el-button type="danger" plain size="small" :disabled="stock.alpha.factor_weights.length === 1" @click="stock.alpha.factor_weights.splice(index, 1)">删除</el-button></div></div>
            </template>
          </section>

          <section class="form-section">
            <h3>Risk / Cost / Construction</h3>
            <div class="module-grid">
              <div class="module-card"><label><span>Risk 模型</span><el-select data-field="risk-model" :model-value="cross.pipeline.risk.model_id" @change="changeRisk"><el-option label="none" value="none" :disabled="cross.pipeline.construction.model_id === 'mean_variance'" /><el-option label="sample_cov" value="sample_cov" /><el-option label="shrinkage" value="shrinkage" /></el-select></label><label v-if="cross.pipeline.risk.model_id !== 'none'"><span>回看窗口</span><el-input-number v-model="cross.pipeline.risk.lookback" :min="2" /></label><label v-if="cross.pipeline.risk.model_id === 'shrinkage'"><span>收缩强度</span><el-input-number v-model="cross.pipeline.risk.shrinkage" :min="0" :max="1" :step="0.1" /></label></div>
              <div class="module-card"><label><span>Cost 模型</span><el-select :model-value="cross.pipeline.cost.model_id" @change="changeCost"><el-option label="fixed_bps" value="fixed_bps" /><el-option label="linear_impact" value="linear_impact" /><el-option label="sqrt_impact" value="sqrt_impact" /></el-select></label><template v-if="cross.pipeline.cost.model_id !== 'fixed_bps'"><label><span>冲击成本（bps）</span><el-input-number v-model="cross.pipeline.cost.impact_bps" :min="0" /></label><label><span>最大参与率</span><el-input-number v-model="cross.pipeline.cost.max_participation" :min="0.0001" :max="1" :step="0.01" /></label></template></div>
              <div class="module-card"><label><span>Construction 模型</span><el-select data-field="construction-model" :model-value="cross.pipeline.construction.model_id" @change="changeConstruction"><el-option label="top_n_equal_weight" value="top_n_equal_weight" /><el-option label="mean_variance" value="mean_variance" /></el-select></label><label v-if="cross.pipeline.construction.model_id === 'top_n_equal_weight'"><span>持仓数量</span><el-input-number v-model="cross.pipeline.construction.top_n" :min="1" /></label><template v-else><label><span>风险厌恶</span><el-input-number v-model="cross.pipeline.construction.risk_aversion" :min="0" /></label><label><span>成本厌恶</span><el-input-number v-model="cross.pipeline.construction.cost_aversion" :min="0" /></label><label><span>迭代次数</span><el-input-number v-model="cross.pipeline.construction.iterations" :min="1" /></label><label><span>学习率</span><el-input-number v-model="cross.pipeline.construction.learning_rate" :min="0.0001" :step="0.01" /></label></template></div>
            </div>
          </section>

          <section class="form-section">
            <div class="section-heading"><h3>Long-only 约束</h3><span class="fixed-value">模型：long_only</span></div>
            <div class="field-grid four">
              <label><span>最少持仓数</span><el-input-number v-model="cross.pipeline.constraints.min_positions" :min="1" /></label>
              <label><span>最大持仓数</span><el-input-number v-model="cross.pipeline.constraints.max_positions" :min="1" /></label>
              <label><span>单标的权重上限</span><el-input-number v-model="cross.pipeline.constraints.max_position_weight" :min="0.0001" :max="1" :step="0.01" /></label>
              <label><span>换手上限</span><el-input-number v-model="cross.pipeline.constraints.max_turnover" :min="0" :max="1" :step="0.1" /></label>
              <label><span>行业权重上限</span><el-input-number v-model="cross.pipeline.constraints.max_industry_weight" :min="0" :max="1" :step="0.1" /></label>
              <label><span>最小 ADV</span><el-input-number v-model="cross.pipeline.constraints.min_adv_amount" :min="0" :step="1000000" /></label>
              <label><span>多头敞口</span><el-input-number v-model="cross.pipeline.constraints.long_exposure" :min="0.0001" :max="1" :step="0.1" /></label>
            </div>
          </section>
        </template>
      </div>

      <el-input v-else v-model="yaml" type="textarea" :rows="36" class="yaml-editor" aria-label="策略研究 YAML" @input="handleYamlInput" />
    </section>
  </div>
</template>

<style scoped>
.composer-header{display:flex;align-items:center;justify-content:space-between;gap:24px}.composer-header h2{margin:8px 0}.composer-header p{margin:0;color:var(--muted)}.mode-panel{padding:22px}.mode-switch,.section-heading,.dynamic-title{display:flex;align-items:center;justify-content:space-between;gap:14px}.client-error{margin:0}.study-form{display:grid;gap:18px}.form-section{border-top:1px solid var(--line);padding-top:18px}.form-section:first-child{border-top:0;padding-top:0}.form-section h3{margin:0 0 14px;font-size:14px}.field-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:15px 20px}.field-grid.three{grid-template-columns:repeat(3,minmax(0,1fr))}.field-grid.four{grid-template-columns:repeat(4,minmax(0,1fr))}.field-grid .wide{grid-column:1/-1}.study-form label,.module-card{display:grid;gap:7px;color:var(--muted);font-size:11px}.study-form :deep(.el-select),.study-form :deep(.el-input-number),.study-form :deep(.el-date-editor){width:100%}.strategy-profile-summary{display:grid;gap:8px;margin-top:16px;padding:15px;border:1px solid rgba(37,99,235,.2);border-radius:10px;background:#f7faff}.strategy-profile-summary>div{display:flex;align-items:baseline;gap:10px}.strategy-profile-summary strong{font-size:13px}.strategy-profile-summary span{color:var(--dim);font:10px ui-monospace,Consolas,monospace}.strategy-profile-summary p{margin:0;color:var(--muted);font-size:12px;line-height:1.65}.strategy-profile-summary a{width:max-content;color:var(--blue);font-size:11px;text-decoration:none}.module-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.module-card{align-content:start;padding:14px;border:1px solid var(--line);border-radius:10px}.dynamic-group{display:grid;gap:8px;margin-top:15px}.dynamic-row{display:grid;grid-template-columns:minmax(180px,1fr) minmax(140px,1fr) auto;gap:8px}.inline-field{display:inline-flex;align-items:center;gap:7px;margin-right:8px}.fixed-value{color:var(--muted);font-size:11px}.yaml-editor :deep(textarea){font:12px/1.65 ui-monospace,Consolas,monospace}@media(max-width:1200px){.field-grid,.field-grid.three,.field-grid.four,.module-grid{grid-template-columns:1fr}.field-grid .wide{grid-column:auto}.composer-header{align-items:flex-start;flex-direction:column}}
</style>
