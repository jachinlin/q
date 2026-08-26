<script setup lang="ts">
import { useMutation, useQuery } from '@tanstack/vue-query'
import { ElMessage } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import { computed, reactive, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { parse, stringify } from 'yaml'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import type { FactorStudy, FactorStudyCatalog, FactorStudyDefinition, FactorStudyValidation } from '../types'

type FormState = Omit<FactorStudyDefinition, 'tags' | 'industry'> & {
  tags: string
  industry_enabled: boolean
  industry_policy: 'EXCLUDE' | 'UNCLASSIFIED'
}

const router = useRouter()
const mode = ref<'form' | 'yaml'>('form')
const syncing = ref(false)
const validatedYaml = ref('')
const form = reactive<FormState>({
  name: '价值动量因子研究', description: '候选因子诊断', tags: 'factor',
  start_date: '2018-01-01', end_date: '2022-12-31', correction: 'BH_FDR',
  factor_ids: ['book_to_price_mrq', 'momentum_120_20'], universe: { name: 'CN_STOCK_STANDARD' },
  horizons: [1, 5, 20], quantiles: 5, industry_enabled: true, industry_policy: 'EXCLUDE',
  cost_bps_scenarios: [5, 10, 20],
})

function definitionFromForm(): FactorStudyDefinition {
  return {
    name: form.name, description: form.description,
    tags: form.tags.split(',').map((item) => item.trim()).filter(Boolean).sort(),
    start_date: form.start_date, end_date: form.end_date, correction: form.correction,
    factor_ids: [...form.factor_ids], universe: { name: 'CN_STOCK_STANDARD' },
    horizons: [...form.horizons].sort((a, b) => a - b), quantiles: form.quantiles,
    industry: form.industry_enabled ? { taxonomy: 'SW2021', unclassified_policy: form.industry_policy } : null,
    cost_bps_scenarios: [...form.cost_bps_scenarios].sort((a, b) => a - b),
  }
}

const yaml = ref(stringify(definitionFromForm(), { lineWidth: 0 }))
function invalidateValidation() { validatedYaml.value = ''; validate.reset() }
watch(form, () => {
  if (syncing.value) return
  yaml.value = stringify(definitionFromForm(), { lineWidth: 0 })
  invalidateValidation()
}, { deep: true })

function syncFormFromYaml() {
  invalidateValidation()
  try {
    const value = parse(yaml.value) as Partial<FactorStudyDefinition>
    if (!value || typeof value !== 'object') return
    syncing.value = true
    if (typeof value.name === 'string') form.name = value.name
    if (typeof value.description === 'string') form.description = value.description
    if (Array.isArray(value.tags)) form.tags = value.tags.join(', ')
    if (typeof value.start_date === 'string') form.start_date = value.start_date
    if (typeof value.end_date === 'string') form.end_date = value.end_date
    if (value.correction === 'BONFERRONI' || value.correction === 'BH_FDR') form.correction = value.correction
    if (Array.isArray(value.factor_ids)) form.factor_ids = value.factor_ids.map(String)
    if (Array.isArray(value.horizons)) form.horizons = value.horizons.map(Number)
    if (typeof value.quantiles === 'number') form.quantiles = value.quantiles
    form.industry_enabled = value.industry != null
    if (value.industry?.unclassified_policy) form.industry_policy = value.industry.unclassified_policy
    if (Array.isArray(value.cost_bps_scenarios)) form.cost_bps_scenarios = value.cost_bps_scenarios.map(Number)
  } catch {
    // 编辑中的不完整 YAML 仍保留原文，错误由后端校验统一报告。
  } finally {
    syncing.value = false
  }
}

const catalog = useQuery({ queryKey: ['factor-study-catalog'], queryFn: () => api.get<FactorStudyCatalog>('/api/v1/factor-studies/catalog') })
const validate = useMutation({
  mutationFn: (candidate: string) => api.post<FactorStudyValidation>('/api/v1/factor-studies/validate', { yaml: candidate }),
  onSuccess: (_, candidate) => { validatedYaml.value = candidate; ElMessage.success('因子研究配置有效') },
})
const submit = useMutation({
  mutationFn: () => api.post<FactorStudy>('/api/v1/factor-studies', { yaml: validatedYaml.value }),
  onSuccess: async (value) => { ElMessage.success('因子研究已入队'); await router.push(`/factor-studies/${value.id}`) },
})
const currentValidated = computed(() => Boolean(validate.data.value) && validatedYaml.value === yaml.value)
const error = computed(() => validate.error.value ?? submit.error.value ?? catalog.error.value)
</script>

<template>
  <div class="page-stack">
    <section class="panel composer-header">
      <div><span class="eyebrow">IMMUTABLE FACTOR STUDY</span><h2>新建因子研究</h2><p>表单与 YAML 双向同步；任何修改都会使旧校验失效，提交只使用后端最近校验通过的原文。</p></div>
      <div class="toolbar"><RouterLink to="/factor-studies"><el-button>返回</el-button></RouterLink><el-button :loading="validate.isPending.value" @click="validate.mutate(yaml)">校验</el-button><el-button type="primary" :disabled="!currentValidated" :loading="submit.isPending.value" @click="submit.mutate()">提交</el-button></div>
    </section>
    <ErrorState v-if="error" :error="error instanceof DashboardApiError ? error : new Error(String(error))" />
    <section class="panel mode-panel">
      <div class="panel-heading"><div><h2>研究配置</h2><p>仅保留明确分析区间和多重检验校正，不含样本分段或测试预算。</p></div><div class="mode-switch" role="group" aria-label="配置模式"><el-radio-group v-model="mode" size="small"><el-radio-button value="form">表单</el-radio-button><el-radio-button value="yaml">YAML</el-radio-button></el-radio-group><span class="hash">{{ currentValidated ? validate.data.value?.config_hash.slice(0,12) : 'UNVALIDATED' }}</span></div></div>
      <div v-if="mode === 'form'" class="factor-form">
        <label class="wide"><span>研究名称</span><el-input v-model="form.name" /></label>
        <label class="wide"><span>描述</span><el-input v-model="form.description" /></label>
        <label><span>开始日期</span><el-input v-model="form.start_date" placeholder="YYYY-MM-DD" /></label>
        <label><span>结束日期</span><el-input v-model="form.end_date" placeholder="YYYY-MM-DD" /></label>
        <label><span>多重检验</span><el-select v-model="form.correction"><el-option v-for="item in catalog.data.value?.corrections ?? []" :key="item" :label="item" :value="item" /></el-select></label>
        <label><span>分位数</span><el-input-number v-model="form.quantiles" :min="2" :max="20" /></label>
        <label class="wide"><span>因子</span><el-select v-model="form.factor_ids" multiple filterable aria-label="因子选择"><el-option v-for="item in catalog.data.value?.factors ?? []" :key="item.factor_id" :label="item.factor_id" :value="item.factor_id" /></el-select></label>
        <label><span>期限（日）</span><el-select v-model="form.horizons" multiple allow-create filterable><el-option v-for="item in [1,5,10,20,60]" :key="item" :label="`${item}D`" :value="item" /></el-select></label>
        <label><span>成本情景（bps）</span><el-select v-model="form.cost_bps_scenarios" multiple allow-create filterable><el-option v-for="item in [0,5,10,20,30]" :key="item" :label="item" :value="item" /></el-select></label>
        <label><span>标签（逗号分隔）</span><el-input v-model="form.tags" /></label>
        <label><span>股票池</span><el-input model-value="CN_STOCK_STANDARD" disabled /></label>
        <label class="industry"><span>行业处理</span><el-switch v-model="form.industry_enabled" active-text="启用" /><el-select v-if="form.industry_enabled" v-model="form.industry_policy"><el-option v-for="item in catalog.data.value?.industry_policies ?? []" :key="item" :label="item" :value="item" /></el-select></label>
      </div>
      <el-input v-else v-model="yaml" type="textarea" :rows="32" class="yaml-editor" aria-label="因子研究 YAML" @input="syncFormFromYaml" />
    </section>
  </div>
</template>

<style scoped>
.composer-header{display:flex;align-items:center;justify-content:space-between;gap:24px}.composer-header h2{margin:8px 0}.composer-header p{margin:0;color:var(--muted)}.mode-panel{padding:22px}.mode-switch{display:flex;align-items:center;gap:14px}.factor-form{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:17px 20px;max-width:1040px}.factor-form label{display:grid;gap:7px;color:var(--muted);font-size:11px}.factor-form .wide{grid-column:1/-1}.factor-form :deep(.el-select),.factor-form :deep(.el-input-number){width:100%}.industry{grid-template-columns:auto auto 1fr;align-items:center}.yaml-editor :deep(textarea){font:12px/1.65 ui-monospace,Consolas,monospace}@media(max-width:1200px){.factor-form{grid-template-columns:1fr}.factor-form .wide{grid-column:auto}}
</style>
