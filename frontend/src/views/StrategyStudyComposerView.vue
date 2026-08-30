<script setup lang="ts">
import { useMutation, useQuery } from '@tanstack/vue-query'
import { ElMessage } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { stringify } from 'yaml'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import type { StrategyCatalog, StrategyStudy, StrategyStudyValidation } from '../types'

const route = useRoute()
const router = useRouter()
const sourceId = computed(() => typeof route.query.from === 'string' ? route.query.from : '')
const defaultYaml = `name: 双均线趋势
description: 沪深300ETF 短长均线趋势
tags: [trend]
start_date: 2018-01-01
end_date: 2024-12-31
strategy:
  strategy_id: dual_ma_trend
  parameters: {instrument_id: 510300.SH, short_window: 20, long_window: 60, long_weight: 1.0, flat_weight: 0.0, target_tolerance: 0.001}
benchmark: 000300.SH
initial_cash_fen: 100000000
execution: {reference_price: OPEN, slippage_bps: 5.0, max_volume_participation: 0.1, limit_order_policy: REJECT}
`
const yaml = ref(defaultYaml)
const validatedYaml = ref('')
const catalog = useQuery({ queryKey: ['strategies'], queryFn: () => api.get<StrategyCatalog>('/api/v1/strategies') })
const source = useQuery({
  queryKey: computed(() => ['strategy-study-copy', sourceId.value]),
  queryFn: () => api.get<StrategyStudy>(`/api/v1/strategy-studies/${sourceId.value}`),
  enabled: computed(() => Boolean(sourceId.value)),
})
watch(() => source.data.value, (value) => {
  if (!value) return
  yaml.value = stringify({ ...value.definition, name: `${value.definition.name}（副本）` })
  invalidate()
}, { immediate: true })
const validate = useMutation({
  mutationFn: (candidate: string) => api.post<StrategyStudyValidation>('/api/v1/strategy-studies/validate', { yaml: candidate }),
  onSuccess: (_, candidate) => { validatedYaml.value = candidate; ElMessage.success('策略研究配置有效') },
})
const submit = useMutation({
  mutationFn: () => api.post<StrategyStudy>('/api/v1/strategy-studies', { yaml: validatedYaml.value }),
  onSuccess: async (value) => { ElMessage.success('策略研究已入队'); await router.push(`/strategy-studies/${value.id}`) },
})
const error = computed(() => validate.error.value ?? submit.error.value ?? source.error.value ?? catalog.error.value)
const currentValidated = computed(() => Boolean(validate.data.value) && validatedYaml.value === yaml.value)
function invalidate() { validatedYaml.value = ''; validate.reset() }
</script>

<template>
  <div class="page-stack">
    <section class="panel composer-header"><div><span class="eyebrow">STRICT STRATEGY STUDY YAML</span><h2>{{ sourceId ? '复制策略研究' : '新建策略研究' }}</h2><p>配置修改后必须重新校验；每次提交都会创建一次独立执行。</p></div><div class="toolbar"><RouterLink to="/strategy-studies"><el-button>返回</el-button></RouterLink><el-button :loading="validate.isPending.value" @click="validate.mutate(yaml)">校验</el-button><el-button type="primary" :disabled="!currentValidated" :loading="submit.isPending.value" @click="submit.mutate()">提交</el-button></div></section>
    <ErrorState v-if="error" :error="error instanceof DashboardApiError ? error : new Error(String(error))" />
    <section class="composer-grid"><aside class="panel"><div class="panel-heading"><div><h2>策略目录</h2><p>由后端注册表提供</p></div></div><ul><li v-for="item in catalog.data.value?.strategies ?? []" :key="item">{{ item }}</li></ul><details><summary>组合能力</summary><pre>{{ JSON.stringify(catalog.data.value?.components ?? {}, null, 2) }}</pre></details></aside><section class="panel"><div class="panel-heading"><div><h2>研究配置</h2><p>单一区间、单次执行。</p></div><span class="hash">{{ currentValidated ? validate.data.value?.config_hash.slice(0,12) : 'UNVALIDATED' }}</span></div><el-input v-model="yaml" class="yaml-editor" type="textarea" :rows="32" @input="invalidate" /></section><aside class="panel"><div class="panel-heading"><div><h2>配置预览</h2><p>以后端规范化结果为准</p></div></div><pre v-if="validate.data.value">{{ JSON.stringify(validate.data.value.normalized, null, 2) }}</pre><div v-else class="empty-state">等待后端校验</div></aside></section>
  </div>
</template>

<style scoped>
.composer-header{display:flex;justify-content:space-between;align-items:center}.composer-header h2{margin:8px 0}.composer-header p{margin:0;color:var(--muted)}.composer-grid{display:grid;grid-template-columns:250px 1fr 310px;gap:14px;align-items:start}.yaml-editor :deep(textarea),pre{font:11px/1.55 ui-monospace,Consolas,monospace}li{margin:8px 0;color:var(--muted);font-size:12px}@media(max-width:1300px){.composer-grid{grid-template-columns:1fr}}
</style>
