<script setup lang="ts">
import { useMutation, useQuery } from '@tanstack/vue-query'
import { ElMessage } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import type { ExperimentAggregate, ExperimentValidation, StrategyCatalog } from '../types'

const router = useRouter()
const selected = ref('dual_ma_trend')
const validatedYaml = ref('')
const templates: Record<string, string> = {
  dual_ma_trend: `name: 双均线趋势
description: 沪深300ETF 短长均线趋势
tags: [trend]
sample_windows:
  train: {start: 2018-01-01, end: 2020-12-31}
  validation: {start: 2021-01-01, end: 2022-12-31}
  test: {start: 2023-01-01, end: 2024-12-31}
governance: {test_budget: 1, correction: BONFERRONI}
initial_run:
  start_date: 2018-01-01
  end_date: 2022-12-31
  strategy:
    strategy_id: dual_ma_trend
    parameters: {instrument_id: 510300.SH, short_window: 20, long_window: 60, long_weight: 1.0, flat_weight: 0.0, target_tolerance: 0.001}
  benchmark: 000300.SH
  initial_cash_fen: 100000000
  execution: {reference_price: OPEN, slippage_bps: 5.0, max_volume_participation: 0.1, limit_order_policy: REJECT}
`,
  stock_multifactor: `name: 股票多因子
description: 价值与动量周频组合
tags: [equity, multifactor]
sample_windows:
  train: {start: 2018-01-01, end: 2020-12-31}
  validation: {start: 2021-01-01, end: 2022-12-31}
  test: {start: 2023-01-01, end: 2024-12-31}
governance: {test_budget: 1, correction: BH_FDR}
initial_run:
  start_date: 2018-01-01
  end_date: 2022-12-31
  strategy:
    strategy_id: stock_multifactor
    parameters:
      pipeline:
        frequency: WEEKLY
        target_tolerance: 0.001
        alpha: {model_id: multi_factor_composite, params: {factor_weights: {book_to_price_mrq: 0.5, momentum_120_20: 0.5}, min_valid_factors: 2}}
        risk: {model_id: none}
        cost: {model_id: fixed_bps}
        construction: {model_id: top_n_equal_weight, params: {top_n: 10}}
        constraints: {model_id: long_only, params: {min_positions: 5, max_positions: 10, max_position_weight: 0.1, max_turnover: 0.4, max_industry_weight: 0.3, min_adv_amount: 50000000.0, long_exposure: 1.0}}
  benchmark: 000300.SH
  initial_cash_fen: 100000000
  execution: {reference_price: OPEN, slippage_bps: 5.0, max_volume_participation: 0.1, limit_order_policy: REJECT}
`,
  etf_rotation: `name: ETF 轮动
description: 固定 ETF 池月频动量、趋势和波动率轮动
tags: [etf, rotation]
sample_windows:
  train: {start: 2018-01-01, end: 2020-12-31}
  validation: {start: 2021-01-01, end: 2022-12-31}
  test: {start: 2023-01-01, end: 2024-12-31}
governance: {test_budget: 1, correction: BONFERRONI}
initial_run:
  start_date: 2018-01-01
  end_date: 2022-12-31
  strategy:
    strategy_id: etf_rotation
    parameters:
      pipeline:
        frequency: MONTHLY
        target_tolerance: 0.001
        alpha: {model_id: multi_factor_composite, params: {etf_pool: [510300.SH, 510500.SH], momentum_windows: [20, 60, 120], momentum_weights: [0.2, 0.3, 0.5], trend_window: 60, trend_weight: 0.5, volatility_window: 20, volatility_weight: 0.5}}
        risk: {model_id: none}
        cost: {model_id: fixed_bps}
        construction: {model_id: top_n_equal_weight, params: {top_n: 1}}
        constraints: {model_id: long_only, params: {min_positions: 1, max_positions: 1, max_position_weight: 1.0, max_turnover: 1.0, max_industry_weight: 1.0, min_adv_amount: 0.0, long_exposure: 1.0}}
  benchmark: 000300.SH
  initial_cash_fen: 100000000
  execution: {reference_price: OPEN, slippage_bps: 5.0, max_volume_participation: 0.1, limit_order_policy: REJECT}
`,
}
const yaml = ref(templates[selected.value])
const catalog = useQuery({ queryKey: ['strategies'], queryFn: () => api.get<StrategyCatalog>('/api/v1/strategies') })
const validate = useMutation({ mutationFn: (candidate: string) => api.post<ExperimentValidation>('/api/v1/experiments/validate', { yaml: candidate }), onSuccess: (_, candidate) => { validatedYaml.value = candidate; ElMessage.success('策略实验配置有效') } })
const submit = useMutation({ mutationFn: () => api.post<ExperimentAggregate>('/api/v1/experiments', { yaml: validatedYaml.value }), onSuccess: async (value) => { ElMessage.success('策略实验和首个 Run 已入队'); await router.push(`/experiments/${value.experiment.id}`) } })
const error = computed(() => validate.error.value ?? submit.error.value ?? catalog.error.value)
const currentValidated = computed(() => Boolean(validate.data.value) && validatedYaml.value === yaml.value)
function invalidate() { validatedYaml.value = ''; validate.reset() }
function useTemplate(id: string) { selected.value = id; yaml.value = templates[id]; invalidate() }
</script>

<template>
  <div class="page-stack">
    <section class="panel composer-header"><div><span class="eyebrow">STRICT STRATEGY YAML</span><h2>新建策略实验</h2><p>实验中心仅接受策略回测配置；提交只使用最近一次校验通过且未改动的 YAML。</p></div><div class="toolbar"><RouterLink to="/experiments"><el-button>返回</el-button></RouterLink><el-button :loading="validate.isPending.value" @click="validate.mutate(yaml)">校验</el-button><el-button type="primary" :disabled="!currentValidated" :loading="submit.isPending.value" @click="submit.mutate()">提交</el-button></div></section>
    <section class="template-strip"><button v-for="(_, id) in templates" :key="id" type="button" :class="{ active: selected === id }" @click="useTemplate(id)"><strong>{{ id }}</strong><small>STRATEGY BACKTEST</small></button></section>
    <ErrorState v-if="error" :error="error instanceof DashboardApiError ? error : new Error(String(error))" />
    <section class="composer-grid"><aside class="panel"><div class="panel-heading"><div><h2>策略目录</h2><p>由后端注册表提供</p></div></div><ul><li v-for="item in catalog.data.value?.strategies ?? []" :key="item">{{ item }}</li></ul><details><summary>组合能力</summary><pre>{{ JSON.stringify(catalog.data.value?.components ?? {}, null, 2) }}</pre></details></aside><section class="panel"><div class="panel-heading"><div><h2>配置</h2><p>不再包含类型判别字段。</p></div><span class="hash">{{ currentValidated ? validate.data.value?.config_hash.slice(0,12) : 'UNVALIDATED' }}</span></div><el-input v-model="yaml" class="yaml-editor" type="textarea" :rows="36" @input="invalidate" /></section><aside class="panel"><div class="panel-heading"><div><h2>协议预览</h2><p>TRAIN / VALIDATION / TEST</p></div></div><pre v-if="validate.data.value">{{ JSON.stringify(validate.data.value.normalized.sample_windows, null, 2) }}</pre><div v-else class="empty-state">等待后端校验</div></aside></section>
  </div>
</template>

<style scoped>
.composer-header{display:flex;justify-content:space-between;align-items:center}.composer-header h2{margin:8px 0}.composer-header p{margin:0;color:var(--muted)}.template-strip{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.template-strip button{padding:14px;border:1px solid var(--border);border-radius:9px;background:#fff;display:flex;flex-direction:column;gap:5px}.template-strip button.active{border-color:var(--blue)}.template-strip small{color:var(--dim)}.composer-grid{display:grid;grid-template-columns:260px 1fr 280px;gap:14px;align-items:start}.yaml-editor :deep(textarea),pre{font:11px/1.55 ui-monospace,Consolas,monospace}li{margin:8px 0;color:var(--muted);font-size:12px}@media(max-width:1300px){.composer-grid{grid-template-columns:1fr}.template-strip{grid-template-columns:repeat(2,1fr)}}
</style>
