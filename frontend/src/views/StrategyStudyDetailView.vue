<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import VChart from 'vue-echarts'

import { api } from '../api'
import { axis, tooltip } from '../charts'
import ChartCard from '../components/ChartCard.vue'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatDuration, formatTime } from '../format'
import type { ArtifactRow, StrategyStudy } from '../types'

type ArtifactPayload = { items?: ArtifactRow[]; page?: number; page_size?: number; total?: number; value?: unknown }

const route = useRoute()
const router = useRouter()
const client = useQueryClient()
const studyId = computed(() => String(route.params.strategyStudyId))
const tab = ref('overview')
const artifactType = ref('performance')
const artifactPage = ref(1)
const detail = useQuery({
  queryKey: computed(() => ['strategy-study', studyId.value]),
  queryFn: () => api.get<StrategyStudy>(`/api/v1/strategy-studies/${studyId.value}`),
  refetchInterval: (query) => ['QUEUED', 'RUNNING'].includes((query.state.data as StrategyStudy | undefined)?.status ?? '') ? 3_000 : false,
})
const artifact = useQuery({
  queryKey: computed(() => ['strategy-study-artifact', studyId.value, artifactType.value, artifactPage.value]),
  queryFn: () => api.get<ArtifactPayload>(`/api/v1/strategy-studies/${studyId.value}/artifacts/${artifactType.value}?page=${artifactPage.value}&page_size=100`),
  enabled: computed(() => detail.data.value?.status === 'SUCCEEDED' && tab.value === 'artifacts'),
})
const performance = useQuery({
  queryKey: computed(() => ['strategy-study-performance', studyId.value]),
  queryFn: () => api.get<ArtifactPayload>(`/api/v1/strategy-studies/${studyId.value}/artifacts/performance?page=1&page_size=1000`),
  enabled: computed(() => detail.data.value?.status === 'SUCCEEDED'),
})
const cancel = useMutation({
  mutationFn: () => api.post(`/api/v1/tasks/${detail.data.value?.task_id}/cancel`),
  onSuccess: async () => { ElMessage.success('取消请求已记录'); await detail.refetch() },
})
const remove = useMutation({
  mutationFn: () => api.delete<{ strategy_study_id: string; status: 'DELETED' }>(`/api/v1/strategy-studies/${studyId.value}`),
  onSuccess: async () => {
    client.removeQueries({ queryKey: ['strategy-study', studyId.value] })
    await client.invalidateQueries({ queryKey: ['strategy-studies'] })
    ElMessage.success('策略研究已删除')
    await router.push('/strategy-studies')
  },
})
const terminal = computed(() => ['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(detail.data.value?.status ?? ''))
const duration = computed(() => formatDuration(detail.data.value?.started_at, detail.data.value?.completed_at))
const metricMap = computed(() => new Map((detail.data.value?.metrics ?? []).map((item) => [item.name, item])))
const headlineMetrics = computed(() => [
  ['cumulative_return', '累计收益'],
  ['annualized_return', '年化收益'],
  ['max_drawdown', '最大回撤'],
  ['sharpe_ratio', 'Sharpe'],
].map(([name, label]) => ({ label, metric: metricMap.value.get(name) })))
const chartOption = computed(() => {
  const rows = performance.data.value?.items ?? []
  return {
    tooltip,
    legend: { data: ['策略', '基准'] },
    grid: { left: 48, right: 18, top: 36, bottom: 38 },
    xAxis: { ...axis, type: 'category', data: rows.map((row) => row.trade_date) },
    yAxis: { ...axis, type: 'value' },
    series: [
      { name: '策略', type: 'line', showSymbol: false, data: rows.map((row) => row.cumulative_return) },
      { name: '基准', type: 'line', showSymbol: false, data: rows.map((row) => row.benchmark_cumulative_return) },
    ],
  }
})
const artifactRows = computed(() => artifact.data.value?.items ?? [])
const artifactColumns = computed(() => Object.keys(artifactRows.value[0] ?? {}))
watch(artifactType, () => { artifactPage.value = 1 })

function formatMetric(value: number | undefined, unit: string | null | undefined) {
  if (value === undefined) return '—'
  if (unit === 'ratio' || unit === 'percent') return `${(value * 100).toFixed(2)}%`
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 4 })
}
async function deleteStudy() {
  const study = detail.data.value
  if (!study) return
  try {
    await ElMessageBox.confirm(`将删除“${study.definition.name}”及其研究产物，此操作不可撤销。`, '确认删除策略研究', { type: 'error', confirmButtonText: '删除', cancelButtonText: '取消' })
  } catch {
    return
  }
  remove.mutate()
}
</script>

<template>
  <div class="page-stack">
    <ErrorState v-if="detail.error.value" :error="detail.error.value" />
    <template v-else-if="detail.data.value">
      <section class="panel detail-head"><div><span class="eyebrow">STRATEGY STUDY</span><h2>{{ detail.data.value.definition.name }}</h2><p>{{ detail.data.value.definition.description || '—' }}</p></div><div class="toolbar"><StatusBadge :status="detail.data.value.status" /><RouterLink :to="`/strategy-studies/new?from=${studyId}`"><el-button>复制研究</el-button></RouterLink><el-button v-if="['QUEUED', 'RUNNING'].includes(detail.data.value.status)" type="danger" plain :loading="cancel.isPending.value" @click="cancel.mutate()">取消</el-button><el-button v-if="terminal" type="danger" plain :loading="remove.isPending.value" @click="deleteStudy">删除</el-button></div></section>
      <section class="governance-grid"><article class="governance-card"><span>状态 / 阶段</span><strong>{{ detail.data.value.status }}</strong><small>{{ detail.data.value.stage }}</small></article><article class="governance-card"><span>回测区间</span><strong>{{ detail.data.value.definition.start_date }}</strong><small>至 {{ detail.data.value.definition.end_date }}</small></article><article class="governance-card"><span>策略</span><strong>{{ detail.data.value.definition.strategy.strategy_id }}</strong><small>{{ detail.data.value.definition.benchmark }}</small></article><article class="governance-card"><span>运行耗时</span><strong>{{ duration }}</strong><small>{{ formatTime(detail.data.value.completed_at ?? detail.data.value.created_at) }}</small></article><article class="governance-card"><span>数据身份</span><strong class="hash identity">{{ detail.data.value.catalog_hash.slice(0, 16) }}</strong><small>catalog_hash</small></article></section>
      <section v-if="detail.data.value.status === 'SUCCEEDED'" class="metrics-grid"><article v-for="item in headlineMetrics" :key="item.label" class="metric-card"><span class="metric-top">{{ item.label }}<i /></span><strong class="metric-value metric-small">{{ formatMetric(item.metric?.value, item.metric?.unit) }}</strong></article></section>
      <section v-else class="panel run-state-panel"><h3>{{ detail.data.value.status === 'FAILED' ? '策略研究执行失败' : detail.data.value.status === 'CANCELLED' ? '策略研究已取消' : '策略研究正在执行' }}</h3><p>{{ detail.data.value.error ? JSON.stringify(detail.data.value.error) : `当前阶段：${detail.data.value.stage}` }}</p></section>
      <ChartCard v-if="detail.data.value.status === 'SUCCEEDED'" title="策略与基准累计收益" subtitle="可信 performance 产物"><VChart class="study-chart" :option="chartOption" autoresize /></ChartCard>
      <section class="panel detail-tabs"><el-tabs v-model="tab"><el-tab-pane label="概览与配置" name="overview" /><el-tab-pane label="指标" name="metrics" /><el-tab-pane label="产物" name="artifacts" /></el-tabs><div v-if="tab === 'overview'" class="config-grid"><article class="config-block"><h3>冻结定义</h3><pre>{{ JSON.stringify(detail.data.value.definition, null, 2) }}</pre></article><article class="config-block"><h3>执行身份</h3><pre>{{ JSON.stringify({ id: detail.data.value.id, task_id: detail.data.value.task_id, config_hash: detail.data.value.config_hash, catalog_hash: detail.data.value.catalog_hash, manifest_hash: detail.data.value.manifest_hash }, null, 2) }}</pre></article></div><el-table v-else-if="tab === 'metrics'" :data="detail.data.value.metrics" empty-text="等待分析完成"><el-table-column prop="name" label="指标" min-width="240" /><el-table-column label="值" min-width="160"><template #default="scope">{{ formatMetric(scope.row.value, scope.row.unit) }}</template></el-table-column><el-table-column prop="unit" label="单位" width="130" /></el-table><div v-else class="artifact-stack"><div class="toolbar"><el-select v-model="artifactType" style="width:220px"><el-option v-for="item in ['performance','nav','monthly_returns','annual_returns','orders','fills','holdings','costs','attribution','quality_disclosure','manifest']" :key="item" :label="item" :value="item" /></el-select></div><ErrorState v-if="artifact.error.value" :error="artifact.error.value" /><el-table v-else :data="artifactRows" max-height="560" empty-text="该产物没有表格记录"><el-table-column v-for="column in artifactColumns" :key="column" :prop="column" :label="column" min-width="145" show-overflow-tooltip /></el-table><el-pagination v-if="(artifact.data.value?.total ?? 0) > 100" v-model:current-page="artifactPage" :page-size="100" :total="artifact.data.value?.total ?? 0" layout="prev, pager, next, total" /></div></section>
    </template>
  </div>
</template>

<style scoped>
.detail-head{display:flex;align-items:center;justify-content:space-between;gap:20px}.detail-head h2{margin:8px 0}.detail-head p{margin:0;color:var(--muted)}.study-chart{height:360px}.config-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.config-block{min-width:0;padding:14px;background:#f7f9fc;border-radius:8px}.config-block pre{overflow:auto;font:11px/1.55 ui-monospace,Consolas,monospace}.artifact-stack{display:flex;flex-direction:column;gap:14px}@media(max-width:1000px){.config-grid{grid-template-columns:1fr}}
</style>
