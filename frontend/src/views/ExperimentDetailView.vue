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
import type { ExperimentAggregate, ExperimentComparison, ExperimentRun, ResearchMark, RunArtifactRow, RunQualityDisclosure } from '../types'

type ArtifactPayload = { items?: RunArtifactRow[]; page?: number; page_size?: number; total?: number; value?: unknown }
type DisplayMetric = { name: string; label: string; value: number | null; unit: string | null; detail: string }

const STRATEGY_CORE = [
  ['cumulative_return', '累计收益'], ['annualized_return', '年化收益'], ['geometric_excess_return', '几何超额'], ['max_drawdown', '最大回撤'],
  ['sharpe_ratio', 'Sharpe'], ['calmar_ratio', 'Calmar'], ['annualized_turnover', '年化换手'], ['annualized_cost_drag', '年化成本拖累'],
] as const
const STRATEGY_GROUPS = [
  { title: '绩效与基准', metrics: [['benchmark_cumulative_return', '基准累计收益'], ['relative_cumulative_return', '相对累计收益'], ['benchmark_annualized_return', '基准年化收益'], ['information_ratio', '信息比率']] },
  { title: '风险与回撤', metrics: [['annualized_volatility', '年化波动率'], ['sortino_ratio', 'Sortino'], ['tracking_error', '跟踪误差'], ['beta', 'Beta'], ['jensen_alpha', 'Jensen Alpha'], ['active_max_drawdown', '主动最大回撤'], ['max_drawdown_duration_sessions', '最长水下期'], ['time_under_water_rate', '水下时间占比']] },
  { title: '执行质量', metrics: [['one_way_turnover', '单边换手'], ['fee_rate', '成本率'], ['failed_fill_rate', '失败成交率'], ['notional_fill_rate', '名义成交率'], ['priced_order_coverage_rate', '可定价订单覆盖率'], ['cumulative_cost_drag', '累计成本拖累']] },
  { title: '仓位与样本', metrics: [['average_cash_weight', '平均现金权重'], ['max_position_weight', '最大单票权重'], ['observations', '有效观察数']] },
] as const
const LARGE_ARTIFACTS = new Set(['orders', 'fills', 'holdings', 'costs'])
const FACTOR_FILTERED = new Set(['ic', 'quantile_returns', 'long_short_returns'])
const TERMINAL_RUN_STATUSES = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED'])

const route = useRoute()
const router = useRouter()
const queryClient = useQueryClient()
const experimentId = computed(() => String(route.params.experimentId))
const detail = useQuery({ queryKey: ['experiment', experimentId], queryFn: () => api.get<ExperimentAggregate>(`/api/v1/experiments/${experimentId.value}`), refetchInterval: 3000 })
const selectedRunId = ref('')
const tab = ref('protocol')
const runYaml = ref('')
const compareIds = ref<string[]>([])
const executionArtifact = ref('execution_summary')
const performanceArtifact = ref('performance')
const attributionArtifact = ref('attribution')
const factorArtifact = ref('coverage')
const artifactPage = ref(1)
const showAllConfigDiffs = ref(false)
const selectedVariant = ref(String(route.query.signal_variant ?? ''))
const selectedFactor = ref(String(route.query.factor ?? ''))
const selectedHorizon = ref(Number(route.query.horizon ?? 0))

const selectedRun = computed<ExperimentRun | undefined>(() => detail.data.value?.runs.find((item) => item.id === selectedRunId.value) ?? detail.data.value?.runs.at(-1))
const baselineRun = computed(() => detail.data.value?.runs.find((item) => item.id === detail.data.value?.experiment.baseline_run_id))
const hasActiveRuns = computed(() => detail.data.value?.runs.some((item) => !TERMINAL_RUN_STATUSES.has(item.status)) ?? false)
const isFactorStudy = computed(() => detail.data.value?.experiment.definition.kind === 'FACTOR_STUDY')
const selectedRunShortId = computed(() => selectedRun.value?.id.slice(0, 12) ?? '—')
const testUses = computed(() => detail.data.value?.runs.filter((item) => item.uses_test_region).length ?? 0)
const testBudget = computed(() => detail.data.value?.experiment.definition.governance.test_budget ?? 0)
const testRemaining = computed(() => Math.max(0, testBudget.value - testUses.value))
const runDuration = computed(() => {
  const start = selectedRun.value?.started_at
  const end = selectedRun.value?.completed_at
  if (!start || !end) return '—'
  const seconds = Math.max(0, Math.round((Date.parse(end) - Date.parse(start)) / 1000))
  return seconds < 60 ? `${seconds} 秒` : `${Math.floor(seconds / 60)}分 ${seconds % 60}秒`
})
const sampleRegion = computed(() => {
  if (!selectedRun.value) return '—'
  if (selectedRun.value.uses_test_region) return 'TEST'
  const config = selectedRun.value.config as { end_date?: string }
  const validation = detail.data.value?.experiment.definition.sample_windows.validation
  return config.end_date && validation && config.end_date >= validation.start ? 'VALIDATION' : 'TRAIN'
})

const artifactType = computed(() => isFactorStudy.value
  ? ({ signals: 'summary', portfolio: factorArtifact.value, execution: 'correlation', performance: 'ic' } as Record<string, string>)[tab.value]
  : ({ signals: 'signals', portfolio: 'holdings', execution: executionArtifact.value, performance: performanceArtifact.value, attribution: attributionArtifact.value } as Record<string, string>)[tab.value])
const isLargeArtifact = computed(() => LARGE_ARTIFACTS.has(artifactType.value))

function artifactPath(type: string, page: number, pageSize: number) {
  const params = new URLSearchParams({ page: String(page), page_size: String(pageSize) })
  if (isFactorStudy.value) {
    if (selectedVariant.value && ['summary', 'coverage', 'ic', 'quantile_returns', 'long_short_returns', 'correlation'].includes(type)) params.set('signal_variant', selectedVariant.value)
    if (selectedFactor.value && ['coverage', ...FACTOR_FILTERED].includes(type)) params.set('factor_ref', selectedFactor.value)
    if (selectedHorizon.value > 0 && FACTOR_FILTERED.has(type)) params.set('horizon', String(selectedHorizon.value))
  }
  return `/api/v1/runs/${selectedRun.value?.id}/artifacts/${type}?${params.toString()}`
}
async function loadArtifact(): Promise<ArtifactPayload> {
  const type = artifactType.value
  const pageSize = isLargeArtifact.value ? 100 : 1000
  const first = await api.get<ArtifactPayload>(artifactPath(type, isLargeArtifact.value ? artifactPage.value : 1, pageSize))
  const total = first.total ?? first.items?.length ?? 0
  if (isLargeArtifact.value || total <= pageSize) return first
  const rest = await Promise.all(Array.from({ length: Math.ceil(total / pageSize) - 1 }, (_, index) => api.get<ArtifactPayload>(artifactPath(type, index + 2, pageSize))))
  return { ...first, items: [first, ...rest].flatMap((item) => item.items ?? []), total }
}
const artifact = useQuery({
  queryKey: ['run-artifact', selectedRunId, artifactType, artifactPage, selectedVariant, selectedFactor, selectedHorizon],
  queryFn: loadArtifact,
  enabled: computed(() => Boolean(selectedRun.value?.manifest_hash && artifactType.value)),
})
const quality = useQuery({
  queryKey: ['run-quality', selectedRunId],
  queryFn: () => api.get<{ value: RunQualityDisclosure }>(`/api/v1/runs/${selectedRun.value?.id}/artifacts/quality_disclosure?page=1&page_size=1`),
  enabled: computed(() => Boolean(!isFactorStudy.value && selectedRun.value?.manifest_hash)),
})
const qualityValue = computed(() => quality.data.value?.value)
const artifactRows = computed(() => artifact.data.value?.items ?? [])
const artifactColumns = computed(() => Object.keys(artifactRows.value[0] ?? {}).slice(0, 14))
const artifactTotal = computed(() => artifact.data.value?.total ?? artifactRows.value.length)
const metricMap = computed(() => new Map((selectedRun.value?.metrics ?? []).map((item) => [item.name, item])))

function displayMetric(name: string, label: string, detail = 'Run 指标'): DisplayMetric {
  const metric = metricMap.value.get(name)
  return { name, label, value: metric?.value ?? null, unit: metric?.unit ?? null, detail: qualityValue.value?.undefined_metrics[name] ?? detail }
}
const factorCoreMetrics = computed<DisplayMetric[]>(() => {
  const hypotheses = (selectedRun.value?.metrics ?? []).filter((item) => item.name.startsWith('rank_ic_mean/'))
  const config = selectedRun.value?.config as { factor_study?: { factor_ids?: unknown[]; horizons?: unknown[] } } | undefined
  const derived: Array<[string, string, number | null, string]> = [
    ['significant', '显著假设', hypotheses.filter((item) => item.adjusted_p_value != null && item.adjusted_p_value <= 0.05).length, 'count'],
    ['tested', '检验总数', hypotheses.length, 'count'], ['factors', '因子数', config?.factor_study?.factor_ids?.length ?? null, 'count'], ['horizons', '期限数', config?.factor_study?.horizons?.length ?? null, 'count'],
  ]
  return [displayMetric('mean_coverage', '平均覆盖率'), displayMetric('rank_ic_mean', 'Rank IC 均值'), displayMetric('pearson_ic_mean', 'Pearson IC 均值'), displayMetric('long_short_mean', '多空收益均值'), ...derived.map(([name, label, value, unit]) => ({ name, label, value, unit, detail: '因子 Run 汇总' }))]
})
const coreMetrics = computed(() => isFactorStudy.value ? factorCoreMetrics.value : STRATEGY_CORE.map(([name, label]) => displayMetric(name, label)))
const groupedMetrics = computed(() => STRATEGY_GROUPS.map((group) => ({ title: group.title, metrics: group.metrics.map(([name, label]) => displayMetric(name, label, '未登记')) })))

const effectiveCompareIds = computed(() => {
  if (compareIds.value.length >= 2) return compareIds.value
  const current = selectedRun.value?.id
  const baseline = baselineRun.value?.id
  return current && baseline && current !== baseline ? [baseline, current] : []
})
const comparison = useQuery({
  queryKey: ['experiment-comparison', effectiveCompareIds],
  queryFn: () => api.post<ExperimentComparison>('/api/v1/experiments/compare', { run_ids: effectiveCompareIds.value }),
  enabled: computed(() => tab.value === 'compare' && effectiveCompareIds.value.length >= 2),
})
const changedConfigs = computed(() => comparison.data.value?.configs.filter((item) => item.differs) ?? [])
const displayedConfigs = computed(() => showAllConfigDiffs.value ? comparison.data.value?.configs ?? [] : changedConfigs.value)

const formatCell = (value: unknown) => value == null ? '—' : typeof value === 'number' ? (Number.isInteger(value) ? String(value) : value.toFixed(6)) : typeof value === 'object' ? JSON.stringify(value) : String(value)
const formatArtifactCell = (column: string, value: unknown) => column.includes('p_value') && typeof value === 'number' ? value.toExponential(3) : formatCell(value)
const formatMetric = (metric: { value: number | null; unit: string | null }) => {
  if (metric.value == null) return '—'
  if (metric.unit === 'ratio') return `${(metric.value * 100).toFixed(2)}%`
  if (metric.unit === 'count' || metric.unit === 'sessions') return `${Math.round(metric.value)}${metric.unit === 'sessions' ? ' 日' : ''}`
  return metric.value.toFixed(3)
}

const chartOption = computed(() => {
  const rows = artifactRows.value
  if (!rows.length) return null
  if (!isFactorStudy.value && tab.value === 'performance' && artifactType.value === 'performance') return {
    tooltip, legend: { data: ['组合净值', '基准净值', '回撤'] }, grid: { left: 55, right: 24, top: 40, bottom: 42 }, xAxis: { type: 'category', data: rows.map((row) => String(row.trade_date)), ...axis }, yAxis: { type: 'value', ...axis }, dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18 }],
    series: [{ name: '组合净值', type: 'line', symbol: 'none', data: rows.map((row) => row.nav) }, { name: '基准净值', type: 'line', symbol: 'none', data: rows.map((row) => row.benchmark_nav) }, { name: '回撤', type: 'line', symbol: 'none', data: rows.map((row) => row.drawdown), areaStyle: { opacity: 0.08 } }],
  }
  if (!isFactorStudy.value && tab.value === 'performance' && ['monthly_returns', 'annual_returns'].includes(artifactType.value)) return {
    tooltip, legend: { data: ['组合收益', '基准收益', '相对收益'] }, grid: { left: 55, right: 24, top: 40, bottom: 52 }, xAxis: { type: 'category', data: rows.map((row) => artifactType.value === 'monthly_returns' ? `${row.year}-${String(row.month).padStart(2, '0')}` : String(row.year)), ...axis }, yAxis: { type: 'value', ...axis }, dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18 }],
    series: [{ name: '组合收益', type: 'bar', data: rows.map((row) => row.portfolio_return) }, { name: '基准收益', type: 'bar', data: rows.map((row) => row.benchmark_return) }, { name: '相对收益', type: 'line', data: rows.map((row) => row.relative_return) }],
  }
  if (!isFactorStudy.value && tab.value === 'execution') {
    const counts = new Map<string, number>()
    for (const row of rows) { const reason = String(row.reason_code ?? row.side ?? 'UNKNOWN'); counts.set(reason, (counts.get(reason) ?? 0) + Number(row.order_count ?? 1)) }
    return { tooltip, grid: { left: 55, right: 24, top: 24, bottom: 70 }, xAxis: { type: 'category', data: [...counts.keys()], ...axis, axisLabel: { rotate: 25 } }, yAxis: { type: 'value', ...axis }, series: [{ type: 'bar', data: [...counts.values()] }] }
  }
  if (isFactorStudy.value && tab.value === 'signals') return { tooltip, legend: { data: ['Rank IC', 'ICIR', '多空收益'] }, grid: { left: 55, right: 24, top: 40, bottom: 80 }, xAxis: { type: 'category', data: rows.map((row) => `${row.factor_ref}/${row.horizon}`), ...axis, axisLabel: { rotate: 30 } }, yAxis: { type: 'value', ...axis }, series: [{ name: 'Rank IC', type: 'bar', data: rows.map((row) => row.rank_ic_mean) }, { name: 'ICIR', type: 'bar', data: rows.map((row) => row.rank_icir_unannualized) }, { name: '多空收益', type: 'bar', data: rows.map((row) => row.long_short_mean) }] }
  if (isFactorStudy.value && tab.value === 'portfolio') {
    const field = factorArtifact.value === 'coverage' ? 'coverage' : factorArtifact.value === 'long_short_returns' ? 'long_short_return' : 'mean_return'
    return { tooltip, grid: { left: 55, right: 24, top: 24, bottom: 42 }, xAxis: { type: 'category', data: rows.map((row) => String(row.signal_date)), ...axis }, yAxis: { type: 'value', ...axis }, dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18 }], series: [{ type: 'line', symbol: 'none', data: rows.map((row) => row[field]) }] }
  }
  if (isFactorStudy.value && tab.value === 'performance') return { tooltip, legend: { data: ['Rank IC', '滚动 Rank IC', 'Pearson IC'] }, grid: { left: 55, right: 24, top: 40, bottom: 42 }, xAxis: { type: 'category', data: rows.map((row) => String(row.signal_date)), ...axis }, yAxis: { type: 'value', ...axis }, dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18 }], series: [{ name: 'Rank IC', type: 'line', symbol: 'none', data: rows.map((row) => row.rank_ic) }, { name: '滚动 Rank IC', type: 'line', symbol: 'none', data: rows.map((row) => row.rank_ic_rolling_mean) }, { name: 'Pearson IC', type: 'line', symbol: 'none', data: rows.map((row) => row.pearson_ic) }] }
  if (isFactorStudy.value && tab.value === 'execution') {
    const factors = [...new Set(rows.flatMap((row) => [String(row.factor_x), String(row.factor_y)]))].sort()
    return { tooltip, grid: { left: 100, right: 30, top: 20, bottom: 80 }, xAxis: { type: 'category', data: factors, ...axis }, yAxis: { type: 'category', data: factors, ...axis }, visualMap: { min: -1, max: 1, calculable: true, orient: 'horizontal', left: 'center', bottom: 0 }, series: [{ type: 'heatmap', data: rows.map((row) => [factors.indexOf(String(row.factor_x)), factors.indexOf(String(row.factor_y)), row.correlation]) }] }
  }
  return null
})

const refresh = async () => { await queryClient.invalidateQueries({ queryKey: ['experiment', experimentId] }) }
const rerun = useMutation({ mutationFn: (id: string) => api.post(`/api/v1/runs/${id}/rerun`), onSuccess: async () => { ElMessage.success('已创建新 Run'); await refresh() } })
const cancel = useMutation({ mutationFn: (taskId: string) => api.post(`/api/v1/tasks/${taskId}/cancel`), onSuccess: async () => { ElMessage.success('已请求取消'); await refresh() } })
const mark = useMutation({ mutationFn: ({ id, value }: { id: string; value: ResearchMark }) => api.patch(`/api/v1/runs/${id}/research`, { mark: value }), onSuccess: refresh })
const addRun = useMutation({ mutationFn: () => api.post(`/api/v1/experiments/${experimentId.value}/runs`, { yaml: runYaml.value }), onSuccess: async () => { ElMessage.success('派生 Run 已入队'); runYaml.value = ''; await refresh() } })
const removeRun = useMutation({
  mutationFn: (id: string) => api.delete<{ experiment_id: string; run_id: string; status: 'DELETED' }>(`/api/v1/runs/${id}`),
  onSuccess: async (result) => {
    selectedRunId.value = ''
    compareIds.value = compareIds.value.filter((id) => id !== result.run_id)
    const query = { ...route.query }
    delete query.run
    await router.replace({ query })
    ElMessage.success(`已删除 Run ${result.run_id.slice(0, 12)}`)
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ['experiment', experimentId] }),
      queryClient.invalidateQueries({ queryKey: ['experiments'] }),
    ])
  },
})
const removeExperiment = useMutation({
  mutationFn: () => api.delete<{ experiment_id: string; run_count: number; status: 'DELETED' }>(`/api/v1/experiments/${experimentId.value}`),
  onSuccess: async (result) => {
    queryClient.removeQueries({ queryKey: ['experiment', result.experiment_id] })
    await queryClient.invalidateQueries({ queryKey: ['experiments'] })
    ElMessage.success(`已删除实验及 ${result.run_count} 个 Run`)
    await router.push('/experiments')
  },
})

async function deleteRun(run: ExperimentRun) {
  try {
    await ElMessageBox.confirm(
      `将删除 Run ${run.id.slice(0, 12)} 及其研究产物。任务和审计历史仍会保留，此操作不可撤销。`,
      '确认删除 Run',
      { type: 'error', confirmButtonText: '删除', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  removeRun.mutate(run.id)
}

async function deleteExperiment() {
  const experiment = detail.data.value?.experiment
  if (!experiment) return
  try {
    await ElMessageBox.confirm(
      `将删除实验“${experiment.definition.name}”、全部 ${detail.data.value?.runs.length ?? 0} 个 Run 及其研究产物。任务和审计历史仍会保留，此操作不可撤销。`,
      '确认删除实验',
      { type: 'error', confirmButtonText: '删除', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  removeExperiment.mutate()
}

function selectForCompare(rows: ExperimentRun[]) { compareIds.value = rows.map((item) => item.id) }
function selectRun(run: ExperimentRun) { selectedRunId.value = run.id; if (route.query.run !== run.id) void router.replace({ query: { ...route.query, run: run.id } }) }
function runRowClassName({ row }: { row: ExperimentRun }) { return row.id === selectedRunId.value ? 'viewing-run' : '' }
function selectFactorRow(row: RunArtifactRow) {
  selectedVariant.value = String(row.signal_variant); selectedFactor.value = String(row.factor_ref); selectedHorizon.value = Number(row.horizon)
  void router.replace({ query: { ...route.query, signal_variant: selectedVariant.value, factor: selectedFactor.value, horizon: String(selectedHorizon.value) } }); tab.value = 'performance'
}
watch([() => detail.data.value?.runs, () => route.query.run], ([runs, rawQueryRun]) => {
  if (!runs?.length) return
  const queryRun = Array.isArray(rawQueryRun) ? rawQueryRun[0] : rawQueryRun
  const next = runs.find((item) => item.id === queryRun) ?? runs.find((item) => item.id === selectedRunId.value) ?? runs.at(-1)
  if (!next) return
  selectedRunId.value = next.id
  if (queryRun !== next.id) void router.replace({ query: { ...route.query, run: next.id } })
}, { immediate: true })
watch([artifactType, selectedRunId, selectedVariant, selectedFactor, selectedHorizon], () => { artifactPage.value = 1 })
watch(artifactRows, (rows) => { if (isFactorStudy.value && artifactType.value === 'summary' && rows.length && !selectedFactor.value) { selectedVariant.value = String(rows[0].signal_variant); selectedFactor.value = String(rows[0].factor_ref); selectedHorizon.value = Number(rows[0].horizon) } })
</script>

<template>
  <div class="page-stack">
    <ErrorState v-if="detail.error.value" :error="detail.error.value" />
    <template v-else-if="detail.data.value">
      <section class="panel detail-head"><div><span class="eyebrow">{{ detail.data.value.experiment.definition.kind }}</span><h2>{{ detail.data.value.experiment.definition.name }}</h2><p>{{ detail.data.value.experiment.definition.description }}</p></div><div class="head-status"><StatusBadge v-if="selectedRun" :status="selectedRun.status" /><strong>{{ detail.data.value.runs.length }}</strong><small> RUNS</small><el-button type="danger" plain :disabled="hasActiveRuns" :loading="removeExperiment.isPending.value" @click="deleteExperiment">删除实验</el-button></div></section>
      <section class="governance-grid">
        <article class="governance-card"><span>当前 Run</span><strong class="hash">{{ selectedRunShortId }}</strong><small>{{ selectedRun?.research_mark }}</small></article><article class="governance-card"><span>样本区域</span><strong>{{ sampleRegion }}</strong><small>{{ (selectedRun?.config as { start_date?: string })?.start_date ?? '—' }} 至 {{ (selectedRun?.config as { end_date?: string })?.end_date ?? '—' }}</small></article><article class="governance-card"><span>TEST 预算</span><strong>{{ testUses }} / {{ testBudget }}</strong><small>剩余 {{ testRemaining }} 次</small></article><article class="governance-card"><span>运行耗时</span><strong>{{ runDuration }}</strong><small>{{ selectedRun?.stage }}</small></article><article class="governance-card"><span>数据身份</span><strong class="hash identity">{{ selectedRun?.catalog_hash.slice(0, 16) ?? '—' }}</strong><small>catalog_hash</small></article>
      </section>
      <section class="panel">
        <div class="panel-heading"><div><h2>Run 时间线</h2><p>点击 Run 编号切换详情；勾选用于多 Run 比较。默认比较当前 Run 与 baseline。</p></div><el-button :disabled="compareIds.length < 2 && effectiveCompareIds.length < 2" @click="tab = 'compare'">比较 Run</el-button></div>
        <el-table :data="detail.data.value.runs" :row-class-name="runRowClassName" @selection-change="selectForCompare"><el-table-column type="selection" width="46" /><el-table-column label="Run" width="170"><template #default="scope"><el-button link type="primary" class="run-link" @click.stop="selectRun(scope.row)"><span class="hash">{{ scope.row.id.slice(0, 12) }}</span></el-button></template></el-table-column><el-table-column label="角色" width="150"><template #default="scope"><el-tag v-if="scope.row.id === selectedRun?.id" type="primary" effect="dark" size="small">当前</el-tag><el-tag v-if="scope.row.id === baselineRun?.id" type="success" size="small">Baseline</el-tag></template></el-table-column><el-table-column label="状态" width="115"><template #default="scope"><StatusBadge :status="scope.row.status" /></template></el-table-column><el-table-column prop="stage" label="阶段" width="150" /><el-table-column label="TEST" width="80"><template #default="scope">{{ scope.row.uses_test_region ? '是' : '否' }}</template></el-table-column><el-table-column prop="research_mark" label="标记" width="120" /><el-table-column label="操作" min-width="320"><template #default="scope"><el-button v-if="scope.row.task_id && ['QUEUED', 'RUNNING'].includes(scope.row.status)" size="small" type="danger" plain @click="cancel.mutate(scope.row.task_id)">取消</el-button><el-button size="small" @click="rerun.mutate(scope.row.id)">重跑</el-button><el-dropdown @command="(value: ResearchMark) => mark.mutate({ id: scope.row.id, value })"><el-button size="small">标记</el-button><template #dropdown><el-dropdown-menu><el-dropdown-item v-for="value in ['UNREVIEWED', 'BASELINE', 'CANDIDATE', 'DISCARDED']" :key="value" :command="value">{{ value }}</el-dropdown-item></el-dropdown-menu></template></el-dropdown><el-button v-if="TERMINAL_RUN_STATUSES.has(scope.row.status)" size="small" text type="danger" @click="deleteRun(scope.row)">删除</el-button></template></el-table-column></el-table>
      </section>
      <section v-if="selectedRun?.status === 'SUCCEEDED'" class="metrics-grid core-metrics"><article v-for="metric in coreMetrics" :key="metric.name" class="metric-card"><span class="metric-top">{{ metric.label }}<i /></span><strong class="metric-value metric-small">{{ formatMetric(metric) }}</strong><p v-if="metric.value == null">{{ metric.detail }}</p></article></section>
      <section v-else class="panel run-state-panel"><h3>{{ selectedRun?.status === 'FAILED' ? 'Run 执行失败' : selectedRun?.status === 'CANCELLED' ? 'Run 已取消' : 'Run 尚未发布指标' }}</h3><p>{{ selectedRun?.error ? JSON.stringify(selectedRun.error) : `当前阶段：${selectedRun?.stage ?? '—'}` }}</p></section>
      <section v-if="!isFactorStudy && selectedRun?.status === 'SUCCEEDED'" class="metric-groups"><article v-for="group in groupedMetrics" :key="group.title" class="panel metric-group"><h3>{{ group.title }}</h3><div v-for="metric in group.metrics" :key="metric.name" class="metric-row"><span>{{ metric.label }}</span><strong>{{ formatMetric(metric) }}</strong><small v-if="metric.value == null">{{ metric.detail }}</small></div></article></section>
      <section v-if="qualityValue?.warnings.length" class="panel quality-warning"><h3>质量披露</h3><el-tag v-for="warning in qualityValue.warnings" :key="warning" type="warning">{{ warning }}</el-tag></section>
      <section class="panel">
        <el-tabs v-model="tab"><el-tab-pane label="协议/配置" name="protocol" /><el-tab-pane :label="isFactorStudy ? '因子矩阵' : '信号'" name="signals" /><el-tab-pane :label="isFactorStudy ? '覆盖/分层' : '持仓'" name="portfolio" /><el-tab-pane :label="isFactorStudy ? '相关性' : '成本/执行'" name="execution" /><el-tab-pane :label="isFactorStudy ? 'IC 下钻' : '绩效时序'" name="performance" /><el-tab-pane v-if="!isFactorStudy" label="归因" name="attribution" /><el-tab-pane label="产物" name="artifacts" /><el-tab-pane label="Run 比较" name="compare" /><el-tab-pane label="新增 Run" name="new-run" /></el-tabs>
        <div v-if="tab === 'protocol'" class="config-grid"><article class="config-block"><h3>实验协议</h3><p>Experiment 级定义，由全部 Run 共享且不可变。</p><pre>{{ JSON.stringify(detail.data.value.experiment.definition, null, 2) }}</pre></article><article class="config-block"><h3>当前 Run 配置</h3><p><span class="hash">{{ selectedRun?.id }}</span> · {{ selectedRun?.status }}</p><pre>{{ JSON.stringify(selectedRun?.config, null, 2) }}</pre></article></div>
        <div v-else-if="tab === 'compare'" class="compare-stack"><div v-if="!effectiveCompareIds.length" class="empty-state">当前 Run 就是 baseline，或实验尚未标记 baseline；请勾选至少两个 Run。</div><ErrorState v-else-if="comparison.error.value" :error="comparison.error.value" /><div v-else-if="comparison.isLoading.value" class="empty-state">正在对齐配置与指标…</div><template v-else-if="comparison.data.value"><h3>指标差异</h3><el-table :data="comparison.data.value.metrics" max-height="520"><el-table-column prop="name" label="指标" min-width="210" /><el-table-column v-for="runItem in comparison.data.value.runs" :key="runItem.id" :label="runItem.id.slice(0, 10)" min-width="160"><template #default="scope"><strong>{{ formatMetric({ value: scope.row.values.find((item: { run_id: string }) => item.run_id === runItem.id)?.value ?? null, unit: scope.row.unit }) }}</strong><small class="delta">Δ {{ formatCell(scope.row.values.find((item: { run_id: string }) => item.run_id === runItem.id)?.delta_from_baseline) }}</small></template></el-table-column></el-table><div class="compare-heading"><h3>配置差异</h3><el-switch v-model="showAllConfigDiffs" active-text="显示全部" inactive-text="仅变化项" /></div><el-table :data="displayedConfigs" max-height="420"><el-table-column prop="path" label="配置路径" min-width="230" /><el-table-column v-for="runItem in comparison.data.value.runs" :key="runItem.id" :label="runItem.id.slice(0, 10)" min-width="180"><template #default="scope">{{ formatCell(scope.row.values.find((item: { run_id: string }) => item.run_id === runItem.id)?.value) }}</template></el-table-column></el-table></template></div>
        <div v-else-if="tab === 'new-run'" class="run-editor"><el-input v-model="runYaml" type="textarea" :rows="18" placeholder="粘贴严格 Run YAML" /><el-button type="primary" :loading="addRun.isPending.value" @click="addRun.mutate()">创建 Run</el-button></div>
        <div v-else-if="tab === 'artifacts'" class="artifact-list"><el-table :data="selectedRun?.artifacts ?? []" empty-text="等待 Run 成功发布"><el-table-column prop="artifact_type" label="类型" width="190" /><el-table-column prop="relative_path" label="相对路径" min-width="250" /><el-table-column prop="row_count" label="行数" width="100" /><el-table-column prop="byte_count" label="字节数" width="120" /><el-table-column label="SHA-256" min-width="240"><template #default="scope"><span class="hash">{{ scope.row.content_hash }}</span></template></el-table-column></el-table></div>
        <template v-else><div v-if="!isFactorStudy && tab === 'execution'" class="artifact-switch"><el-radio-group v-model="executionArtifact" size="small"><el-radio-button value="orders">订单</el-radio-button><el-radio-button value="fills">成交/拒绝</el-radio-button><el-radio-button value="costs">成本</el-radio-button><el-radio-button value="execution_summary">汇总</el-radio-button></el-radio-group></div><div v-if="!isFactorStudy && tab === 'performance'" class="artifact-switch"><el-radio-group v-model="performanceArtifact" size="small"><el-radio-button value="performance">净值/回撤</el-radio-button><el-radio-button value="monthly_returns">月度收益</el-radio-button><el-radio-button value="annual_returns">年度收益</el-radio-button></el-radio-group></div><div v-if="!isFactorStudy && tab === 'attribution'" class="artifact-switch"><el-radio-group v-model="attributionArtifact" size="small"><el-radio-button value="attribution">收益归因</el-radio-button><el-radio-button value="exposure_summary">仓位暴露</el-radio-button></el-radio-group></div><div v-if="isFactorStudy && tab === 'portfolio'" class="artifact-switch"><el-radio-group v-model="factorArtifact" size="small"><el-radio-button value="coverage">覆盖率</el-radio-button><el-radio-button value="quantile_returns">分层收益</el-radio-button><el-radio-button value="long_short_returns">多空收益</el-radio-button></el-radio-group></div><div v-if="isFactorStudy && selectedFactor && ['portfolio', 'performance'].includes(tab)" class="selection-note">当前下钻：<strong>{{ selectedVariant }} / {{ selectedFactor }} / {{ selectedHorizon }}D</strong></div><ErrorState v-if="artifact.error.value" :error="artifact.error.value" /><div v-else-if="artifact.isLoading.value" class="empty-state">正在读取并复核可信产物…</div><template v-else><ChartCard v-if="chartOption" :title="`${artifactType} 图表`" :subtitle="`可信 Manifest 读取 · 共 ${artifactTotal} 行`"><VChart class="artifact-chart" :option="chartOption" autoresize /></ChartCard><el-table :data="artifactRows" max-height="560" :empty-text="selectedRun?.manifest_hash ? '该产物没有记录' : '等待 Run 成功发布'" @row-click="(row: RunArtifactRow) => isFactorStudy && tab === 'signals' ? selectFactorRow(row) : undefined"><el-table-column v-for="column in artifactColumns" :key="column" :prop="column" :label="column" min-width="145" show-overflow-tooltip><template #default="scope">{{ formatArtifactCell(column, scope.row[column]) }}</template></el-table-column></el-table><el-pagination v-if="isLargeArtifact && artifactTotal > 100" v-model:current-page="artifactPage" :page-size="100" :total="artifactTotal" layout="prev, pager, next, total" class="artifact-pagination" /></template></template>
      </section>
    </template>
  </div>
</template>

<style scoped>
.detail-head{display:flex;justify-content:space-between;align-items:center}.detail-head h2{margin:8px 0}.detail-head p{color:var(--muted)}.head-status{display:flex;align-items:center;gap:10px}.head-status strong{font-size:34px}.head-status small{color:var(--dim)}.governance-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px}.governance-card{display:flex;min-height:94px;flex-direction:column;gap:8px;padding:14px 16px;border:1px solid var(--border);border-radius:10px;background:var(--surface)}.governance-card span,.governance-card small{color:var(--dim);font-size:12px}.governance-card strong{font-size:20px}.governance-card .identity{font-size:15px}.core-metrics{grid-template-columns:repeat(4,minmax(0,1fr))}.metric-small{font-size:20px;overflow-wrap:anywhere}.metric-groups{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.metric-group h3{margin-top:0}.metric-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;padding:9px 0;border-bottom:1px solid var(--border)}.metric-row small{grid-column:1/-1;color:var(--dim)}.quality-warning{display:flex;align-items:center;gap:8px}.quality-warning h3{margin:0 12px 0 0}pre{max-height:620px;overflow:auto;padding:14px;border-radius:8px;background:var(--surface-raised);font:11px/1.55 ui-monospace,Consolas,monospace}.run-editor{display:flex;flex-direction:column;gap:12px;align-items:flex-end}.artifact-switch{display:flex;justify-content:flex-end;margin-bottom:12px}.artifact-chart{height:360px}.artifact-list,.artifact-pagination{margin-top:12px}.artifact-pagination{justify-content:flex-end}.selection-note{margin:0 0 12px;color:var(--muted)}.config-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:16px}.config-block{min-width:0}.config-block h3{margin:8px 0}.config-block p{min-height:34px;color:var(--muted);font-size:12px}.run-link{padding:0}:deep(.el-table .viewing-run>td.el-table__cell){background:color-mix(in srgb,var(--el-color-primary) 10%,transparent)}.compare-stack{display:flex;flex-direction:column;gap:14px}.compare-stack h3{margin:10px 0 0}.compare-heading{display:flex;align-items:center;justify-content:space-between}.compare-heading h3{margin:10px 0}.delta{display:block;margin-top:4px;color:var(--dim)}
@media(max-width:1280px){.governance-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.core-metrics{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:980px){.config-grid,.metric-groups{grid-template-columns:1fr}.governance-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
</style>
