<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed, reactive, ref, watch } from 'vue'
import VChart from 'vue-echarts'
import { useRoute, useRouter } from 'vue-router'

import '../charts'
import { api } from '../api'
import { axis, tooltip } from '../charts'
import ChartCard from '../components/ChartCard.vue'
import ErrorState from '../components/ErrorState.vue'
import FactorStudyTaskProgress from '../components/FactorStudyTaskProgress.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime } from '../format'
import type { FactorDecisionMark, FactorStudy, FactorStudyMatrixRow, RunRawArtifactRow, TaskDetail } from '../types'

type SummaryMetricValue = string | number | null
type SummaryMetricDefinition = { name: string; label: string }
type SummaryMetricGroupDefinition = { title: string; metrics: SummaryMetricDefinition[] }
type CurveArtifactType = 'ic' | 'quantile_returns' | 'long_short_returns' | 'monotonicity' | 'turnover' | 'cost_scenarios' | 'coverage' | 'label_quality' | 'industry_coverage' | 'correlation'
type CurveDefinition = { type: CurveArtifactType; title: string }

function icMetricDefinitions(prefix: 'pearson_ic' | 'rank_ic'): SummaryMetricDefinition[] {
  return [
    ['mean', '均值'],
    ['sample_std', '样本标准差'],
    ['ir_unannualized', '未年化 ICIR'],
    ['positive_rate', '正值比例'],
    ['p05', 'P05'],
    ['p25', 'P25'],
    ['p50', 'P50'],
    ['p75', 'P75'],
    ['p95', 'P95'],
    ['valid_date_count', '有效日期数'],
    ['max_positive_streak', '最长正向连续期'],
    ['positive_streak_start', '正向连续期开始'],
    ['positive_streak_end', '正向连续期结束'],
    ['max_negative_streak', '最长负向连续期'],
    ['negative_streak_start', '负向连续期开始'],
    ['negative_streak_end', '负向连续期结束'],
  ].map(([suffix, label]) => ({ name: suffix === 'ir_unannualized' ? `${prefix}ir_unannualized` : `${prefix}_${suffix}`, label }))
}

function hacMetricDefinitions(prefix: string, includeDoubleHacNames = false): SummaryMetricDefinition[] {
  const fields = [
    ['mean', '均值'],
    ['valid_count', '有效样本数'],
    ['lag', 'HAC 滞后阶数'],
    ['standard_error', 'HAC 标准误'],
    ['t_stat', 'HAC t 统计量'],
    ['p_value', 'HAC p 值'],
    ['ci_lower', '95% CI 下界'],
    ['ci_upper', '95% CI 上界'],
    ['invalid_reason', '无效原因'],
  ]
  const definitions = fields.map(([suffix, label]) => ({
    name: `${prefix}_${suffix === 'mean' || suffix === 'valid_count' || includeDoubleHacNames ? suffix : `hac_${suffix}`}`,
    label,
  }))
  if (!includeDoubleHacNames) return definitions
  return [
    ...definitions,
    ...fields.slice(2).map(([suffix, label]) => ({ name: `${prefix}_hac_${suffix}`, label })),
  ]
}

const SUMMARY_METRIC_GROUPS: SummaryMetricGroupDefinition[] = [
  { title: 'Pearson IC 描述统计', metrics: icMetricDefinitions('pearson_ic') },
  { title: 'Rank IC 描述统计', metrics: icMetricDefinitions('rank_ic') },
  { title: 'Pearson IC HAC 推断', metrics: hacMetricDefinitions('pearson_ic_hac', true) },
  { title: 'Rank IC HAC 推断', metrics: hacMetricDefinitions('rank_ic_hac', true) },
  { title: '多空收益 HAC 推断', metrics: [
    ...hacMetricDefinitions('long_short'),
    { name: 'long_short_positive_rate', label: '多空收益为正比例' },
  ] },
  { title: '单调性、连续性与换手', metrics: [
    { name: 'monotonicity_mean', label: '平均分层单调性' },
    { name: 'monotonic_day_rate', label: '单调日期比例' },
    { name: 'rank_autocorrelation_mean', label: '平均秩自相关' },
    { name: 'high_quantile_turnover_mean', label: '高分位平均换手' },
    { name: 'low_quantile_turnover_mean', label: '低分位平均换手' },
    { name: 'total_turnover_mean', label: '平均总换手' },
    { name: 'break_even_cost_bps', label: '盈亏平衡成本' },
  ] },
]
const knownSummaryMetricNames = new Set(SUMMARY_METRIC_GROUPS.flatMap((group) => group.metrics.map((metric) => metric.name)))
const CURVE_GROUPS: Array<{ title: string; curves: CurveDefinition[] }> = [
  { title: 'IC 与分层收益', curves: [
    { type: 'ic', title: 'IC 时序' },
    { type: 'quantile_returns', title: '分层收益' },
    { type: 'long_short_returns', title: '多空收益' },
    { type: 'monotonicity', title: '分层单调性' },
  ] },
  { title: '换手与成本', curves: [
    { type: 'turnover', title: '换手与秩自相关' },
    { type: 'cost_scenarios', title: '成本情景与净 Spread' },
  ] },
  { title: '质量与相关性', curves: [
    { type: 'coverage', title: '因子覆盖率' },
    { type: 'label_quality', title: '标签质量' },
    { type: 'industry_coverage', title: '行业覆盖率' },
    { type: 'correlation', title: '因子相关性' },
  ] },
]
const CURVE_DEFINITIONS = CURVE_GROUPS.flatMap((group) => group.curves)

const route = useRoute()
const router = useRouter()
const queryClient = useQueryClient()
const studyId = computed(() => String(route.params.factorStudyId))
const tab = ref('research')
const selectedVariant = ref(String(route.query.signal_variant ?? ''))
const selectedLabel = ref(String(route.query.label_kind ?? ''))
const selectedFactor = ref(String(route.query.factor ?? ''))
const selectedHorizon = ref(Number(route.query.horizon ?? 0))
const notes = reactive<Record<string, string>>({})

const detail = useQuery({
  queryKey: computed(() => ['factor-study', studyId.value]),
  queryFn: () => api.get<FactorStudy>(`/api/v1/factor-studies/${studyId.value}`),
  refetchInterval: (query) => {
    const current = query.state.data as FactorStudy | undefined
    return ['QUEUED', 'RUNNING'].includes(current?.status ?? '') ? 2500 : false
  },
})
const task = useQuery({
  queryKey: computed(() => ['factor-study-task', detail.data.value?.task_id ?? '']),
  queryFn: () => api.get<TaskDetail>(`/api/v1/tasks/${detail.data.value?.task_id}`),
  enabled: computed(() => Boolean(detail.data.value?.task_id)),
  refetchInterval: (query) => {
    const current = query.state.data as TaskDetail | undefined
    return current && ['SUCCEEDED', 'FAILED', 'CANCELLED', 'ORPHANED'].includes(current.status)
      ? false
      : 2_500
  },
})
const matrix = useQuery({
  queryKey: computed(() => ['factor-study-matrix', studyId.value]),
  queryFn: () => api.get<{ items: FactorStudyMatrixRow[]; total: number }>(`/api/v1/factor-studies/${studyId.value}/matrix`),
  enabled: computed(() => detail.data.value?.status === 'SUCCEEDED'),
})
const variants = computed(() => [...new Set((matrix.data.value?.items ?? []).map((row) => row.signal_variant))])
const labels = computed(() => [...new Set((matrix.data.value?.items ?? []).map((row) => row.label_kind))])
const factors = computed(() => [...new Set((matrix.data.value?.items ?? []).map((row) => row.factor_ref))])
const horizons = computed(() => [...new Set((matrix.data.value?.items ?? []).map((row) => row.horizon))].sort((a, b) => a - b))
const filteredMatrix = computed(() => (matrix.data.value?.items ?? []).filter((row) =>
  (!selectedVariant.value || row.signal_variant === selectedVariant.value)
  && (!selectedLabel.value || row.label_kind === selectedLabel.value)
  && (!selectedFactor.value || row.factor_ref === selectedFactor.value)
  && (!selectedHorizon.value || row.horizon === selectedHorizon.value),
))
const review = computed(() => {
  const rows = matrix.data.value?.items ?? []
  const reviewed = rows.filter((row) => row.decision).length
  return { reviewed, total: rows.length, percent: rows.length ? Math.round(reviewed / rows.length * 100) : 0 }
})
const selectedMatrixRow = computed(() => filteredMatrix.value[0] ?? null)
watch(() => matrix.data.value?.items, (rows) => {
  if (!rows?.length) return
  selectedVariant.value ||= rows[0].signal_variant
  selectedLabel.value ||= rows[0].label_kind
  selectedFactor.value ||= rows[0].factor_ref
  selectedHorizon.value ||= rows[0].horizon
  for (const row of rows) notes[key(row)] = row.decision?.note ?? ''
}, { immediate: true })
watch([selectedVariant, selectedLabel, selectedFactor, selectedHorizon], () => {
  if (!selectedVariant.value) return
  void router.replace({ query: {
    ...route.query, signal_variant: selectedVariant.value, label_kind: selectedLabel.value,
    factor: selectedFactor.value, horizon: String(selectedHorizon.value),
  } })
})

function key(row: FactorStudyMatrixRow) { return `${row.signal_variant}/${row.label_kind}/${row.factor_ref}/${row.horizon}` }
function signalVariantLabel(value: string) {
  return value === 'DIRECTION_ADJUSTED'
    ? '方向统一'
    : value === 'INDUSTRY_NEUTRALIZED'
      ? '行业中性'
      : value
}
function returnLabel(value: string) {
  return value === 'THEORETICAL_FORWARD_RETURN'
    ? '理论远期收益'
    : value === 'EXECUTABLE_FORWARD_RETURN'
      ? '可执行远期收益'
      : value
}
function filters(type: string) {
  const params = new URLSearchParams({ page: '1', page_size: '1000' })
  const variantTypes = ['ic','quantile_returns','long_short_returns','monotonicity','turnover','cost_scenarios','coverage','correlation']
  const labelTypes = ['ic','quantile_returns','long_short_returns','monotonicity','cost_scenarios','label_quality']
  const factorTypes = ['ic','quantile_returns','long_short_returns','monotonicity','turnover','cost_scenarios','coverage']
  const horizonTypes = ['ic','quantile_returns','long_short_returns','monotonicity','cost_scenarios','label_quality']
  if (variantTypes.includes(type) && selectedVariant.value) params.set('signal_variant', selectedVariant.value)
  if (labelTypes.includes(type) && selectedLabel.value) params.set('label_kind', selectedLabel.value)
  if (factorTypes.includes(type) && selectedFactor.value) params.set('factor_ref', selectedFactor.value)
  if (horizonTypes.includes(type) && selectedHorizon.value) params.set('horizon', String(selectedHorizon.value))
  return params
}
const curveQueries = CURVE_DEFINITIONS.map((definition) => useQuery({
  queryKey: computed(() => ['factor-study-artifact', studyId.value, definition.type, filters(definition.type).toString()]),
  queryFn: () => api.get<{ items: RunRawArtifactRow[]; total: number }>(`/api/v1/factor-studies/${studyId.value}/artifacts/${definition.type}?${filters(definition.type)}`),
  enabled: computed(() => detail.data.value?.status === 'SUCCEEDED'
    && tab.value === 'research'
    && Boolean(selectedVariant.value && selectedLabel.value && selectedFactor.value && selectedHorizon.value)),
}))

function buildChartOption(type: CurveArtifactType, rows: RunRawArtifactRow[]) {
  if (!rows.length) return null
  if (type === 'correlation') {
    const names = [...new Set(rows.flatMap((row) => [String(row.factor_x), String(row.factor_y)]))].sort()
    return { tooltip, grid: { left: 100, right: 25, top: 18, bottom: 80 }, xAxis: { type: 'category', data: names, ...axis }, yAxis: { type: 'category', data: names, ...axis }, visualMap: { min: -1, max: 1, calculable: true, orient: 'horizontal', left: 'center', bottom: 0 }, series: [{ type: 'heatmap', data: rows.map((row) => [names.indexOf(String(row.factor_x)), names.indexOf(String(row.factor_y)), row.rank_correlation]) }] }
  }
  if (type === 'cost_scenarios') return lineOption(rows.map((row) => `${row.cost_bps}bps`), [{ name: '净 Spread', field: 'net_spread_mean' }, { name: '毛 Spread', field: 'gross_spread_mean' }], rows)
  if (type === 'turnover') return lineOption(rows.map((row) => String(row.signal_date)), [{ name: '秩自相关', field: 'rank_autocorrelation' }, { name: '高分位换手', field: 'high_quantile_turnover' }, { name: '低分位换手', field: 'low_quantile_turnover' }, { name: '总换手', field: 'total_turnover' }], rows)
  if (type === 'ic') return lineOption(rows.map((row) => String(row.signal_date)), [{ name: 'Rank IC', field: 'rank_ic' }, { name: '滚动 Rank IC', field: 'rank_ic_rolling_mean' }, { name: 'Pearson IC', field: 'pearson_ic' }], rows)
  if (type === 'long_short_returns') return lineOption(rows.map((row) => String(row.signal_date)), [{ name: '多空收益', field: 'long_short_return' }], rows)
  if (type === 'monotonicity') return lineOption(rows.map((row) => String(row.signal_date)), [{ name: '分位秩相关', field: 'quantile_rank_correlation' }, { name: '趋势斜率', field: 'trend_slope' }, { name: '端点 Spread', field: 'terminal_spread' }], rows)
  if (type === 'coverage') return lineOption(rows.map((row) => String(row.signal_date)), [{ name: '因子覆盖率', field: 'coverage' }], rows)
  if (type === 'industry_coverage') return lineOption(rows.map((row) => String(row.signal_date)), [{ name: '分类覆盖率', field: 'classified_coverage' }, { name: '可用覆盖率', field: 'usable_coverage' }], rows)
  if (type === 'quantile_returns') {
    const quantiles = [...new Set(rows.map((row) => Number(row.quantile)))].sort((a, b) => a - b)
    const dates = [...new Set(rows.map((row) => String(row.signal_date)))]
    return { ...lineOption(dates, [], rows), legend: { data: quantiles.map((q) => `Q${q}`), top: 2, left: 'center' }, series: quantiles.map((q) => ({ name: `Q${q}`, type: 'line', symbol: 'none', data: dates.map((date) => rows.find((row) => row.signal_date === date && Number(row.quantile) === q)?.mean_return ?? null) })) }
  }
  if (type === 'label_quality') {
    const reasons = [...new Set(rows.map((row) => String(row.reason)))]
    const dates = [...new Set(rows.map((row) => String(row.signal_date)))]
    return { tooltip, legend: { data: reasons, top: 2, left: 'center' }, grid: { left: 55, right: 24, top: 50, bottom: 56, containLabel: true }, xAxis: { type: 'category', data: dates, ...axis }, yAxis: { type: 'value', ...axis }, series: reasons.map((reason) => ({ name: reason, type: 'bar', stack: 'quality', data: dates.map((date) => rows.find((row) => row.signal_date === date && row.reason === reason)?.rate ?? 0) })) }
  }
  const dateField = rows[0].signal_date != null ? 'signal_date' : Object.keys(rows[0])[0]
  const numeric = Object.keys(rows[0]).find((name) => typeof rows[0][name] === 'number') ?? ''
  return lineOption(rows.map((row) => String(row[dateField])), [{ name: numeric, field: numeric }], rows)
}
function lineOption(categories: string[], series: Array<{ name: string; field: string }>, rows: RunRawArtifactRow[]) {
  return { tooltip, legend: { data: series.map((item) => item.name), top: 2, left: 'center' }, grid: { left: 55, right: 24, top: 50, bottom: 56, containLabel: true }, xAxis: { type: 'category', data: categories, ...axis }, yAxis: { type: 'value', ...axis }, dataZoom: [{ type: 'inside' }], series: series.map((item) => ({ name: item.name, type: 'line', symbol: 'none', data: rows.map((row) => row[item.field]) })) }
}

const curveGroups = computed(() => {
  let queryIndex = 0
  return CURVE_GROUPS.map((group) => ({
    title: group.title,
    curves: group.curves.map((definition) => {
      const query = curveQueries[queryIndex++]
      const rows = query.data.value?.items ?? []
      return { ...definition, option: buildChartOption(definition.type, rows), isLoading: query.isLoading.value, error: query.error.value }
    }),
  }))
})

function summaryMetricGroups(row: FactorStudyMatrixRow) {
  const groups = SUMMARY_METRIC_GROUPS.map((group) => ({
    title: group.title,
    metrics: group.metrics
      .filter((metric) => Object.prototype.hasOwnProperty.call(row.summary_metrics, metric.name))
      .map((metric) => ({ ...metric, value: row.summary_metrics[metric.name] })),
  })).filter((group) => group.metrics.length)
  const unknown = Object.entries(row.summary_metrics)
    .filter(([name]) => !knownSummaryMetricNames.has(name))
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([name, value]) => ({ name, label: name, value }))
  if (unknown.length) groups.push({ title: '其他 Summary 指标', metrics: unknown })
  return groups
}

function formatSummaryMetric(name: string, value: SummaryMetricValue | undefined) {
  if (value == null) return '—'
  if (typeof value === 'string') return value
  if (name.includes('p_value')) return value.toExponential(3)
  if (name === 'break_even_cost_bps') return `${Number(value.toFixed(6))} bps`
  if (/(?:count|lag|streak)$/.test(name)) return String(Math.round(value))
  return String(Number(value.toFixed(6)))
}

function formatHeadlineMetric(value: number | null, digits = 4) {
  if (value == null) return '—'
  return value.toFixed(digits)
}

function formatPValue(value: number | null) {
  if (value == null) return '—'
  return value < 0.001 ? value.toExponential(2) : value.toFixed(4)
}

function summaryNumber(row: FactorStudyMatrixRow, name: string) {
  const value = row.summary_metrics[name]
  return typeof value === 'number' ? value : null
}

function decisionLabel(mark: FactorDecisionMark | undefined) {
  if (mark === 'CANDIDATE') return '候选'
  if (mark === 'DISCARDED') return '排除'
  return '待评审'
}

const decide = useMutation({
  mutationFn: ({ row, mark }: { row: FactorStudyMatrixRow; mark: FactorDecisionMark }) => api.put<FactorStudy>(`/api/v1/factor-studies/${studyId.value}/decisions`, { signal_variant: row.signal_variant, label_kind: row.label_kind, factor_ref: row.factor_ref, horizon: row.horizon, mark, note: mark === 'UNREVIEWED' ? '' : notes[key(row)] ?? '' }),
  onSuccess: async () => { ElMessage.success('人工结论已保存'); await Promise.all([queryClient.invalidateQueries({ queryKey: ['factor-study', studyId.value] }), queryClient.invalidateQueries({ queryKey: ['factor-study-matrix', studyId.value] }), queryClient.invalidateQueries({ queryKey: ['factor-studies'] })]) },
})
const taskAction = useMutation({ mutationFn: (action: 'cancel' | 'retry') => api.post(`/api/v1/tasks/${detail.data.value?.task_id}/${action}`), onSuccess: async () => { await Promise.all([queryClient.invalidateQueries({ queryKey: ['factor-study', studyId.value] }), queryClient.invalidateQueries({ queryKey: ['factor-study-task'] })]) } })
const remove = useMutation({ mutationFn: () => api.delete(`/api/v1/factor-studies/${studyId.value}`), onSuccess: async () => { ElMessage.success('因子研究已删除'); await router.push('/factor-studies') } })
async function removeStudy() { try { await ElMessageBox.confirm('将删除终态研究及其可信产物。任务审计仍保留，此操作不可撤销。', '确认删除研究', { type: 'error' }) } catch { return } remove.mutate() }
const duration = computed(() => {
  const value = detail.data.value
  if (!value?.started_at) return '—'
  const end = value.completed_at ? new Date(value.completed_at).getTime() : Date.now()
  return `${Math.max(0, Math.round((end - new Date(value.started_at).getTime()) / 1000))}s`
})
const manifest = useQuery({ queryKey: computed(() => ['factor-study-manifest', studyId.value]), queryFn: () => api.get<Record<string, unknown>>(`/api/v1/factor-studies/${studyId.value}/artifacts/manifest`), enabled: computed(() => tab.value === 'evidence' && detail.data.value?.status === 'SUCCEEDED') })
</script>

<template>
  <div class="page-stack factor-detail">
    <ErrorState v-if="detail.error.value" :error="detail.error.value" />
    <section v-else-if="detail.isLoading.value" class="panel detail-loading" aria-label="正在读取因子研究详情">
      <el-skeleton :rows="6" animated />
    </section>
    <template v-else-if="detail.data.value">
      <section class="panel detail-hero">
        <div class="hero-copy">
          <RouterLink class="back-link" to="/factor-studies">← 返回因子研究</RouterLink>
          <span class="eyebrow">FACTOR STUDY · {{ detail.data.value.stage }}</span>
          <h2>{{ detail.data.value.definition.name }}</h2>
          <p>{{ detail.data.value.definition.description || '暂无研究说明' }}</p>
          <div class="evidence-line">
            <span>{{ detail.data.value.definition.start_date }} → {{ detail.data.value.definition.end_date }}</span>
            <span>{{ detail.data.value.definition.factor_ids.length }} 个因子 · {{ detail.data.value.definition.horizons.length }}个期限</span>
            <span class="hash">DATA {{ detail.data.value.catalog_hash.slice(0,12) }}</span>
            <span>耗时 {{ duration }}</span>
          </div>
        </div>
        <div class="hero-actions">
          <div class="hero-status"><StatusBadge :status="detail.data.value.status" /><span>{{ detail.data.value.stage }}</span></div>
          <div v-if="detail.data.value.status === 'SUCCEEDED'" class="review-progress">
            <div><span>人工评审进度</span><strong>{{ review.reviewed }} / {{ review.total }}</strong></div>
            <el-progress :percentage="review.percent" :show-text="false" />
            <small>{{ review.total - review.reviewed }} 个研究单元待判断</small>
          </div>
          <div class="toolbar hero-toolbar"><el-button v-if="['QUEUED','RUNNING'].includes(detail.data.value.status)" :loading="taskAction.isPending.value" @click="taskAction.mutate('cancel')">取消任务</el-button><el-button v-if="['FAILED','CANCELLED'].includes(detail.data.value.status)" :loading="taskAction.isPending.value" @click="taskAction.mutate('retry')">重试</el-button><el-button v-if="['SUCCEEDED','FAILED','CANCELLED'].includes(detail.data.value.status)" type="danger" text @click="removeStudy">删除研究</el-button></div>
        </div>
      </section>
      <FactorStudyTaskProgress v-if="task.data.value && detail.data.value.status !== 'SUCCEEDED'" :task="task.data.value" mode="detail" />
      <section v-if="detail.data.value.status === 'FAILED'" class="panel failure-state"><strong>研究执行失败</strong><pre>{{ JSON.stringify(detail.data.value.error, null, 2) }}</pre></section>
      <section v-else-if="['QUEUED','RUNNING'].includes(detail.data.value.status)" class="panel running-state"><StatusBadge :status="detail.data.value.status" /><div><strong>{{ detail.data.value.stage }}</strong><p>研究正在按固定阶段推进，发布成功后开放研究指标、全部曲线与人工结论。</p></div></section>
      <template v-else-if="detail.data.value.status === 'SUCCEEDED'">
        <section class="panel global-filters" aria-label="全局研究选择器">
          <div class="filter-heading"><span class="eyebrow">RESEARCH UNIT</span><strong>选择研究单元</strong><small>下方指标、曲线和结论始终使用同一组维度。</small></div>
          <label><span>因子处理</span><el-select v-model="selectedVariant"><el-option v-for="item in variants" :key="item" :label="signalVariantLabel(item)" :value="item" /></el-select></label><label><span>收益标签</span><el-select v-model="selectedLabel"><el-option v-for="item in labels" :key="item" :label="returnLabel(item)" :value="item" /></el-select></label><label><span>因子</span><el-select v-model="selectedFactor"><el-option v-for="item in factors" :key="item" :label="item" :value="item" /></el-select></label><label><span>期限</span><el-select v-model="selectedHorizon"><el-option v-for="item in horizons" :key="item" :label="`${item}D`" :value="item" /></el-select></label>
        </section>
        <section class="section-tabs"><el-tabs v-model="tab"><el-tab-pane label="研究指标与曲线" name="research" /><el-tab-pane label="配置 / 产物" name="evidence" /></el-tabs>
          <div v-if="tab === 'research'" class="research-stack">
            <section v-if="selectedMatrixRow" class="research-overview" aria-label="当前研究单元摘要">
              <div class="overview-title"><div><h2>摘要</h2><p>{{ selectedMatrixRow.factor_ref }} · {{ selectedMatrixRow.horizon }}D</p></div><span class="decision-state" :data-mark="selectedMatrixRow.decision?.mark ?? 'UNREVIEWED'">{{ decisionLabel(selectedMatrixRow.decision?.mark) }}</span></div>
              <div class="headline-metrics">
                <article><span>Rank IC</span><strong>{{ formatHeadlineMetric(selectedMatrixRow.rank_ic_mean) }}</strong><small>横截面排序预测力</small></article>
                <article><span>HAC t</span><strong>{{ formatHeadlineMetric(selectedMatrixRow.rank_ic_hac_t_stat ?? summaryNumber(selectedMatrixRow, 'rank_ic_hac_hac_t_stat'), 2) }}</strong><small>时序相关性稳健推断</small></article>
                <article><span>显著性 p 值</span><strong>{{ formatPValue(selectedMatrixRow.rank_ic_adjusted_p_value ?? summaryNumber(selectedMatrixRow, 'rank_ic_hac_hac_p_value')) }}</strong><small>{{ selectedMatrixRow.rank_ic_adjusted_p_value == null ? 'HAC 原始显著性' : `${detail.data.value.definition.correction} 多重检验` }}</small></article>
                <article><span>分层单调性</span><strong>{{ formatHeadlineMetric(selectedMatrixRow.monotonicity_mean) }}</strong><small>分位收益的顺序一致性</small></article>
                <article><span>毛 Spread</span><strong>{{ formatHeadlineMetric(selectedMatrixRow.gross_spread_mean, 6) }}</strong><small>未扣交易成本</small></article>
                <article><span>盈亏平衡成本</span><strong>{{ selectedMatrixRow.break_even_cost_bps == null ? '—' : `${selectedMatrixRow.break_even_cost_bps.toFixed(2)} bps` }}</strong><small>策略可容忍成本上限</small></article>
              </div>
              <div class="decision-workbench">
                <div><strong>结论</strong></div>
                <el-input v-model="notes[key(selectedMatrixRow)]" placeholder="记录保留或排除的理由" aria-label="人工结论备注" />
                <el-button-group><el-button type="success" plain :loading="decide.isPending.value" :disabled="selectedMatrixRow.decision?.mark === 'CANDIDATE'" @click="decide.mutate({ row: selectedMatrixRow, mark: 'CANDIDATE' })">Candidate</el-button><el-button type="danger" plain :loading="decide.isPending.value" :disabled="selectedMatrixRow.decision?.mark === 'DISCARDED'" @click="decide.mutate({ row: selectedMatrixRow, mark: 'DISCARDED' })">Discarded</el-button><el-button :loading="decide.isPending.value" @click="decide.mutate({ row: selectedMatrixRow, mark: 'UNREVIEWED' })">清除</el-button></el-button-group>
              </div>
            </section>
            <div v-else class="empty-research-unit">当前选择没有研究指标</div>
            <details v-for="row in filteredMatrix" :key="key(row)" class="summary-detail" :aria-label="`${row.factor_ref} ${row.horizon}D 完整 Summary 指标`">
              <summary class="summary-detail-heading"><div><h3>完整 Summary 指标</h3><p>{{ signalVariantLabel(row.signal_variant) }} · {{ returnLabel(row.label_kind) }} · {{ row.factor_ref }} · {{ row.horizon }}D</p></div><span>{{ Object.keys(row.summary_metrics).length }} 项 <b>展开</b></span></summary>
              <div class="summary-groups"><article v-for="group in summaryMetricGroups(row)" :key="group.title" class="summary-group"><h4>{{ group.title }}</h4><div class="summary-metric-grid"><div v-for="metric in group.metrics" :key="metric.name" class="summary-metric" :data-summary-metric="metric.name"><span>{{ metric.label }}</span><strong>{{ formatSummaryMetric(metric.name, metric.value) }}</strong><small>{{ metric.name }}</small></div></div></article></div>
            </details>
            <section v-for="group in curveGroups" :key="group.title" class="curve-section">
              <details :open="group.title === 'IC 与分层收益'" class="curve-disclosure">
              <summary class="panel-heading"><div><h2>{{ group.title }}</h2></div><span>{{ group.curves.length }} 张图 <b>展开</b></span></summary>
              <div class="curve-grid">
                <div v-for="curve in group.curves" :key="curve.type" class="curve-card" :data-curve-artifact="curve.type">
                  <ErrorState v-if="curve.error" :error="curve.error" />
                  <div v-else-if="curve.isLoading" class="curve-loading">正在读取 {{ curve.title }}…</div>
                  <ChartCard v-else :title="curve.title" :subtitle="curve.type" :empty="!curve.option"><VChart v-if="curve.option" class="artifact-chart" :option="curve.option" autoresize /></ChartCard>
                </div>
              </div>
              </details>
            </section>
          </div>
          <div v-else class="evidence-grid"><section class="panel"><div class="panel-heading"><div><h2>规范配置</h2><p>{{ detail.data.value.config_hash }}</p></div></div><pre>{{ JSON.stringify(detail.data.value.definition, null, 2) }}</pre></section><section class="panel"><div class="panel-heading"><div><h2>任务与 Manifest</h2><p class="hash">TASK {{ detail.data.value.task_id }}</p></div></div><pre>{{ JSON.stringify(manifest.data.value ?? {}, null, 2) }}</pre></section><section class="panel artifact-register"><div class="panel-heading"><div><h2>产物登记</h2><p>类型、路径、行数和哈希</p></div></div><el-table :data="detail.data.value.artifacts"><el-table-column prop="artifact_type" label="类型" /><el-table-column prop="relative_path" label="路径" min-width="190" /><el-table-column prop="row_count" label="行数" width="80" /><el-table-column prop="content_hash" label="SHA-256" min-width="180" show-overflow-tooltip /></el-table></section></div>
        </section>
      </template>
    </template>
  </div>
</template>

<style scoped>
.factor-detail{max-width:1500px;margin:0 auto}.detail-loading{min-height:360px;padding:30px}.detail-hero{display:grid;grid-template-columns:minmax(0,1fr) 300px;align-items:stretch;gap:38px;padding:26px 28px;border-color:rgba(37,99,235,.18);background:linear-gradient(118deg,#fff 0%,#f4f8ff 62%,#edf8f7 100%)}.detail-hero::after{content:"";position:absolute;width:230px;height:230px;right:70px;top:-175px;border-radius:50%;background:radial-gradient(circle,rgba(8,127,121,.14),rgba(37,99,235,0));pointer-events:none}.hero-copy,.hero-actions{position:relative;z-index:1}.hero-copy{min-width:0}.back-link{display:flex;width:max-content;margin-bottom:22px;color:var(--dim);font-size:11px;text-decoration:none;transition:color 140ms ease}.back-link:hover{color:var(--blue)}.detail-hero h2{margin:9px 0 7px;font-size:27px;letter-spacing:-.035em}.detail-hero p{max-width:700px;margin:0;color:var(--muted);font-size:13px;line-height:1.65}.evidence-line{display:flex;flex-wrap:wrap;gap:8px;margin-top:18px}.evidence-line>span{padding:6px 9px;border:1px solid rgba(128,151,181,.18);border-radius:6px;color:var(--dim);background:rgba(255,255,255,.72);font-size:10px}.hero-actions{display:flex;flex-direction:column;padding-left:24px;border-left:1px solid rgba(128,151,181,.2)}.hero-status{display:flex;align-items:center;justify-content:space-between;gap:10px}.hero-status>span{color:var(--dim);font:10px ui-monospace,Consolas,monospace}.review-progress{display:grid;gap:8px;margin-top:25px}.review-progress>div{display:flex;align-items:baseline;justify-content:space-between}.review-progress span,.review-progress small{color:var(--dim);font-size:10px}.review-progress strong{font-size:18px;font-variant-numeric:tabular-nums}.hero-toolbar{justify-content:flex-end;margin-top:auto;padding-top:20px}.global-filters{display:grid;grid-template-columns:minmax(210px,.9fr) repeat(4,minmax(145px,1fr));align-items:end;gap:14px;padding:16px 18px}.filter-heading{display:flex;min-width:0;flex-direction:column;align-self:center}.filter-heading strong{margin-top:5px;font-size:13px}.filter-heading small{overflow:hidden;margin-top:5px;color:var(--dim);font-size:10px;line-height:1.45;text-overflow:ellipsis;white-space:nowrap}.global-filters label{display:grid;gap:6px;color:var(--dim);font-size:10px}.research-stack{display:grid;gap:16px}.research-overview{overflow:hidden;border:1px solid var(--border);border-radius:11px;background:linear-gradient(180deg,#fff,#fbfcfe)}.overview-title{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;padding:18px 20px 15px}.overview-title h2{margin:0;font-size:17px}.overview-title p{margin:5px 0 0;color:var(--dim);font:10px ui-monospace,Consolas,monospace}.decision-state{padding:6px 10px;border:1px solid rgba(169,109,10,.24);border-radius:999px;color:var(--warning);background:rgba(169,109,10,.07);font-size:11px;font-weight:700}.decision-state[data-mark="CANDIDATE"]{border-color:rgba(22,130,95,.25);color:var(--success);background:rgba(22,130,95,.07)}.decision-state[data-mark="DISCARDED"]{border-color:rgba(214,59,86,.25);color:var(--danger);background:rgba(214,59,86,.07)}.headline-metrics{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));border-top:1px solid var(--border);border-bottom:1px solid var(--border);background:#f8fafd}.headline-metrics article{min-width:0;padding:16px 18px;border-right:1px solid var(--border)}.headline-metrics article:last-child{border-right:0}.headline-metrics span{display:block;color:var(--muted);font-size:10px}.headline-metrics strong{display:block;overflow:hidden;margin-top:10px;font-size:19px;font-variant-numeric:tabular-nums;letter-spacing:-.03em;text-overflow:ellipsis;white-space:nowrap}.headline-metrics small{display:block;overflow:hidden;margin-top:6px;color:var(--dim);font-size:9px;text-overflow:ellipsis;white-space:nowrap}.decision-workbench{display:grid;grid-template-columns:minmax(100px,.35fr) minmax(260px,1.4fr) max-content;align-items:center;gap:18px;padding:17px 20px}.decision-workbench>div:first-child{display:flex;min-width:0;flex-direction:column}.decision-workbench>div:first-child strong{margin:0;font-size:13px}.decision-workbench>div:first-child small{margin-top:4px;color:var(--dim);font-size:10px;line-height:1.45}.empty-research-unit{display:grid;min-height:160px;place-items:center;border:1px dashed var(--border);border-radius:10px;color:var(--dim);font-size:12px}.summary-detail,.curve-disclosure{overflow:hidden;border:1px solid var(--border);border-radius:10px;background:var(--surface)}.summary-detail-heading,.curve-disclosure>.panel-heading{display:flex;align-items:center;justify-content:space-between;gap:16px;margin:0;padding:16px 18px;cursor:pointer;list-style:none}.summary-detail-heading::-webkit-details-marker,.curve-disclosure>.panel-heading::-webkit-details-marker{display:none}.summary-detail-heading:hover,.curve-disclosure>.panel-heading:hover{background:#f1f5fa}.summary-detail-heading h3{margin:0;font-size:13px}.summary-detail-heading p{margin:5px 0 0;color:var(--muted);font-size:11px}.summary-detail-heading>span,.curve-disclosure>.panel-heading>span{flex:none;color:var(--dim);font-size:11px}.summary-detail-heading b,.curve-disclosure>.panel-heading b{margin-left:7px;color:var(--blue);font-weight:600}.summary-detail[open] .summary-detail-heading b,.curve-disclosure[open]>.panel-heading b{font-size:0}.summary-detail[open] .summary-detail-heading b::after,.curve-disclosure[open]>.panel-heading b::after{content:"收起";font-size:11px}.summary-groups{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;padding:0 16px 16px}.summary-group{padding:14px;border:1px solid var(--border);border-radius:9px;background:var(--surface)}.summary-group h4{margin:0 0 10px;font-size:12px}.summary-metric-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1px 14px}.summary-metric{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:3px 10px;padding:8px 0;border-bottom:1px solid var(--border)}.summary-metric span{color:var(--muted);font-size:11px}.summary-metric strong{text-align:right;font-size:12px}.summary-metric small{grid-column:1/-1;color:var(--dim);font:9px/1.3 ui-monospace,Consolas,monospace;overflow-wrap:anywhere}.curve-section{display:grid}.curve-disclosure{background:var(--surface)}.curve-disclosure>.panel-heading{min-height:auto}.curve-disclosure>.panel-heading h2{font-size:13px}.curve-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;padding:0 14px 14px}.curve-card{min-width:0}.curve-loading{display:grid;min-height:330px;place-items:center;border:1px solid var(--border);border-radius:10px;color:var(--muted);background:var(--surface)}.artifact-chart{height:330px}.running-state{display:flex;align-items:center;gap:17px;padding:30px}.running-state p{margin:6px 0 0;color:var(--muted)}.failure-state{border-color:rgba(214,59,86,.3)}.failure-state pre,.evidence-grid pre{max-height:520px;overflow:auto;font:11px/1.6 ui-monospace,Consolas,monospace;white-space:pre-wrap}.evidence-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.artifact-register{grid-column:1/-1}
@media(max-width:1360px){.global-filters{grid-template-columns:repeat(4,minmax(0,1fr))}.filter-heading{grid-column:1/-1}.filter-heading small{white-space:normal}.headline-metrics{grid-template-columns:repeat(3,minmax(0,1fr))}.headline-metrics article:nth-child(3){border-right:0}.headline-metrics article:nth-child(-n+3){border-bottom:1px solid var(--border)}.evidence-grid,.summary-groups{grid-template-columns:1fr}.artifact-register{grid-column:auto}}
@media(max-width:1120px){.detail-hero{grid-template-columns:minmax(0,1fr) 260px;gap:24px}.decision-workbench{grid-template-columns:1fr}.decision-workbench .el-button-group{justify-self:start}.curve-grid{grid-template-columns:1fr}}
</style>
