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
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime } from '../format'
import type { FactorDecisionMark, FactorStudy, FactorStudyMatrixRow, RunRawArtifactRow } from '../types'

const route = useRoute()
const router = useRouter()
const queryClient = useQueryClient()
const studyId = computed(() => String(route.params.factorStudyId))
const tab = ref('matrix')
const analysisArtifact = ref('ic')
const executionArtifact = ref('turnover')
const qualityArtifact = ref('coverage')
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
const activeArtifact = computed(() => tab.value === 'analysis' ? analysisArtifact.value : tab.value === 'execution' ? executionArtifact.value : tab.value === 'quality' ? qualityArtifact.value : '')
const artifact = useQuery({
  queryKey: computed(() => ['factor-study-artifact', studyId.value, activeArtifact.value, selectedVariant.value, selectedLabel.value, selectedFactor.value, selectedHorizon.value]),
  queryFn: () => api.get<{ items: RunRawArtifactRow[]; total: number }>(`/api/v1/factor-studies/${studyId.value}/artifacts/${activeArtifact.value}?${filters(activeArtifact.value)}`),
  enabled: computed(() => detail.data.value?.status === 'SUCCEEDED' && Boolean(activeArtifact.value)),
})
const artifactRows = computed(() => artifact.data.value?.items ?? [])
const artifactColumns = computed(() => Object.keys(artifactRows.value[0] ?? {}))
const chartOption = computed(() => {
  const rows = artifactRows.value
  const type = activeArtifact.value
  if (!rows.length) return null
  if (type === 'correlation') {
    const names = [...new Set(rows.flatMap((row) => [String(row.factor_x), String(row.factor_y)]))].sort()
    return { tooltip, grid: { left: 100, right: 25, top: 18, bottom: 80 }, xAxis: { type: 'category', data: names, ...axis }, yAxis: { type: 'category', data: names, ...axis }, visualMap: { min: -1, max: 1, calculable: true, orient: 'horizontal', left: 'center', bottom: 0 }, series: [{ type: 'heatmap', data: rows.map((row) => [names.indexOf(String(row.factor_x)), names.indexOf(String(row.factor_y)), row.rank_correlation]) }] }
  }
  if (type === 'cost_scenarios') return lineOption(rows.map((row) => `${row.cost_bps}bps`), [{ name: '净 spread', field: 'net_spread_mean' }])
  if (type === 'turnover') return lineOption(rows.map((row) => String(row.signal_date)), [{ name: '秩自相关', field: 'rank_autocorrelation' }, { name: '高分位换手', field: 'high_quantile_turnover' }, { name: '低分位换手', field: 'low_quantile_turnover' }, { name: '总换手', field: 'total_turnover' }])
  if (type === 'ic') return lineOption(rows.map((row) => String(row.signal_date)), [{ name: 'Rank IC', field: 'rank_ic' }, { name: '滚动 IC', field: 'rank_ic_rolling_mean' }, { name: 'Pearson IC', field: 'pearson_ic' }])
  if (type === 'quantile_returns') {
    const quantiles = [...new Set(rows.map((row) => Number(row.quantile)))].sort((a, b) => a - b)
    const dates = [...new Set(rows.map((row) => String(row.signal_date)))]
    return { ...lineOption(dates, []), legend: { data: quantiles.map((q) => `Q${q}`), top: 2, left: 'center' }, series: quantiles.map((q) => ({ name: `Q${q}`, type: 'line', symbol: 'none', data: dates.map((date) => rows.find((row) => row.signal_date === date && Number(row.quantile) === q)?.mean_return ?? null) })) }
  }
  if (type === 'label_quality') {
    const reasons = [...new Set(rows.map((row) => String(row.reason)))]
    const dates = [...new Set(rows.map((row) => String(row.signal_date)))]
    return { tooltip, legend: { data: reasons, top: 2, left: 'center' }, grid: { left: 55, right: 24, top: 50, bottom: 56, containLabel: true }, xAxis: { type: 'category', data: dates, ...axis }, yAxis: { type: 'value', ...axis }, series: reasons.map((reason) => ({ name: reason, type: 'bar', stack: 'quality', data: dates.map((date) => rows.find((row) => row.signal_date === date && row.reason === reason)?.rate ?? 0) })) }
  }
  const dateField = rows[0].signal_date != null ? 'signal_date' : Object.keys(rows[0])[0]
  const numeric = Object.keys(rows[0]).find((name) => typeof rows[0][name] === 'number') ?? ''
  return lineOption(rows.map((row) => String(row[dateField])), [{ name: numeric, field: numeric }])
})
function lineOption(rows: string[], series: Array<{ name: string; field: string }>) {
  return { tooltip, legend: { data: series.map((item) => item.name), top: 2, left: 'center' }, grid: { left: 55, right: 24, top: 50, bottom: 56, containLabel: true }, xAxis: { type: 'category', data: rows, ...axis }, yAxis: { type: 'value', ...axis }, dataZoom: [{ type: 'inside' }], series: series.map((item) => ({ name: item.name, type: 'line', symbol: 'none', data: artifactRows.value.map((row) => row[item.field]) })) }
}

const decide = useMutation({
  mutationFn: ({ row, mark }: { row: FactorStudyMatrixRow; mark: FactorDecisionMark }) => api.put<FactorStudy>(`/api/v1/factor-studies/${studyId.value}/decisions`, { signal_variant: row.signal_variant, label_kind: row.label_kind, factor_ref: row.factor_ref, horizon: row.horizon, mark, note: mark === 'UNREVIEWED' ? '' : notes[key(row)] ?? '' }),
  onSuccess: async () => { ElMessage.success('人工结论已保存'); await Promise.all([queryClient.invalidateQueries({ queryKey: ['factor-study', studyId.value] }), queryClient.invalidateQueries({ queryKey: ['factor-study-matrix', studyId.value] }), queryClient.invalidateQueries({ queryKey: ['factor-studies'] })]) },
})
const taskAction = useMutation({ mutationFn: (action: 'cancel' | 'retry') => api.post(`/api/v1/tasks/${detail.data.value?.task_id}/${action}`), onSuccess: async () => { await queryClient.invalidateQueries({ queryKey: ['factor-study', studyId.value] }) } })
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
    <template v-else-if="detail.data.value">
      <section class="panel detail-hero">
        <div><span class="eyebrow">FACTOR STUDY · {{ detail.data.value.stage }}</span><h2>{{ detail.data.value.definition.name }}</h2><p>{{ detail.data.value.definition.description || '—' }}</p><div class="evidence-line"><span>{{ detail.data.value.definition.start_date }} → {{ detail.data.value.definition.end_date }}</span><span class="hash">DATA {{ detail.data.value.catalog_hash.slice(0,12) }}</span><span>耗时 {{ duration }}</span></div></div>
        <div class="hero-actions"><StatusBadge :status="detail.data.value.status" /><el-progress :percentage="review.percent" :format="() => `${review.reviewed}/${review.total}`" /><div class="toolbar"><el-button v-if="['QUEUED','RUNNING'].includes(detail.data.value.status)" :loading="taskAction.isPending.value" @click="taskAction.mutate('cancel')">取消任务</el-button><el-button v-if="['FAILED','CANCELLED'].includes(detail.data.value.status)" :loading="taskAction.isPending.value" @click="taskAction.mutate('retry')">重试</el-button><el-button v-if="['SUCCEEDED','FAILED','CANCELLED'].includes(detail.data.value.status)" type="danger" plain @click="removeStudy">删除</el-button></div></div>
      </section>
      <section v-if="detail.data.value.status === 'FAILED'" class="panel failure-state"><strong>研究执行失败</strong><pre>{{ JSON.stringify(detail.data.value.error, null, 2) }}</pre></section>
      <section v-else-if="['QUEUED','RUNNING'].includes(detail.data.value.status)" class="panel running-state"><StatusBadge :status="detail.data.value.status" /><div><strong>{{ detail.data.value.stage }}</strong><p>研究正在按固定阶段推进，发布成功后开放候选矩阵与人工结论。</p></div></section>
      <template v-else-if="detail.data.value.status === 'SUCCEEDED'">
        <section class="panel global-filters" aria-label="全局研究选择器"><label><span>因子处理</span><el-select v-model="selectedVariant"><el-option v-for="item in variants" :key="item" :label="signalVariantLabel(item)" :value="item" /></el-select></label><label><span>收益标签</span><el-select v-model="selectedLabel"><el-option v-for="item in labels" :key="item" :label="returnLabel(item)" :value="item" /></el-select></label><label><span>因子</span><el-select v-model="selectedFactor"><el-option v-for="item in factors" :key="item" :label="item" :value="item" /></el-select></label><label><span>期限</span><el-select v-model="selectedHorizon"><el-option v-for="item in horizons" :key="item" :label="`${item}D`" :value="item" /></el-select></label></section>
        <section class="section-tabs"><el-tabs v-model="tab"><el-tab-pane label="候选矩阵" name="matrix" /><el-tab-pane label="IC / 分层" name="analysis" /><el-tab-pane label="换手 / 成本" name="execution" /><el-tab-pane label="质量 / 相关" name="quality" /><el-tab-pane label="配置 / 产物" name="evidence" /></el-tabs>
          <div v-if="tab === 'matrix'">
            <el-table :data="filteredMatrix" empty-text="当前选择没有矩阵行"><el-table-column prop="factor_ref" label="因子" min-width="155" /><el-table-column prop="horizon" label="期限" width="70" /><el-table-column prop="rank_ic_mean" label="Rank IC" width="100" /><el-table-column prop="rank_ic_hac_t_stat" label="HAC t" width="90" /><el-table-column prop="rank_ic_adjusted_p_value" label="校正 p" width="100" /><el-table-column prop="monotonicity_mean" label="单调性" width="90" /><el-table-column prop="gross_spread_mean" label="毛 spread" width="105" /><el-table-column prop="break_even_cost_bps" label="盈亏平衡 bps" width="120" /><el-table-column prop="total_turnover_mean" label="换手" width="90" /><el-table-column label="结论与备注" min-width="330"><template #default="scope"><div class="decision-cell"><el-input v-model="notes[key(scope.row)]" size="small" placeholder="人工备注" aria-label="人工结论备注" /><el-button-group><el-button size="small" type="success" plain :disabled="scope.row.decision?.mark === 'CANDIDATE'" @click="decide.mutate({ row: scope.row, mark: 'CANDIDATE' })">Candidate</el-button><el-button size="small" type="danger" plain :disabled="scope.row.decision?.mark === 'DISCARDED'" @click="decide.mutate({ row: scope.row, mark: 'DISCARDED' })">Discarded</el-button><el-button size="small" @click="decide.mutate({ row: scope.row, mark: 'UNREVIEWED' })">清除</el-button></el-button-group></div></template></el-table-column></el-table>
          </div>
          <div v-else-if="tab === 'analysis'"><div class="artifact-switch"><el-radio-group v-model="analysisArtifact" size="small"><el-radio-button value="ic">IC 时序</el-radio-button><el-radio-button value="quantile_returns">独立分层</el-radio-button><el-radio-button value="long_short_returns">多空收益</el-radio-button><el-radio-button value="monotonicity">单调性</el-radio-button></el-radio-group></div><ChartCard :title="analysisArtifact" :empty="!chartOption"><VChart v-if="chartOption" class="artifact-chart" :option="chartOption" autoresize /></ChartCard></div>
          <div v-else-if="tab === 'execution'"><div class="artifact-switch"><el-radio-group v-model="executionArtifact" size="small"><el-radio-button value="turnover">换手与秩自相关</el-radio-button><el-radio-button value="cost_scenarios">bps—净 spread</el-radio-button></el-radio-group></div><ChartCard :title="executionArtifact" :empty="!chartOption"><VChart v-if="chartOption" class="artifact-chart" :option="chartOption" autoresize /></ChartCard></div>
          <div v-else-if="tab === 'quality'"><div class="artifact-switch"><el-radio-group v-model="qualityArtifact" size="small"><el-radio-button value="coverage">因子覆盖率</el-radio-button><el-radio-button value="label_quality">标签失败原因</el-radio-button><el-radio-button value="industry_coverage">行业覆盖</el-radio-button><el-radio-button value="correlation">因子相关性</el-radio-button></el-radio-group></div><div class="quality-grid"><ChartCard :title="qualityArtifact" :empty="!chartOption"><VChart v-if="chartOption" class="artifact-chart" :option="chartOption" autoresize /></ChartCard><section class="panel raw-table"><div class="panel-heading"><div><h2>证据明细</h2><p>Manifest 复核后的原始行</p></div></div><el-table :data="artifactRows" max-height="420"><el-table-column v-for="column in artifactColumns" :key="column" :prop="column" :label="column" min-width="130" /></el-table></section></div></div>
          <div v-else-if="tab === 'evidence'" class="evidence-grid"><section class="panel"><div class="panel-heading"><div><h2>规范配置</h2><p>{{ detail.data.value.config_hash }}</p></div></div><pre>{{ JSON.stringify(detail.data.value.definition, null, 2) }}</pre></section><section class="panel"><div class="panel-heading"><div><h2>任务与 Manifest</h2><p class="hash">TASK {{ detail.data.value.task_id }}</p></div></div><pre>{{ JSON.stringify(manifest.data.value ?? {}, null, 2) }}</pre></section><section class="panel artifact-register"><div class="panel-heading"><div><h2>产物登记</h2><p>类型、路径、行数和哈希</p></div></div><el-table :data="detail.data.value.artifacts"><el-table-column prop="artifact_type" label="类型" /><el-table-column prop="relative_path" label="路径" min-width="190" /><el-table-column prop="row_count" label="行数" width="80" /><el-table-column prop="content_hash" label="SHA-256" min-width="180" show-overflow-tooltip /></el-table></section></div>
        </section>
      </template>
    </template>
  </div>
</template>

<style scoped>
.detail-hero{display:flex;align-items:flex-start;justify-content:space-between;gap:26px;padding:24px;background:linear-gradient(120deg,#fff,#f3f7ff 60%,#eef9f7)}.detail-hero h2{margin:9px 0 7px;font-size:24px}.detail-hero p{margin:0;color:var(--muted)}.evidence-line{display:flex;gap:15px;margin-top:17px;color:var(--dim);font-size:11px}.hero-actions{width:310px;display:grid;gap:13px}.global-filters{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;padding:15px 18px}.global-filters label{display:grid;gap:6px;color:var(--dim);font-size:10px}.decision-cell{display:grid;gap:7px;padding:5px 0}.artifact-switch{display:flex;justify-content:flex-end;margin:5px 0 12px}.artifact-chart{height:390px}.quality-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(440px,.8fr);gap:14px}.raw-table{padding-bottom:6px}.running-state{display:flex;align-items:center;gap:17px;padding:30px}.running-state p{margin:6px 0 0;color:var(--muted)}.failure-state{border-color:rgba(214,59,86,.3)}.failure-state pre,.evidence-grid pre{max-height:520px;overflow:auto;font:11px/1.6 ui-monospace,Consolas,monospace;white-space:pre-wrap}.evidence-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.artifact-register{grid-column:1/-1}@media(max-width:1360px){.quality-grid,.evidence-grid{grid-template-columns:1fr}.artifact-register{grid-column:auto}}
</style>
