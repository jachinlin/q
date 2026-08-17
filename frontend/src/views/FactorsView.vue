<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage } from 'element-plus'
import { computed, reactive, ref, watch } from 'vue'
import VChart from 'vue-echarts'
import { useRoute, useRouter } from 'vue-router'

import { api } from '../api'
import { axis, tooltip } from '../charts'
import ChartCard from '../components/ChartCard.vue'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime, shortHash } from '../format'
import type { FactorCatalog, FactorCorrelationResponse, FactorIndustryCoverageResponse, FactorRun, FactorSeries, FactorSignalVariant, FactorStudy, Page } from '../types'

const queryClient = useQueryClient()
const route = useRoute()
const router = useRouter()

function queryRunId(value: unknown) {
  return typeof value === 'string' && value ? value : ''
}

const studyId = ref('')
const runId = ref(queryRunId(route.query.run))
const factorRef = ref('')
const horizon = ref(20)
const icKind = ref<'rank' | 'pearson'>('rank')
const signalVariant = ref<FactorSignalVariant>('DIRECTION_ADJUSTED')
const createOpen = ref(false)
const runHistoryOpen = ref<string[]>([])
const form = reactive({
  name: '',
  factor_refs: [] as string[],
  dates: [] as string[],
  industry_enabled: false,
  unclassified_policy: 'EXCLUDE' as 'EXCLUDE' | 'UNCLASSIFIED',
})

const catalog = useQuery({
  queryKey: ['factor-catalog'],
  queryFn: () => api.get<FactorCatalog>('/api/v1/factors/catalog'),
})
const studies = useQuery({
  queryKey: ['factor-studies'],
  queryFn: () => api.get<Page<FactorStudy>>('/api/v1/factor-studies?page=1&page_size=100'),
})
const study = useQuery({
  queryKey: computed(() => ['factor-study', studyId.value]),
  queryFn: () => api.get<FactorStudy>(`/api/v1/factor-studies/${studyId.value}`),
  enabled: computed(() => Boolean(studyId.value)),
  refetchInterval: (query) => {
    const current = query.state.data as FactorStudy | undefined
    return ['QUEUED', 'RUNNING'].includes(current?.runs?.[0]?.status ?? '') ? 3000 : false
  },
})
const run = useQuery({
  queryKey: computed(() => ['factor-run', runId.value]),
  queryFn: () => api.get<FactorRun>(`/api/v1/factor-runs/${runId.value}`),
  enabled: computed(() => Boolean(runId.value)),
  refetchInterval: (query) => ['QUEUED', 'RUNNING'].includes(String(query.state.data?.status)) ? 3000 : false,
})
const series = useQuery({
  queryKey: computed(() => ['factor-series', runId.value, factorRef.value, horizon.value, signalVariant.value, run.data.value?.manifest_hash]),
  queryFn: () => api.get<FactorSeries>(`/api/v1/factor-runs/${runId.value}/series?factor_ref=${encodeURIComponent(factorRef.value)}&horizon=${horizon.value}&signal_variant=${signalVariant.value}`),
  enabled: computed(() => run.data.value?.status === 'SUCCEEDED' && Boolean(factorRef.value)),
})
const correlation = useQuery({
  queryKey: computed(() => ['factor-correlation', runId.value, signalVariant.value, run.data.value?.manifest_hash]),
  queryFn: () => api.get<FactorCorrelationResponse>(`/api/v1/factor-runs/${runId.value}/correlation?signal_variant=${signalVariant.value}`),
  enabled: computed(() => run.data.value?.status === 'SUCCEEDED'),
})
const industryCoverage = useQuery({
  queryKey: computed(() => ['factor-industry-coverage', runId.value, run.data.value?.manifest_hash]),
  queryFn: () => api.get<FactorIndustryCoverageResponse>(`/api/v1/factor-runs/${runId.value}/industry-coverage`),
  enabled: computed(() => run.data.value?.status === 'SUCCEEDED' && Boolean(run.data.value?.config.industry)),
})

const runs = computed(() => study.data.value?.runs ?? [])
const latestRunId = computed(() => runs.value[0]?.id ?? '')
const latestRun = computed(() => runs.value[0] ?? null)
const analysisActive = computed(() => ['QUEUED', 'RUNNING'].includes(latestRun.value?.status ?? ''))
const viewingLatest = computed(() => Boolean(runId.value) && runId.value === latestRunId.value)
const selectedStudy = computed(() => study.data.value ?? studies.data.value?.items.find((item) => item.id === studyId.value))
const availableHorizons = computed(() => run.data.value?.config.horizons ?? catalog.data.value?.horizons ?? [1, 5, 20])

watch(() => studies.data.value?.items, (items) => {
  if (!studyId.value && !runId.value && items?.length) studyId.value = items[0].id
}, { immediate: true })
watch(studyId, (value) => {
  const selectedRun = run.data.value
  if (selectedRun?.id !== runId.value || selectedRun.study_id !== value) runId.value = ''
  factorRef.value = ''
  runHistoryOpen.value = []
  signalVariant.value = 'DIRECTION_ADJUSTED'
})
watch(() => route.query.run, (value) => {
  const next = queryRunId(value)
  if (next === runId.value) return
  runId.value = next || study.data.value?.runs?.[0]?.id || ''
})
watch(runId, (value) => {
  if (queryRunId(route.query.run) === value) return
  const query = { ...route.query }
  if (value) query.run = value
  else delete query.run
  void router.replace({ name: 'factors', query })
})
watch(() => run.data.value, (value) => {
  if (value?.id === runId.value && value.study_id !== studyId.value) {
    studyId.value = value.study_id
  }
}, { immediate: true })
watch(() => study.data.value?.runs, (items) => {
  if (!items?.length) return
  if (!runId.value) runId.value = items[0].id
}, { immediate: true })
watch(() => run.data.value?.config.factor_refs, (refs) => {
  if (refs?.length && !refs.includes(factorRef.value)) factorRef.value = refs[0]
}, { immediate: true })
watch(() => run.data.value?.config.industry, (industry) => {
  if (!industry) signalVariant.value = 'DIRECTION_ADJUSTED'
}, { immediate: true })
watch(() => run.data.value?.status, async (status) => {
  if (status && !['QUEUED', 'RUNNING'].includes(status) && studyId.value) {
    await queryClient.invalidateQueries({ queryKey: ['factor-study', studyId.value] })
    await queryClient.invalidateQueries({ queryKey: ['factor-studies'] })
  }
})

const createStudy = useMutation({
  mutationFn: () => api.post<FactorStudy>('/api/v1/factor-studies', {
    name: form.name,
    factor_refs: form.factor_refs,
    start_date: form.dates[0],
    end_date: form.dates[1],
    industry: form.industry_enabled
      ? {
          taxonomy: catalog.data.value?.industry.taxonomy ?? '证监会行业分类',
          unclassified_policy: form.unclassified_policy,
        }
      : null,
  }),
  onSuccess: async (value) => {
    createOpen.value = false
    studyId.value = value.id
    await queryClient.invalidateQueries({ queryKey: ['factor-studies'] })
    ElMessage.success('因子研究已创建')
  },
  onError: (error) => ElMessage.error(`创建研究失败：${String(error)}`),
})
const startRun = useMutation({
  mutationFn: () => api.post<{ run_id: string }>(`/api/v1/factor-studies/${studyId.value}/runs`),
  onSuccess: async (value) => {
    runId.value = value.run_id
    await queryClient.invalidateQueries({ queryKey: ['factor-study', studyId.value] })
    await queryClient.invalidateQueries({ queryKey: ['factor-studies'] })
    ElMessage.success('分析已开始，结果会自动更新')
  },
  onError: (error) => ElMessage.error(`运行分析失败：${String(error)}`),
})

const summary = computed(() => run.data.value?.summary?.find((item) => item.factor_ref === factorRef.value && Number(item.horizon) === horizon.value && item.signal_variant === signalVariant.value))
const icStats = computed(() => {
  const item = summary.value
  if (!item) return null
  return icKind.value === 'rank'
    ? {
        mean: item.rank_ic_mean,
        median: item.rank_ic_p50,
        icir: item.rank_icir_unannualized,
        positiveRate: item.rank_ic_positive_rate,
        validDates: item.rank_ic_valid_date_count,
        positiveStreak: item.rank_ic_max_positive_streak,
        positiveStart: item.rank_ic_positive_streak_start,
        positiveEnd: item.rank_ic_positive_streak_end,
        negativeStreak: item.rank_ic_max_negative_streak,
        negativeStart: item.rank_ic_negative_streak_start,
        negativeEnd: item.rank_ic_negative_streak_end,
      }
    : {
        mean: item.pearson_ic_mean,
        median: item.pearson_ic_p50,
        icir: item.pearson_icir_unannualized,
        positiveRate: item.pearson_ic_positive_rate,
        validDates: item.pearson_ic_valid_date_count,
        positiveStreak: item.pearson_ic_max_positive_streak,
        positiveStart: item.pearson_ic_positive_streak_start,
        positiveEnd: item.pearson_ic_positive_streak_end,
        negativeStreak: item.pearson_ic_max_negative_streak,
        negativeStart: item.pearson_ic_negative_streak_start,
        negativeEnd: item.pearson_ic_negative_streak_end,
      }
})
const icLabel = computed(() => icKind.value === 'rank' ? 'Rank IC' : 'Pearson IC')
const runStateTitle = computed(() => {
  if (run.data.value?.status === 'FAILED') return '本次分析失败'
  if (run.data.value?.status === 'CANCELLED') return '本次分析已取消'
  if (run.data.value?.status === 'RUNNING') return '正在分析'
  return '分析已排队'
})
const runStateHint = computed(() => {
  if (run.data.value?.status === 'FAILED') return '本次分析未生成结果，可前往任务与日志查看原因并安全重试。'
  if (run.data.value?.status === 'CANCELLED') return '没有发布新的结果；需要时可再次运行分析。'
  if (run.data.value?.status === 'RUNNING') return '正在计算因子、未来收益和统计指标，完成后会自动显示结果。'
  return '分析将在后台开始，页面会自动显示最新进度和结果。'
})

const metric = (value: unknown, digits: number, scale = 1, suffix = '') => {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—'
  return `${(Number(value) * scale).toFixed(digits)}${suffix}`
}
const streakRange = (start: string | null | undefined, end: string | null | undefined) => {
  if (!start || !end) return '无连续区间'
  return start === end ? start : `${start} 至 ${end}`
}
const quantileAverages = computed(() => [1, 2, 3, 4, 5].map((q) => {
  const rows = series.data.value?.quantile_returns.filter((item) => Number(item.quantile) === q && !item.is_empty) ?? []
  const values = rows.map((item) => Number(item.mean_return)).filter(Number.isFinite)
  return values.length ? values.reduce((a, b) => a + b, 0) / values.length : null
}))
const icRows = computed(() => series.data.value?.ic ?? [])
const icOption = computed(() => ({
  tooltip,
  grid: { left: 48, right: 18, top: 16, bottom: 42 },
  dataZoom: [{ type: 'inside' }],
  legend: { data: ['日度 IC', '20日滚动'], top: 0, textStyle: { color: '#64748b' } },
  xAxis: { type: 'category', boundaryGap: false, data: icRows.value.map((item) => item.signal_date), ...axis, splitLine: { show: false } },
  yAxis: { type: 'value', min: -1, max: 1, ...axis },
  series: [
    { name: '日度 IC', type: 'line', showSymbol: false, connectNulls: false, lineStyle: { color: '#087f79', width: 1.5 }, data: icRows.value.map((item) => item.is_valid ? (icKind.value === 'rank' ? item.rank_ic : item.pearson_ic) : null) },
    { name: '20日滚动', type: 'line', showSymbol: false, connectNulls: false, lineStyle: { color: '#2563eb', width: 2.5 }, data: icRows.value.map((item) => icKind.value === 'rank' ? item.rank_ic_rolling_mean : item.pearson_ic_rolling_mean) },
  ],
}))
const cumulativeIcOption = computed(() => ({
  tooltip,
  grid: { left: 52, right: 18, top: 16, bottom: 42 },
  dataZoom: [{ type: 'inside' }],
  xAxis: { type: 'category', boundaryGap: false, data: icRows.value.map((item) => item.signal_date), ...axis, splitLine: { show: false } },
  yAxis: { type: 'value', ...axis },
  series: [{ type: 'line', showSymbol: false, connectNulls: false, lineStyle: { color: '#7c3aed', width: 2 }, areaStyle: { color: 'rgba(124,58,237,.08)' }, data: icRows.value.map((item) => icKind.value === 'rank' ? item.rank_ic_cumulative_sum : item.pearson_ic_cumulative_sum) }],
}))
const icDecayRows = computed(() => (run.data.value?.summary ?? [])
  .filter((item) => item.factor_ref === factorRef.value && item.signal_variant === signalVariant.value)
  .sort((left, right) => Number(left.horizon) - Number(right.horizon)))
const icDecayOption = computed(() => ({
  tooltip,
  grid: { left: 48, right: 18, top: 16, bottom: 36 },
  xAxis: { type: 'category', data: icDecayRows.value.map((item) => `${item.horizon}日`), ...axis, splitLine: { show: false } },
  yAxis: { type: 'value', min: -1, max: 1, ...axis },
  series: [{ type: 'line', showSymbol: true, symbolSize: 8, lineStyle: { color: '#d97706', width: 2 }, itemStyle: { color: '#d97706' }, data: icDecayRows.value.map((item) => icKind.value === 'rank' ? item.rank_ic_mean : item.pearson_ic_mean) }],
}))
const quantileOption = computed(() => ({
  tooltip,
  grid: { left: 50, right: 18, top: 16, bottom: 36 },
  xAxis: { type: 'category', data: ['Q1', 'Q2', 'Q3', 'Q4', 'Q5'], ...axis, splitLine: { show: false } },
  yAxis: { type: 'value', ...axis },
  series: [{ type: 'bar', itemStyle: { color: '#2563eb', borderRadius: [4, 4, 0, 0] }, data: quantileAverages.value }],
}))
const longShortOption = computed(() => ({
  tooltip,
  grid: { left: 48, right: 18, top: 16, bottom: 42 },
  xAxis: { type: 'category', boundaryGap: false, data: series.data.value?.long_short_returns.map((item) => item.signal_date) ?? [], ...axis, splitLine: { show: false } },
  yAxis: { type: 'value', ...axis },
  series: [{ type: 'line', showSymbol: false, lineStyle: { color: '#d63b56', width: 2 }, data: series.data.value?.long_short_returns.map((item) => item.is_valid ? item.long_short_return : null) ?? [] }],
}))
const correlationOption = computed(() => {
  const refs = run.data.value?.config.factor_refs ?? []
  const index = new Map(refs.map((item, i) => [item, i]))
  return {
    tooltip,
    grid: { left: 120, right: 24, top: 20, bottom: 90 },
    xAxis: { type: 'category', data: refs, ...axis, axisLabel: { ...axis.axisLabel, rotate: 35 } },
    yAxis: { type: 'category', data: refs, ...axis },
    visualMap: { min: -1, max: 1, calculable: true, orient: 'horizontal', left: 'center', bottom: 4, inRange: { color: ['#2563eb', '#ffffff', '#d63b56'] } },
    series: [{ type: 'heatmap', data: (correlation.data.value?.data ?? []).map((item) => [index.get(String(item.factor_x)), index.get(String(item.factor_y)), item.correlation]) }],
  }
})
</script>

<template>
  <div class="page-stack">
    <section class="panel factor-hero">
      <div>
        <span class="eyebrow">FACTOR RESEARCH</span>
        <h2 class="factor-title">因子研究</h2>
        <p class="factor-hint">集中查看一项研究的最新分析结果，历史运行按需展开。</p>
      </div>
      <div class="factor-actions">
        <el-select v-model="studyId" filterable placeholder="选择研究" class="study-select">
          <el-option v-for="item in studies.data.value?.items ?? []" :key="item.id" :label="item.name" :value="item.id" />
        </el-select>
        <el-button @click="createOpen = true">新建研究</el-button>
        <el-button
          type="primary"
          :disabled="!studyId || analysisActive"
          :loading="startRun.isPending.value || analysisActive"
          @click="startRun.mutate()"
        >
          {{ analysisActive ? '分析中' : '运行分析' }}
        </el-button>
      </div>
    </section>

    <ErrorState v-if="study.isError.value || run.isError.value || series.isError.value || industryCoverage.isError.value" :message="String(study.error.value || run.error.value || series.error.value || industryCoverage.error.value)" />

    <section v-if="selectedStudy" class="panel study-context">
      <div class="study-summary">
        <div>
          <span class="context-label">当前研究</span>
          <strong>{{ selectedStudy.name }}</strong>
        </div>
        <dl>
          <div><dt>研究区间</dt><dd>{{ selectedStudy.config.start_date }} 至 {{ selectedStudy.config.end_date }}</dd></div>
          <div><dt>分析因子</dt><dd>{{ selectedStudy.config.factor_refs.length }} 个</dd></div>
          <div><dt>行业对照</dt><dd>{{ selectedStudy.config.industry ? `${selectedStudy.config.industry.taxonomy} · ${selectedStudy.config.industry.unclassified_policy}` : '未启用' }}</dd></div>
          <div><dt>最新分析</dt><dd>{{ latestRun ? formatTime(latestRun.created_at) : '尚未运行' }}</dd></div>
        </dl>
      </div>
      <div v-if="run.data.value?.status === 'SUCCEEDED'" class="result-controls">
        <div class="result-caption">
          <span>{{ viewingLatest ? '最新结果' : '历史结果' }}</span>
          <small>{{ formatTime(run.data.value.completed_at ?? run.data.value.created_at) }}</small>
        </div>
        <el-select v-model="factorRef" class="factor-select" aria-label="选择因子">
          <el-option v-for="item in run.data.value.config.factor_refs" :key="item" :label="item" :value="item" />
        </el-select>
        <el-radio-group v-model="horizon" aria-label="选择未来收益周期">
          <el-radio-button v-for="item in availableHorizons" :key="item" :value="item">未来{{ item }}日</el-radio-button>
        </el-radio-group>
        <el-radio-group v-if="run.data.value.config.industry" v-model="signalVariant" aria-label="选择信号版本">
          <el-radio-button value="DIRECTION_ADJUSTED">方向调整基线</el-radio-button>
          <el-radio-button value="INDUSTRY_NEUTRALIZED">PIT 行业中性化</el-radio-button>
        </el-radio-group>
        <el-radio-group v-model="icKind" aria-label="选择IC类型">
          <el-radio-button value="rank">Rank IC</el-radio-button>
          <el-radio-button value="pearson">Pearson IC</el-radio-button>
        </el-radio-group>
        <span v-if="run.data.value.manifest_hash" class="hash" title="结果内容身份">{{ shortHash(run.data.value.manifest_hash) }}</span>
      </div>
    </section>

    <div v-if="run.data.value && run.data.value.status !== 'SUCCEEDED'" class="run-state panel" :class="{ 'is-failed': run.data.value.status === 'FAILED' }">
      <div class="run-state-copy">
        <span class="run-state-mark" />
        <div><strong>{{ runStateTitle }}</strong><small>{{ runStateHint }}</small></div>
      </div>
      <RouterLink v-if="run.data.value.status === 'FAILED' && run.data.value.task_id" :to="{ path: '/tasks', query: { task: run.data.value.task_id } }">
        <el-button type="danger" plain>查看失败原因</el-button>
      </RouterLink>
    </div>

    <template v-else-if="series.data.value">
      <div class="metrics-grid">
        <article class="metric-card tone-cyan"><div class="metric-top"><span>{{ icLabel }} 均值</span><i /></div><strong class="metric-value">{{ metric(icStats?.mean, 4) }}</strong><p>有效信号日截面相关</p></article>
        <article class="metric-card"><div class="metric-top"><span>{{ icLabel }} P50</span><i /></div><strong class="metric-value">{{ metric(icStats?.median, 4) }}</strong><p>日度 IC 中位数</p></article>
        <article class="metric-card"><div class="metric-top"><span>未年化 ICIR</span><i /></div><strong class="metric-value">{{ metric(icStats?.icir, 3) }}</strong><p>IC均值 / 样本标准差</p></article>
        <article class="metric-card tone-green"><div class="metric-top"><span>IC 正值比例</span><i /></div><strong class="metric-value">{{ metric(icStats?.positiveRate, 1, 100, '%') }}</strong><p>零值不计为正</p></article>
        <article class="metric-card"><div class="metric-top"><span>有效日期</span><i /></div><strong class="metric-value">{{ metric(icStats?.validDates, 0) }}</strong><p>满足最小截面的信号日</p></article>
        <article class="metric-card tone-red"><div class="metric-top"><span>Q5 − Q1</span><i /></div><strong class="metric-value">{{ metric(summary?.long_short_mean, 2, 100, '%') }}</strong><p>未来收益差，不复利</p></article>
      </div>
      <section class="panel streak-panel">
        <div><span>最长连续正 IC</span><strong>{{ icStats?.positiveStreak ?? 0 }} 日</strong><small>{{ streakRange(icStats?.positiveStart, icStats?.positiveEnd) }}</small></div>
        <div><span>最长连续负 IC</span><strong>{{ icStats?.negativeStreak ?? 0 }} 日</strong><small>{{ streakRange(icStats?.negativeStart, icStats?.negativeEnd) }}</small></div>
      </section>
      <div class="dashboard-grid">
        <ChartCard :title="`${icLabel} 时间序列`" subtitle="日度值与最近20个信号日滚动均值"><VChart class="chart" :option="icOption" autoresize /></ChartCard>
        <ChartCard :title="`累计 ${icLabel}`" subtitle="仅累加有效日，无效日承接此前累计值"><VChart class="chart" :option="cumulativeIcOption" autoresize /></ChartCard>
      </div>
      <div class="dashboard-grid">
        <ChartCard :title="`${icLabel} 衰减`" subtitle="比较1、5、20日未来收益窗口的IC均值"><VChart class="chart" :option="icDecayOption" autoresize /></ChartCard>
        <ChartCard title="五分位平均未来收益" subtitle="Q1最低，Q5为方向校正后的最高组"><VChart class="chart" :option="quantileOption" autoresize /></ChartCard>
      </div>
      <div class="dashboard-grid">
        <ChartCard title="Q5 − Q1 多空收益" subtitle="重叠未来窗口不做复利"><VChart class="chart" :option="longShortOption" autoresize /></ChartCard>
        <ChartCard title="因子 Spearman 相关矩阵" subtitle="按日计算后等权平均" :empty="!correlation.data.value?.data.length"><VChart class="chart" :option="correlationOption" autoresize /></ChartCard>
      </div>
      <section class="panel table-panel">
        <header class="panel-heading"><div><h2>覆盖率与样本质量</h2><p>样本不足30只时保留原因，不填充为零</p></div></header>
        <el-table :data="series.data.value.coverage" height="360">
          <el-table-column prop="signal_date" label="信号日" />
          <el-table-column prop="eligible_count" label="股票池" align="right" />
          <el-table-column prop="valid_count" label="有效样本" align="right" />
          <el-table-column label="覆盖率" align="right"><template #default="scope">{{ (Number(scope.row.coverage) * 100).toFixed(1) }}%</template></el-table-column>
          <el-table-column prop="quality_reason" label="质量原因" />
        </el-table>
      </section>
      <section v-if="run.data.value?.config.industry" class="panel table-panel">
        <header class="panel-heading"><div><h2>PIT 行业覆盖</h2><p>按信号日披露已分类、明确未分类和没有历史状态的股票</p></div></header>
        <el-table :data="industryCoverage.data.value?.data ?? []" height="360">
          <el-table-column prop="signal_date" label="信号日" />
          <el-table-column prop="eligible_count" label="股票池" align="right" />
          <el-table-column prop="classified_count" label="已分类" align="right" />
          <el-table-column prop="tombstone_count" label="明确未分类" align="right" />
          <el-table-column prop="missing_state_count" label="无历史状态" align="right" />
          <el-table-column prop="usable_count" label="策略后可用" align="right" />
          <el-table-column label="分类覆盖率" align="right"><template #default="scope">{{ metric(scope.row.classified_coverage, 1, 100, '%') }}</template></el-table-column>
          <el-table-column label="可用率" align="right"><template #default="scope">{{ metric(scope.row.usable_coverage, 1, 100, '%') }}</template></el-table-column>
        </el-table>
      </section>
    </template>

    <div v-else-if="selectedStudy && !runId" class="empty-state panel">
      <strong>这项研究还没有分析结果</strong>
      <small>点击“运行分析”，完成后会在这里直接展示最新结果。</small>
    </div>
    <div v-else-if="!studyId && !studies.isLoading.value" class="empty-state panel">
      <strong>创建第一项因子研究</strong>
      <small>确定因子和日期范围后，可随时运行并比较历史结果。</small>
      <el-button type="primary" class="empty-action" @click="createOpen = true">新建研究</el-button>
    </div>

    <section v-if="runs.length" class="panel run-history">
      <el-collapse v-model="runHistoryOpen">
        <el-collapse-item name="history">
          <template #title>
            <div class="history-title"><strong>运行历史</strong><span>{{ runs.length }} 次分析</span></div>
          </template>
          <div class="history-list">
            <article v-for="item in runs" :key="item.id" class="history-row" :class="{ 'is-selected': item.id === runId }">
              <div class="history-time"><strong>{{ formatTime(item.created_at) }}</strong><small>{{ item.id.slice(0, 8) }}</small></div>
              <StatusBadge :status="item.status" />
              <span class="history-hash">{{ item.manifest_hash ? shortHash(item.manifest_hash) : '未发布结果' }}</span>
              <span class="spacer" />
              <el-button v-if="item.status === 'SUCCEEDED'" text type="primary" @click="runId = item.id">{{ item.id === runId ? '正在查看' : '查看结果' }}</el-button>
              <RouterLink v-else-if="item.status === 'FAILED' && item.task_id" :to="{ path: '/tasks', query: { task: item.task_id } }"><el-button text type="danger">排查失败</el-button></RouterLink>
              <span v-else class="history-note">{{ ['QUEUED', 'RUNNING'].includes(item.status) ? '进行中' : '无结果' }}</span>
            </article>
          </div>
        </el-collapse-item>
      </el-collapse>
    </section>

    <el-dialog v-model="createOpen" title="新建因子研究" width="560px">
      <el-form label-position="top">
        <el-form-item label="研究名称"><el-input v-model="form.name" placeholder="例如：价值与质量因子 2024" /></el-form-item>
        <el-form-item label="研究区间"><el-date-picker v-model="form.dates" type="daterange" value-format="YYYY-MM-DD" start-placeholder="开始日期" end-placeholder="结束日期" style="width:100%" /></el-form-item>
        <el-form-item label="因子"><el-select v-model="form.factor_refs" multiple filterable style="width:100%"><el-option v-for="item in catalog.data.value?.items ?? []" :key="item.factor_ref" :label="`${item.name} · ${item.factor_ref}`" :value="item.factor_ref" /></el-select></el-form-item>
        <el-form-item label="PIT 行业中性化对照">
          <el-switch v-model="form.industry_enabled" active-text="同时分析行业中性化信号" />
        </el-form-item>
        <template v-if="form.industry_enabled">
          <el-form-item label="行业分类体系"><el-input :model-value="catalog.data.value?.industry.taxonomy ?? '证监会行业分类'" disabled /></el-form-item>
          <el-form-item label="未分类样本">
            <el-radio-group v-model="form.unclassified_policy">
              <el-radio-button value="EXCLUDE">排除</el-radio-button>
              <el-radio-button value="UNCLASSIFIED">归入未分类组</el-radio-button>
            </el-radio-group>
          </el-form-item>
        </template>
      </el-form>
      <template #footer><el-button @click="createOpen = false">取消</el-button><el-button type="primary" :disabled="!form.name || form.dates.length !== 2 || !form.factor_refs.length" :loading="createStudy.isPending.value" @click="createStudy.mutate()">创建研究</el-button></template>
    </el-dialog>
  </div>
</template>

<style scoped>
.factor-hero { display: flex; align-items: center; gap: 24px; justify-content: space-between; }
.factor-actions { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
.study-select { width: 280px; }
.study-context { display: flex; align-items: center; gap: 24px; justify-content: space-between; padding-top: 15px; padding-bottom: 15px; }
.study-summary { display: flex; align-items: center; gap: 30px; min-width: 0; }
.study-summary > div { min-width: 180px; }
.study-summary strong { display: block; margin-top: 5px; font-size: 15px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.context-label { color: var(--dim); font-size: 10px; letter-spacing: .08em; }
.study-summary dl { display: flex; align-items: center; gap: 28px; margin: 0; }
.study-summary dl div { display: grid; gap: 4px; }
.study-summary dt { color: var(--dim); font-size: 10px; }
.study-summary dd { margin: 0; color: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; white-space: nowrap; }
.result-controls { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
.result-caption { display: grid; gap: 3px; margin-right: 4px; text-align: right; }
.result-caption span { color: var(--success); font-size: 11px; font-weight: 700; }
.result-caption small { color: var(--dim); font-size: 9px; }
.factor-select { width: 190px; }
.streak-panel { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1px; padding: 0; overflow: hidden; background: var(--border); }
.streak-panel > div { display: grid; grid-template-columns: auto auto 1fr; align-items: center; gap: 12px; min-height: 64px; padding: 14px 18px; background: var(--panel); }
.streak-panel span { color: var(--muted); font-size: 11px; }
.streak-panel strong { font-size: 17px; font-variant-numeric: tabular-nums; }
.streak-panel small { color: var(--dim); font-size: 10px; text-align: right; }
.run-state { min-height: 118px; display: flex; align-items: center; justify-content: space-between; gap: 18px; padding: 24px; }
.run-state-copy { display: flex; align-items: center; gap: 14px; }
.run-state-mark { width: 10px; height: 10px; border-radius: 50%; background: var(--blue); box-shadow: 0 0 0 6px rgba(37, 99, 235, .09); }
.run-state.is-failed .run-state-mark { background: var(--danger); box-shadow: 0 0 0 6px rgba(214, 59, 86, .09); }
.run-state-copy strong, .run-state-copy small { display: block; }
.run-state-copy strong { font-size: 14px; }
.run-state-copy small { margin-top: 6px; color: var(--dim); font-size: 11px; }
.empty-action { margin-top: 16px; }
.run-history { padding-top: 6px; padding-bottom: 6px; }
.run-history :deep(.el-collapse) { border: 0; }
.run-history :deep(.el-collapse-item__header) { height: 48px; border: 0; background: transparent; }
.run-history :deep(.el-collapse-item__wrap) { border: 0; }
.run-history :deep(.el-collapse-item__content) { padding-bottom: 12px; }
.history-title { display: flex; align-items: center; gap: 9px; }
.history-title strong { font-size: 13px; }
.history-title span { color: var(--dim); font-size: 10px; font-weight: 400; }
.history-list { display: grid; border-top: 1px solid var(--border); }
.history-row { min-height: 52px; display: flex; align-items: center; gap: 18px; padding: 7px 4px 7px 10px; border-bottom: 1px solid var(--border); }
.history-row.is-selected { background: rgba(37, 99, 235, .035); }
.history-time { width: 115px; display: grid; gap: 3px; }
.history-time strong { font-size: 11px; font-variant-numeric: tabular-nums; }
.history-time small, .history-hash { color: var(--dim); font: 9px ui-monospace, Consolas, monospace; }
.history-hash { width: 100px; }
.history-note { color: var(--dim); font-size: 11px; }
.history-row .spacer { flex: 1; }
@media (max-width: 1360px) {
  .factor-hero, .study-context { align-items: flex-start; flex-direction: column; }
  .factor-actions, .result-controls { width: 100%; justify-content: flex-start; }
  .study-summary { width: 100%; }
  .streak-panel { grid-template-columns: 1fr; }
}
</style>
