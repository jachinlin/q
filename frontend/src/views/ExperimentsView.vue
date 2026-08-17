<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed, reactive, ref, watch } from 'vue'
import VChart from 'vue-echarts'
import { useRouter } from 'vue-router'

import { api } from '../api'
import '../charts'
import ErrorState from '../components/ErrorState.vue'
import MetricCard from '../components/MetricCard.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { EXPERIMENT_TEMPLATES } from '../experimentTemplates'
import type { ExperimentTemplateId } from '../experimentTemplates'
import { formatNumber, formatPercent, formatTime } from '../format'
import type { Experiment, Page } from '../types'

type StatusKey = 'CREATED' | 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED'

const STATUS_FILTERS: Array<{ value: '' | StatusKey; label: string }> = [
  { value: '', label: '全部' },
  { value: 'RUNNING', label: '运行中' },
  { value: 'QUEUED', label: '排队' },
  { value: 'SUCCEEDED', label: '成功' },
  { value: 'FAILED', label: '失败' },
]

const router = useRouter()
const client = useQueryClient()
const page = ref(1)
const filters = reactive({ status: '', strategy: '', mark: '' })
const submitOpen = ref(false)
const templateId = ref<ExperimentTemplateId>('etf_rotation')
const configYaml = ref(EXPERIMENT_TEMPLATES.etf_rotation.yaml)
const queryKey = computed(() => ['experiments', page.value, filters.status, filters.strategy, filters.mark])
const list = useQuery({
  queryKey,
  queryFn: () => {
    const params = new URLSearchParams({ page: String(page.value), page_size: '25' })
    if (filters.status) params.set('status', filters.status)
    if (filters.strategy) params.set('strategy_id', filters.strategy)
    if (filters.mark) params.set('research_mark', filters.mark)
    return api.get<Page<Experiment>>(`/api/v1/experiments?${params}`)
  },
})
const submitExperiment = useMutation({
  mutationFn: () => api.post<{ experiment_id: string; task_id: string; status: string }>('/api/v1/experiments', { config_yaml: configYaml.value }),
  onSuccess: async (result) => {
    submitOpen.value = false
    ElMessage.success(`实验已提交 · ${result.experiment_id.slice(0, 8)}`)
    await client.invalidateQueries({ queryKey: ['experiments'] })
    await router.push({ name: 'experiment-detail', params: { experimentId: result.experiment_id }, query: { tab: 'overview' } })
  },
  onError: (error) => ElMessage.error(`提交失败：${String(error)}`),
})
const deletingId = ref('')
const removeExperiment = useMutation({
  mutationFn: (id: string) => api.delete<{ experiment_id: string; status: 'DELETED' }>(`/api/v1/experiments/${id}`),
  onMutate: (id) => {
    deletingId.value = id
  },
  onSuccess: async (result) => {
    ElMessage.success(`已删除实验：${result.experiment_id.slice(0, 8)}`)
    client.removeQueries({ queryKey: ['experiment', result.experiment_id] })
    if (page.value > 1 && experiments.value.length === 1) page.value -= 1
    await Promise.all([
      client.invalidateQueries({ queryKey: ['experiments'] }),
      client.invalidateQueries({ queryKey: ['tasks'] }),
    ])
  },
  onError: (error) => ElMessage.error(`删除失败：${String(error)}`),
  onSettled: () => {
    deletingId.value = ''
  },
})

const experiments = computed(() => list.data.value?.items ?? [])
const hasFilters = computed(() => Boolean(filters.status || filters.strategy || filters.mark))
const activeCount = computed(() => experiments.value.filter((item) => ['CREATED', 'QUEUED', 'RUNNING'].includes(item.status)).length)
const succeededCount = computed(() => experiments.value.filter((item) => item.status === 'SUCCEEDED').length)
const closedCount = computed(() => experiments.value.filter((item) => ['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(item.status)).length)
const successRate = computed(() => closedCount.value ? succeededCount.value / closedCount.value : null)
const candidateCount = computed(() => experiments.value.filter((item) => item.research_mark === 'CANDIDATE').length)
const baselineCount = computed(() => experiments.value.filter((item) => item.research_mark === 'BASELINE').length)
const unreviewedCount = computed(() => experiments.value.filter((item) => item.research_mark === 'UNREVIEWED').length)
const bestExperiment = computed(() => experiments.value
  .filter((item) => item.status === 'SUCCEEDED' && Number.isFinite(item.metrics.sharpe_ratio))
  .sort((left, right) => Number(right.metrics.sharpe_ratio) - Number(left.metrics.sharpe_ratio))[0] ?? null)
const latestExperiment = computed(() => experiments.value[0] ?? null)

const statusBreakdown = computed(() => {
  const counts = new Map<string, number>()
  for (const item of experiments.value) counts.set(item.status, (counts.get(item.status) ?? 0) + 1)
  return [
    { status: 'SUCCEEDED', label: '已成功', value: counts.get('SUCCEEDED') ?? 0, color: '#16825f' },
    { status: 'RUNNING', label: '运行中', value: counts.get('RUNNING') ?? 0, color: '#2563eb' },
    { status: 'QUEUED', label: '等待执行', value: counts.get('QUEUED') ?? 0, color: '#6d5bd0' },
    { status: 'FAILED', label: '需关注', value: counts.get('FAILED') ?? 0, color: '#d63b56' },
  ]
})

const trendOption = computed(() => {
  const buckets = new Map<string, { succeeded: number; active: number; failed: number }>()
  for (const item of [...experiments.value].reverse()) {
    const date = item.created_at.slice(0, 10)
    const bucket = buckets.get(date) ?? { succeeded: 0, active: 0, failed: 0 }
    if (item.status === 'SUCCEEDED') bucket.succeeded += 1
    else if (item.status === 'FAILED' || item.status === 'CANCELLED') bucket.failed += 1
    else bucket.active += 1
    buckets.set(date, bucket)
  }
  const dates = [...buckets.keys()].sort().slice(-10)
  return {
    animationDuration: 500,
    grid: { left: 8, right: 10, top: 24, bottom: 6, containLabel: true },
    tooltip: { trigger: 'axis', backgroundColor: '#172033', borderWidth: 0, textStyle: { color: '#fff', fontSize: 11 } },
    legend: { top: 0, right: 0, itemWidth: 9, itemHeight: 9, textStyle: { color: '#7d8da3', fontSize: 10 } },
    xAxis: { type: 'category', data: dates.map((date) => date.slice(5)), axisLine: { lineStyle: { color: '#d8e2ee' } }, axisTick: { show: false }, axisLabel: { color: '#7d8da3', fontSize: 10 } },
    yAxis: { type: 'value', minInterval: 1, splitLine: { lineStyle: { color: '#edf1f6' } }, axisLabel: { color: '#93a0b2', fontSize: 10 } },
    series: [
      { name: '成功', type: 'bar', stack: 'total', data: dates.map((date) => buckets.get(date)?.succeeded ?? 0), barMaxWidth: 24, itemStyle: { color: '#16825f', borderRadius: [0, 0, 3, 3] } },
      { name: '进行中', type: 'bar', stack: 'total', data: dates.map((date) => buckets.get(date)?.active ?? 0), barMaxWidth: 24, itemStyle: { color: '#2563eb' } },
      { name: '未完成', type: 'bar', stack: 'total', data: dates.map((date) => buckets.get(date)?.failed ?? 0), barMaxWidth: 24, itemStyle: { color: '#d63b56', borderRadius: [3, 3, 0, 0] } },
    ],
  }
})

watch(() => [filters.status, filters.strategy, filters.mark], () => {
  page.value = 1
})

function openSubmit() {
  templateId.value = 'etf_rotation'
  configYaml.value = EXPERIMENT_TEMPLATES.etf_rotation.yaml
  submitOpen.value = true
}

async function selectTemplate(next: ExperimentTemplateId) {
  if (next === templateId.value) return
  const current = EXPERIMENT_TEMPLATES[templateId.value].yaml
  if (configYaml.value !== current) {
    try {
      await ElMessageBox.confirm('切换模板会覆盖当前编辑内容。', '确认切换策略模板', { type: 'warning' })
    } catch {
      return
    }
  }
  templateId.value = next
  configYaml.value = EXPERIMENT_TEMPLATES[next].yaml
}

function openDetail(row: Experiment, tab = 'overview') {
  void router.push({ name: 'experiment-detail', params: { experimentId: row.id }, query: { tab } })
}

function canDelete(row: Experiment) {
  return !['QUEUED', 'RUNNING'].includes(row.status)
}

async function deleteExperiment(row: Experiment) {
  if (!canDelete(row)) return
  try {
    await ElMessageBox.confirm(
      '将永久删除该实验、关联任务、实验产物和任务日志；删除审计仍会保留。此操作不可撤销。',
      '确认删除实验',
      { type: 'error', confirmButtonText: '删除', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  removeExperiment.mutate(row.id)
}

function resetFilters() {
  filters.status = ''
  filters.strategy = ''
  filters.mark = ''
}

function strategyLabel(value: string) {
  if (value === 'etf_rotation') return 'ETF 轮动'
  if (value === 'stock_multifactor') return '股票多因子'
  return value.replaceAll('_', ' ')
}

function strategyInitials(value: string) {
  return value.split('_').map((part) => part[0]?.toUpperCase()).join('').slice(0, 2)
}
</script>

<template>
  <div class="page-stack experiment-center">
    <ErrorState v-if="list.isError.value" :message="String(list.error.value)" />
    <template v-else>
      <section class="panel experiment-heading">
        <div>
          <span class="eyebrow">EXPERIMENT RESEARCH</span>
          <h2 class="experiment-title">实验研究</h2>
          <p class="experiment-hint">提交、筛选并复核实验结果，将可靠方案沉淀为研究基线。</p>
        </div>
        <el-button type="primary" @click="openSubmit">提交实验</el-button>
      </section>

      <div class="metrics-grid" aria-label="当前结果摘要">
        <MetricCard label="匹配实验" :value="list.data.value?.total ?? 0" :hint="hasFilters ? '已应用筛选条件' : '登记册全部记录'" />
        <MetricCard label="闭环成功率" :value="formatPercent(successRate)" :hint="`当前页 ${succeededCount} / ${closedCount} 次闭环`" tone="green" />
        <MetricCard label="正在推进" :value="activeCount" hint="创建、排队与运行中的实验" tone="cyan" />
        <MetricCard label="研究候选" :value="candidateCount" :hint="`${baselineCount} 个基线 · ${unreviewedCount} 个待评审`" />
      </div>

      <div class="experiment-insights">
        <section class="panel insight-panel">
          <header class="panel-heading">
            <div><span class="section-kicker">RUN VELOCITY</span><h2>实验运行节奏</h2><p>当前页记录按创建日期与结果状态聚合</p></div>
            <span class="scope-pill">最近 {{ experiments.length }} 条</span>
          </header>
          <div v-if="experiments.length" class="trend-wrap"><VChart class="experiment-trend" :option="trendOption" autoresize /></div>
          <div v-else class="mini-empty"><span>⌁</span><strong>等待第一条实验记录</strong><small>提交实验后，这里会呈现运行节奏。</small></div>
        </section>

        <section class="panel research-panel">
          <header class="panel-heading"><div><span class="section-kicker">RESEARCH SIGNAL</span><h2>结果信号</h2><p>快速识别运行状态与当前页领跑者</p></div></header>
          <div class="status-breakdown">
            <button v-for="item in statusBreakdown" :key="item.status" type="button" @click="filters.status = filters.status === item.status ? '' : item.status">
              <span><i :style="{ background: item.color }" />{{ item.label }}</span><strong>{{ item.value }}</strong>
              <em><i :style="{ width: `${experiments.length ? Math.max(8, item.value / experiments.length * 100) : 0}%`, background: item.color }" /></em>
            </button>
          </div>
          <button v-if="bestExperiment" class="leader-card" type="button" @click="openDetail(bestExperiment, 'backtest')">
            <span class="leader-medal">01</span>
            <span class="leader-copy"><small>当前页 SHARPE 领跑</small><strong>{{ strategyLabel(bestExperiment.strategy_id) }}</strong><em>{{ bestExperiment.id.slice(0, 8) }} · 查看回测 →</em></span>
            <span class="leader-score">{{ formatNumber(bestExperiment.metrics.sharpe_ratio) }}</span>
          </button>
          <div v-else class="leader-card leader-empty"><span class="leader-medal">—</span><span class="leader-copy"><small>当前页 SHARPE 领跑</small><strong>暂无成功结果</strong><em>实验完成后自动识别</em></span></div>
        </section>
      </div>

      <section class="panel registry-panel">
        <header class="registry-heading">
          <div><span class="section-kicker">IMMUTABLE REGISTRY</span><h2>实验登记册</h2><p>每条记录绑定配置、数据与代码身份，可直接进入结果工作区。</p></div>
          <div class="registry-meta"><span v-if="latestExperiment">最近更新 {{ formatTime(latestExperiment.created_at) }}</span><strong>{{ list.data.value?.total ?? 0 }} RECORDS</strong></div>
        </header>

        <div class="filter-row">
          <div class="quick-filters" aria-label="实验状态快捷筛选">
            <button v-for="item in STATUS_FILTERS" :key="item.value" type="button" :class="{ active: filters.status === item.value }" @click="filters.status = item.value">{{ item.label }}</button>
          </div>
          <div class="filter-fields">
            <el-input v-model="filters.strategy" clearable placeholder="搜索策略 ID" class="strategy-search" />
            <el-select v-model="filters.mark" clearable placeholder="研究标记" class="mark-filter"><el-option v-for="item in ['UNREVIEWED', 'BASELINE', 'CANDIDATE', 'DISCARDED']" :key="item" :label="item" :value="item" /></el-select>
            <button v-if="hasFilters" class="reset-button" type="button" @click="resetFilters">清除筛选</button>
          </div>
        </div>

        <el-table v-loading="list.isPending.value" :data="experiments" height="560" class="experiment-table" row-key="id">
          <el-table-column label="策略 / 实验" min-width="250">
            <template #default="scope">
              <button class="experiment-link" @click="openDetail(scope.row)">
                <span class="strategy-avatar">{{ strategyInitials(scope.row.strategy_id) }}</span>
                <span class="experiment-identity"><strong>{{ strategyLabel(scope.row.strategy_id) }}</strong><span><code>{{ scope.row.id.slice(0, 8) }}</code><i v-for="tag in scope.row.tags.slice(0, 2)" :key="tag">{{ tag }}</i></span></span>
              </button>
            </template>
          </el-table-column>
          <el-table-column label="运行状态" width="118"><template #default="scope"><StatusBadge :status="scope.row.status" /></template></el-table-column>
          <el-table-column label="研究结论" width="128"><template #default="scope"><StatusBadge :status="scope.row.research_mark" /></template></el-table-column>
          <el-table-column label="累计收益" width="112" align="right"><template #default="scope"><span class="metric-cell" :class="Number(scope.row.metrics.cumulative_return) >= 0 ? 'up' : 'down'"><strong>{{ formatPercent(scope.row.metrics.cumulative_return) }}</strong><small>RETURN</small></span></template></el-table-column>
          <el-table-column label="Sharpe" width="96" align="right"><template #default="scope"><span class="metric-cell"><strong>{{ formatNumber(scope.row.metrics.sharpe_ratio) }}</strong><small>RISK ADJ.</small></span></template></el-table-column>
          <el-table-column label="最大回撤" width="112" align="right"><template #default="scope"><span class="metric-cell"><strong>{{ formatPercent(scope.row.metrics.max_drawdown) }}</strong><small>DRAWDOWN</small></span></template></el-table-column>
          <el-table-column label="创建时间" width="128"><template #default="scope"><span class="time-cell">{{ formatTime(scope.row.created_at) }}</span></template></el-table-column>
          <el-table-column label="" width="176" fixed="right">
            <template #default="scope">
              <div class="row-actions">
                <button class="row-action" type="button" @click="openDetail(scope.row, scope.row.status === 'SUCCEEDED' ? 'backtest' : 'overview')">{{ scope.row.status === 'SUCCEEDED' ? '查看结果' : '查看详情' }} <span>→</span></button>
                <button
                  class="row-delete"
                  type="button"
                  :disabled="!canDelete(scope.row) || deletingId === scope.row.id"
                  :title="canDelete(scope.row) ? '永久删除实验' : '运行中或排队中的实验不能删除'"
                  @click="deleteExperiment(scope.row)"
                >{{ deletingId === scope.row.id ? '删除中' : '删除' }}</button>
              </div>
            </template>
          </el-table-column>
        </el-table>
        <footer class="registry-footer"><span>第 {{ page }} 页 · 每页 25 条</span><el-pagination v-model:current-page="page" :page-size="25" :total="list.data.value?.total ?? 0" layout="prev, pager, next" /></footer>
      </section>
    </template>

    <el-dialog v-model="submitOpen" title="提交新实验" width="760px" destroy-on-close>
      <div class="template-switcher">
        <button v-for="(item, id) in EXPERIMENT_TEMPLATES" :key="id" type="button" :class="{ active: templateId === id }" @click="selectTemplate(id)"><strong>{{ item.name }}</strong><span>{{ item.description }}</span></button>
      </div>
      <el-form label-position="top"><el-form-item label="实验 YAML"><el-input v-model="configYaml" class="yaml-editor" type="textarea" :rows="22" resize="vertical" maxlength="1048576" show-word-limit /></el-form-item></el-form>
      <p class="submit-hint">提交时会校验当前数据门，并绑定数据、源码、依赖和交易规则身份。任务由独立 Worker 执行。</p>
      <template #footer><el-button @click="submitOpen = false">取消</el-button><el-button type="primary" :disabled="!configYaml.trim()" :loading="submitExperiment.isPending.value" @click="submitExperiment.mutate()">提交并运行</el-button></template>
    </el-dialog>

  </div>
</template>

<style scoped>
.experiment-center { --violet: #6d5bd0; }
.experiment-heading { display: flex; align-items: center; justify-content: space-between; gap: 24px; }.experiment-title { margin: 5px 0 0; font-size: 16px; }.experiment-hint { margin: 6px 0 0; color: var(--dim); font-size: 11px; }
.section-kicker { color: #426b9d; font-size: 9px; font-weight: 800; letter-spacing: .18em; }
.experiment-insights { display: grid; grid-template-columns: minmax(0, 1.55fr) minmax(320px, .85fr); gap: 14px; }
.insight-panel,.research-panel { min-height: 336px; }.panel-heading .section-kicker { display: block; margin-bottom: 7px; }.scope-pill { padding: 6px 9px; border: 1px solid var(--border); border-radius: 999px; color: var(--dim); background: var(--surface-raised); font-size: 9px; }
.trend-wrap { height: 250px; }.experiment-trend { width: 100%; height: 100%; }.mini-empty { height: 235px; display: grid; place-content: center; text-align: center; }.mini-empty > span { color: #8ba1bd; font-size: 32px; }.mini-empty strong { margin: 8px 0 5px; font-size: 13px; }.mini-empty small { color: var(--dim); }
.status-breakdown { display: grid; grid-template-columns: 1fr 1fr; gap: 9px; }.status-breakdown button { padding: 10px 11px; border: 1px solid #e3eaf2; border-radius: 9px; color: var(--muted); background: #fbfcfe; text-align: left; cursor: pointer; }.status-breakdown button:hover { border-color: #c8d7ea; background: #f7faff; }.status-breakdown button > span { display: inline-flex; align-items: center; gap: 6px; font-size: 9px; }.status-breakdown button > span i { width: 6px; height: 6px; border-radius: 50%; }.status-breakdown button > strong { float: right; color: var(--text); font-size: 13px; }.status-breakdown em { clear: both; height: 3px; display: block; overflow: hidden; margin-top: 8px; border-radius: 2px; background: #edf1f5; }.status-breakdown em i { height: 100%; display: block; border-radius: inherit; transition: width .2s ease; }
.leader-card { width: 100%; min-height: 88px; display: grid; grid-template-columns: 42px 1fr auto; align-items: center; gap: 11px; margin-top: 13px; padding: 12px; border: 1px solid rgba(37,99,235,.16); border-radius: 10px; color: var(--text); background: linear-gradient(105deg, rgba(37,99,235,.07), rgba(109,91,208,.05)); text-align: left; cursor: pointer; }.leader-card:hover { border-color: rgba(37,99,235,.35); transform: translateY(-1px); }.leader-medal { width: 38px; height: 38px; display: grid; place-items: center; border-radius: 10px; color: #fff; background: linear-gradient(145deg, #2563eb, #6d5bd0); font-size: 10px; font-weight: 800; }.leader-copy { min-width: 0; display: grid; gap: 3px; }.leader-copy small { color: #647894; font-size: 8px; font-weight: 700; letter-spacing: .08em; }.leader-copy strong { overflow: hidden; font-size: 12px; text-overflow: ellipsis; white-space: nowrap; }.leader-copy em { color: var(--dim); font-size: 9px; font-style: normal; }.leader-score { color: #1e4fb5; font-size: 24px; font-weight: 700; font-variant-numeric: tabular-nums; }.leader-empty { cursor: default; }.leader-empty:hover { transform: none; }.leader-empty .leader-medal { color: #7788a0; background: #e7edf5; }
.registry-panel { padding: 0; }.registry-heading { display: flex; justify-content: space-between; gap: 20px; padding: 21px 22px 16px; }.registry-heading h2 { margin: 7px 0 5px; font-size: 16px; }.registry-heading p { margin: 0; color: var(--dim); font-size: 10px; }.registry-meta { display: grid; align-content: center; justify-items: end; gap: 5px; color: var(--dim); font-size: 9px; }.registry-meta strong { color: #54708f; font-size: 9px; letter-spacing: .1em; }
.filter-row { min-height: 62px; display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 10px 16px; border-top: 1px solid #e7edf4; border-bottom: 1px solid #e7edf4; background: #f8fafc; }.quick-filters { display: flex; padding: 3px; border: 1px solid var(--border); border-radius: 9px; background: #fff; }.quick-filters button { min-width: 52px; height: 30px; padding: 0 10px; border: 0; border-radius: 6px; color: var(--muted); background: transparent; font-size: 10px; cursor: pointer; }.quick-filters button.active { color: #1d4fb2; background: #eaf1ff; font-weight: 700; box-shadow: 0 1px 3px rgba(37,99,235,.1); }.filter-fields { display: flex; align-items: center; gap: 8px; }.strategy-search { width: 180px; }.mark-filter { width: 145px; }.reset-button { border: 0; color: var(--blue); background: transparent; font-size: 10px; cursor: pointer; }
.experiment-link { display: flex; align-items: center; gap: 11px; padding: 0; border: 0; color: var(--text); background: none; text-align: left; cursor: pointer; }.strategy-avatar { flex: 0 0 34px; width: 34px; height: 34px; display: grid; place-items: center; border: 1px solid #d8e4f2; border-radius: 9px; color: #315e9d; background: linear-gradient(145deg, #f4f8ff, #edf3fb); font-size: 9px; font-weight: 800; letter-spacing: .05em; }.experiment-identity { min-width: 0; display: grid; gap: 5px; }.experiment-identity > strong { overflow: hidden; font-size: 12px; text-overflow: ellipsis; white-space: nowrap; }.experiment-link:hover .experiment-identity > strong { color: var(--blue); }.experiment-identity > span { display: flex; align-items: center; gap: 5px; }.experiment-identity code { color: #55769d; font-size: 9px; }.experiment-identity i { max-width: 68px; overflow: hidden; padding: 2px 5px; border-radius: 4px; color: #73839a; background: #eef2f7; font-size: 8px; font-style: normal; text-overflow: ellipsis; white-space: nowrap; }
.metric-cell { display: inline-grid; justify-items: end; gap: 2px; color: var(--text); font-variant-numeric: tabular-nums; }.metric-cell strong { font-size: 11px; }.metric-cell small { color: #96a2b1; font-size: 7px; letter-spacing: .06em; }.metric-cell.up strong { color: var(--up); }.metric-cell.down strong { color: var(--down); }.time-cell { color: #64758b; font-size: 10px; font-variant-numeric: tabular-nums; }.row-actions { display: flex; align-items: center; justify-content: flex-end; gap: 6px; }.row-action,.row-delete { padding: 7px 9px; border: 1px solid #dce5f0; border-radius: 7px; background: #fff; font-size: 9px; cursor: pointer; }.row-action { color: #315e9d; }.row-action:hover { border-color: #9db8db; background: #f4f8ff; }.row-action span { margin-left: 3px; }.row-delete { color: #c23b52; }.row-delete:hover:not(:disabled) { border-color: #e1a2ad; background: #fff5f6; }.row-delete:disabled { color: #aab4c1; background: #f6f8fa; cursor: not-allowed; }
.registry-footer { height: 58px; display: flex; align-items: center; justify-content: space-between; padding: 0 17px; border-top: 1px solid #e7edf4; color: var(--dim); font-size: 9px; }.experiment-table :deep(.el-table__row td.el-table__cell) { height: 58px; }.experiment-table :deep(th.el-table__cell) { height: 42px; }
.template-switcher { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 18px; }.template-switcher button { display: grid; gap: 5px; padding: 13px 14px; border: 1px solid var(--border); border-radius: 9px; color: var(--text); background: var(--surface-raised); text-align: left; cursor: pointer; }.template-switcher button.active { border-color: rgba(37, 99, 235, .5); background: rgba(37, 99, 235, .06); }.template-switcher span { color: var(--dim); font-size: 10px; }.yaml-editor :deep(textarea) { color: #33445a; font: 11px/1.65 ui-monospace, Consolas, monospace; tab-size: 2; }.submit-hint { margin: -4px 0 0; color: var(--dim); font-size: 10px; }
@media (max-width: 1360px) { .experiment-insights { grid-template-columns: 1fr; }.filter-row { align-items: flex-start; flex-direction: column; }.filter-fields { width: 100%; }.filter-fields .strategy-search { flex: 1; } }
@media (prefers-reduced-motion: reduce) { .leader-card:hover { transform: none; } }
</style>
