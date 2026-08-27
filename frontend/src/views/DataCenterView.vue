<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'

import { api, DashboardApiError } from '../api'
import DataTaskProgress from '../components/DataTaskProgress.vue'
import ErrorState from '../components/ErrorState.vue'
import MetricCard from '../components/MetricCard.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatDate, formatNumber, formatTime, shortHash } from '../format'
import type {
  DataSummary,
  DataUpdatePlan,
  DataUpdateWindow,
  Dataset,
  DatasetDetail,
  Page,
  QualityRun,
  QualityRunDetail,
  QualityRuleResult,
  Task,
} from '../types'

const client = useQueryClient()
const router = useRouter()
const datasetFilter = ref('')
const freshnessFilter = ref('')
const qualityStatus = ref('')
const qualityDataset = ref('')
const qualitySeverity = ref('')
const qualityRule = ref('')
const detailQualityDataset = ref('')
const detailQualityStatus = ref('')
const detailQualityRule = ref('')
const selectedDataset = ref<string | null>(null)
const selectedQualityRun = ref<string | null>(null)
const bootstrapDialog = ref(false)
const bootstrapYears = ref(5)
const updateDialog = ref(false)
const qualityRunDialog = ref(false)
const qualityRunDataset = ref('ALL')
const updateMode = ref<'AUTO_INCREMENTAL' | 'EXPLICIT'>('AUTO_INCREMENTAL')
const dateRange = ref<[string, string] | null>(null)
const selectedUpdateDatasets = ref<string[]>([])
const trackedTaskId = ref<string | null>(null)
const trackedTaskKind = ref<'BOOTSTRAP' | 'UPDATE' | 'QUALITY'>('UPDATE')
const qualityResultStatuses = ['PASS', 'FAIL', 'SKIPPED', 'UNKNOWN'] as const

const summary = useQuery({
  queryKey: ['data-summary'],
  queryFn: () => api.get<DataSummary>('/api/v1/data/summary'),
  refetchInterval: 15_000,
})
const datasets = useQuery({
  queryKey: ['data-datasets'],
  queryFn: () => api.get<{ items: Dataset[] }>('/api/v1/data/datasets'),
})
const qualityQuery = computed(() => {
  const query = new URLSearchParams({ page: '1', page_size: '50' })
  if (qualityStatus.value) query.set('status', qualityStatus.value)
  if (qualityDataset.value) query.set('dataset', qualityDataset.value)
  if (qualitySeverity.value) query.set('severity', qualitySeverity.value)
  if (qualityRule.value) query.set('rule', qualityRule.value)
  return query.toString()
})
const qualityRuns = useQuery({
  queryKey: computed(() => ['data-quality-runs', qualityQuery.value]),
  queryFn: () => api.get<Page<QualityRun>>(`/api/v1/data/quality-runs?${qualityQuery.value}`),
})
const datasetDetail = useQuery({
  queryKey: computed(() => ['data-dataset', selectedDataset.value]),
  queryFn: () => api.get<DatasetDetail>(`/api/v1/data/datasets/${selectedDataset.value}`),
  enabled: computed(() => selectedDataset.value !== null),
})
const qualityDetail = useQuery({
  queryKey: computed(() => ['data-quality-run', selectedQualityRun.value]),
  queryFn: () => api.get<QualityRunDetail>(`/api/v1/data/quality-runs/${selectedQualityRun.value}`),
  enabled: computed(() => selectedQualityRun.value !== null),
})
const trackedTask = useQuery({
  queryKey: computed(() => ['task', trackedTaskId.value]),
  queryFn: () => api.get<Task>(`/api/v1/tasks/${trackedTaskId.value}`),
  enabled: computed(() => trackedTaskId.value !== null),
  refetchInterval: (query) => {
    const status = (query.state.data as Task | undefined)?.status
    return status && ['SUCCEEDED', 'FAILED', 'CANCELLED', 'ORPHANED'].includes(status) ? false : 3_000
  },
})
const updateDatasetOptions = computed(() => (datasets.data.value?.items ?? [])
  .map((item) => item.dataset)
  .sort((left, right) => left.localeCompare(right)))
const normalizedUpdateDatasets = computed(() => [...selectedUpdateDatasets.value]
  .sort((left, right) => left.localeCompare(right)))
const updatePlanRequest = computed(() => ({
  datasets: normalizedUpdateDatasets.value,
  ...(updateMode.value === 'EXPLICIT' && dateRange.value
    ? { start: dateRange.value[0], end: dateRange.value[1] }
    : {}),
}))
const canPreviewUpdate = computed(() => updateDialog.value && (
  updateMode.value === 'AUTO_INCREMENTAL' || dateRange.value !== null
) && normalizedUpdateDatasets.value.length > 0)
const updatePlan = useQuery({
  queryKey: computed(() => [
    'data-update-plan',
    updateMode.value,
    normalizedUpdateDatasets.value.join(','),
    ...(dateRange.value ?? []),
  ]),
  queryFn: () => api.post<DataUpdatePlan>('/api/v1/data/update-plans/preview', updatePlanRequest.value),
  enabled: canPreviewUpdate,
  retry: false,
  staleTime: 0,
})

const visibleDatasets = computed(() => (datasets.data.value?.items ?? []).filter((item) => {
  const matchesDataset = !datasetFilter.value || item.dataset.includes(datasetFilter.value.trim())
  const matchesFreshness = !freshnessFilter.value || item.freshness.status === freshnessFilter.value
  return matchesDataset && matchesFreshness
}))
const datasetDrawerOpen = computed({
  get: () => selectedDataset.value !== null,
  set: (open: boolean) => { if (!open) selectedDataset.value = null },
})
const qualityDrawerOpen = computed({
  get: () => selectedQualityRun.value !== null,
  set: (open: boolean) => { if (!open) selectedQualityRun.value = null },
})
const totalRows = computed(() => (datasets.data.value?.items ?? []).reduce((sum, item) => sum + item.row_count, 0))
const gateRunId = computed(() => summary.data.value?.gate.quality_run_id)
const initializationReady = computed(() => summary.data.value?.initialization.status === 'COMPLETED')
const bootstrapTaskActive = computed(() => {
  const task = summary.data.value?.active_update
  return task?.task_type === 'DATA_BOOTSTRAP'
    && ['QUEUED', 'RUNNING', 'CANCEL_REQUESTED'].includes(task.status)
})
const visibleQualityResults = computed(() => (qualityDetail.data.value?.rule_results ?? []).filter((item) => {
  const ruleFilter = detailQualityRule.value.trim().toLowerCase()
  return (!detailQualityDataset.value || item.dataset === detailQualityDataset.value)
    && (!detailQualityStatus.value || item.status === detailQualityStatus.value)
    && (!ruleFilter || `${item.rule_id} ${item.title}`.toLowerCase().includes(ruleFilter))
}))
const detailQualityDatasets = computed(() => [...new Set(
  (qualityDetail.data.value?.rule_results ?? []).map((item) => item.dataset),
)].sort((left, right) => left.localeCompare(right)))

async function refreshDataCenter() {
  await Promise.all([
    client.invalidateQueries({ queryKey: ['data-summary'] }),
    client.invalidateQueries({ queryKey: ['data-datasets'] }),
    client.invalidateQueries({ queryKey: ['data-quality-runs'] }),
  ])
}

watch(() => trackedTask.data.value?.status, async (status, previous) => {
  if (status && status !== previous && ['SUCCEEDED', 'FAILED', 'CANCELLED', 'ORPHANED'].includes(status)) {
    await refreshDataCenter()
  }
})

watch(selectedQualityRun, () => {
  detailQualityDataset.value = ''
  detailQualityStatus.value = ''
  detailQualityRule.value = ''
})

const update = useMutation({
  mutationFn: (confirmedPlan: DataUpdatePlan) => {
    const requestedWindow = confirmedPlan.window_mode === 'EXPLICIT'
      ? { start: confirmedPlan.requested_start, end: confirmedPlan.requested_end }
      : {}
    return api.post<{ task_id: string; request_id: string; plan_hash: string }>('/api/v1/data/updates', {
      ...requestedWindow,
      datasets: [
        ...confirmedPlan.dataset_windows.map((item) => item.dataset),
        ...confirmedPlan.skipped_datasets.map((item) => item.dataset),
      ].sort((left, right) => left.localeCompare(right)),
      plan_hash: confirmedPlan.plan_hash,
    })
  },
  onSuccess: async (result) => {
    trackedTaskId.value = result.task_id
    trackedTaskKind.value = 'UPDATE'
    updateDialog.value = false
    ElMessage.success(`更新任务已提交 · ${result.task_id.slice(0, 8)}`)
    await client.invalidateQueries({ queryKey: ['tasks'] })
  },
  onError: async (error) => {
    if (error instanceof DashboardApiError && error.code === 'DATA_UPDATE_PLAN_STALE') {
      await updatePlan.refetch()
      ElMessage.warning('数据水位已变化，计划预览已刷新，请重新确认。')
      return
    }
    ElMessage.error(error instanceof DashboardApiError
      ? (error.remediation ?? error.message)
      : String(error))
  },
})

const bootstrap = useMutation({
  mutationFn: () => api.post<{
    task_id: string
    request_id: string
    status: string
    years: number
  }>('/api/v1/data/bootstrap', { years: bootstrapYears.value }),
  onSuccess: async (result) => {
    trackedTaskId.value = result.task_id
    trackedTaskKind.value = 'BOOTSTRAP'
    bootstrapDialog.value = false
    ElMessage.success(`初始化任务已提交 · ${result.task_id.slice(0, 8)}`)
    await Promise.all([
      client.invalidateQueries({ queryKey: ['data-summary'] }),
      client.invalidateQueries({ queryKey: ['tasks'] }),
    ])
  },
  onError: (error) => {
    ElMessage.error(error instanceof DashboardApiError
      ? (error.remediation ?? error.message)
      : String(error))
  },
})

const createQualityRun = useMutation({
  mutationFn: () => api.post<{
    task_id: string
    request_id: string
    status: string
    scope: 'ALL' | 'DATASET'
    dataset?: string
  }>('/api/v1/data/quality-runs', qualityRunDataset.value === 'ALL'
    ? {}
    : { dataset: qualityRunDataset.value }),
  onSuccess: async (result) => {
    trackedTaskId.value = result.task_id
    trackedTaskKind.value = 'QUALITY'
    qualityRunDialog.value = false
    const target = result.dataset ? ` · ${result.dataset}` : ' · 全部数据集'
    ElMessage.success(`质量运行已提交${target} · ${result.task_id.slice(0, 8)}`)
    await client.invalidateQueries({ queryKey: ['tasks'] })
  },
  onError: (error) => {
    ElMessage.error(error instanceof DashboardApiError
      ? (error.remediation ?? error.message)
      : String(error))
  },
})

function openUpdateDialog() {
  updateMode.value = 'AUTO_INCREMENTAL'
  dateRange.value = null
  selectedUpdateDatasets.value = [...updateDatasetOptions.value]
  updateDialog.value = true
}

function openBootstrapDialog() {
  bootstrapYears.value = summary.data.value?.initialization.years ?? 5
  bootstrapDialog.value = true
}

function openQualityRunDialog() {
  qualityRunDataset.value = 'ALL'
  qualityRunDialog.value = true
}

function updateBasisLabel(basis: string) {
  if (basis === 'BOOTSTRAP') return '首次构建'
  if (basis === 'INCREMENTAL') return '增量水位'
  if (basis === 'SNAPSHOT_REFRESH') return '全量快照'
  if (basis === 'DISCLOSURE_TRIGGER') return '季度披露'
  return '指定日期'
}

function freshnessEvidence(dataset: Dataset) {
  if (['stock_master', 'fund_master', 'index_master', 'industry_catalog'].includes(dataset.dataset)) {
    return `全量快照 · 最近刷新 ${formatTime(dataset.operational.last_localized_at)}`
  }
  if (dataset.dataset === 'trade_calendar') {
    return `日历覆盖至 ${formatDate(dataset.end_date)} · 已检查至 ${formatDate(dataset.operational.localized_through)}`
  }
  if (dataset.freshness.trigger_date) {
    const state = dataset.freshness.update_required ? '待更新' : '无需更新'
    return `季度披露 · ${dataset.freshness.trigger_date} · ${state}`
  }
  return `${formatDate(dataset.freshness.actual_watermark)} / ${formatDate(dataset.freshness.expected_watermark)}`
}

function coverageDate(dataset: Dataset, boundary: 'start' | 'end') {
  if (['stock_master', 'fund_master', 'index_master', 'industry_catalog'].includes(dataset.dataset)) {
    return formatDate(dataset.operational.localized_through)
  }
  return formatDate(boundary === 'start' ? dataset.start_date : dataset.end_date)
}

function windowState(window: DataUpdateWindow) {
  if (['stock_financial_indicator', 'stock_income_statement', 'stock_balance_sheet', 'stock_cash_flow_statement', 'stock_master', 'fund_master', 'index_master', 'industry_catalog'].includes(window.dataset)) {
    return '不适用'
  }
  if (window.dataset === 'trade_calendar') {
    return `覆盖至 ${window.current_watermark ?? '—'}`
  }
  return window.current_watermark ?? '—'
}

function windowLookback(window: DataUpdateWindow) {
  if (['stock_financial_indicator', 'stock_income_statement', 'stock_balance_sheet', 'stock_cash_flow_statement', 'stock_master', 'fund_master', 'index_master', 'industry_catalog'].includes(window.dataset)) {
    return '不适用'
  }
  return window.dataset === 'trade_calendar'
    ? `修订回看 ${window.overlap_days} 天`
    : `${window.overlap_days} 天`
}

function windowRange(window: DataUpdateWindow) {
  if (window.basis === 'SNAPSHOT_REFRESH') {
    return `快照日期 ${window.start}`
  }
  if (window.dataset === 'trade_calendar') {
    return `抓取 ${window.start} 至 ${window.end}`
  }
  const trigger = window.trigger_date ? ` · 截止 ${window.trigger_date}` : ''
  return `${window.start} 至 ${window.end}${trigger}`
}

async function submitUpdate() {
  const plan = updatePlan.data.value
  if (!plan) return
  const windowText = `${plan.start} 至 ${plan.end}，执行 ${plan.dataset_windows.length} 个数据集，跳过 ${plan.skipped_datasets.length} 个`
  await ElMessageBox.confirm(
    `本次范围：${windowText}。Canonical 内容变化可能暂时关闭研究门；活动实验可能因 data_hash 漂移失败。`,
    '确认数据更新',
    { type: 'warning', confirmButtonText: '提交更新任务' },
  )
  update.mutate(plan)
}
</script>

<template>
  <div class="page-stack">
    <ErrorState
      v-if="summary.isError.value || datasets.isError.value || qualityRuns.isError.value"
      message="无法读取数据中心状态，请检查本地服务和元数据库。"
    />
    <template v-else>
      <div class="metrics-grid">
        <MetricCard
          label="研究门"
          :value="summary.data.value?.gate.status ?? 'UNKNOWN'"
          :hint="`${summary.data.value?.gate.reason ?? 'UNKNOWN'} · ${shortHash(summary.data.value?.gate.catalog_hash)}`"
          :tone="summary.data.value?.gate.status === 'READY' ? 'cyan' : 'red'"
        />
        <MetricCard
          label="新鲜度"
          :value="summary.data.value?.freshness.status ?? 'UNKNOWN'"
          :hint="`最近完整交易日 ${formatDate(summary.data.value?.freshness.latest_complete_session)}`"
          :tone="summary.data.value?.freshness.status === 'CURRENT' ? 'cyan' : 'red'"
        />
        <MetricCard
          label="数据规模"
          :value="formatNumber(totalRows)"
          :hint="`${datasets.data.value?.items.length ?? 0} 个目录数据集`"
        />
        <MetricCard
          label="活动研究任务"
          :value="summary.data.value?.active_research_task_count ?? 0"
          :hint="`Worker 心跳 ${formatTime(summary.data.value?.worker?.heartbeat_at)}`"
        />
      </div>

      <section v-if="trackedTaskId" class="panel" data-testid="update-progress">
        <header class="panel-heading">
          <div>
            <h2>{{ trackedTaskKind === 'QUALITY' ? '质量运行任务' : trackedTaskKind === 'BOOTSTRAP' ? '初始化任务' : '更新任务' }} {{ trackedTaskId.slice(0, 8) }}</h2>
            <p>任务活动进度每 3 秒自动刷新。</p>
          </div>
          <div style="display:flex;gap:10px;align-items:center">
            <StatusBadge :status="trackedTask.data.value?.status ?? 'QUEUED'" />
            <el-button @click="router.push(`/tasks?task=${trackedTaskId}`)">查看任务</el-button>
          </div>
        </header>
        <DataTaskProgress v-if="trackedTask.data.value" :task="trackedTask.data.value" mode="detail" />
        <p v-else class="muted-value">等待 Worker 接收任务</p>
      </section>

      <el-alert
        v-if="summary.data.value && !initializationReady"
        class="bootstrap-alert"
        :title="summary.data.value.initialization.status === 'IN_PROGRESS' ? '数据初始化尚未完成' : '数据尚未初始化'"
        :description="summary.data.value.initialization.status === 'IN_PROGRESS'
          ? `请使用已冻结的 ${summary.data.value.initialization.years} 年窗口继续 Bootstrap。完成全市场采集、Canonical 清洗和全目录校验后，研究门才会开放。`
          : '先执行 Bootstrap，建立 Tushare 全市场 Canonical 基线。完成后才会开放日常更新和质量运行入口。'"
        type="warning"
        :closable="false"
        show-icon
      />

      <section class="panel table-panel">
        <header class="panel-heading">
          <div><h2>数据资产</h2><p>VALIDATED 表示研究门状态，新鲜度是独立的非阻断运营告警。</p></div>
          <div class="data-actions">
            <el-button
              v-if="!initializationReady"
              data-testid="bootstrap-action"
              type="primary"
              :disabled="bootstrapTaskActive"
              @click="openBootstrapDialog"
            >{{ bootstrapTaskActive ? '初始化任务进行中' : summary.data.value?.initialization.status === 'IN_PROGRESS' ? '继续初始化' : '初始化数据' }}</el-button>
            <template v-else>
              <el-button type="primary" @click="openUpdateDialog">创建更新任务</el-button>
              <el-button @click="openQualityRunDialog">质量运行</el-button>
            </template>
          </div>
        </header>
        <div style="display:flex;gap:10px;margin-bottom:14px">
          <el-input v-model="datasetFilter" clearable placeholder="筛选数据集" style="width:220px" />
          <el-select v-model="freshnessFilter" clearable placeholder="新鲜度" style="width:160px">
            <el-option v-for="status in ['CURRENT', 'STALE', 'MISSING', 'UNKNOWN']" :key="status" :label="status" :value="status" />
          </el-select>
        </div>
        <el-table data-testid="data-assets-table" :data="visibleDatasets" height="420" @row-click="(row: Dataset) => selectedDataset = row.dataset">
          <el-table-column label="数据集" prop="dataset" min-width="180" />
          <el-table-column label="开始日期" width="120"><template #default="scope">{{ coverageDate(scope.row, 'start') }}</template></el-table-column>
          <el-table-column label="结束日期" width="120"><template #default="scope">{{ coverageDate(scope.row, 'end') }}</template></el-table-column>
          <el-table-column label="研究门" width="110"><template #default><StatusBadge :status="summary.data.value?.gate.status === 'READY' ? 'VALIDATED' : 'BLOCKED'" /></template></el-table-column>
          <el-table-column label="新鲜度" width="110"><template #default="scope"><StatusBadge :status="scope.row.freshness.status" /></template></el-table-column>
          <el-table-column label="更新依据" min-width="250"><template #default="scope">{{ freshnessEvidence(scope.row) }}</template></el-table-column>
          <el-table-column label="延迟" width="90"><template #default="scope">{{ scope.row.freshness.lag_days == null ? '—' : `${scope.row.freshness.lag_days} 天` }}</template></el-table-column>
          <el-table-column label="质量问题" width="100" prop="quality_issue_count" align="right" />
          <el-table-column label="记录数" width="120" align="right"><template #default="scope">{{ formatNumber(scope.row.row_count) }}</template></el-table-column>
          <el-table-column label="检查时间" width="135"><template #default="scope">{{ formatTime(scope.row.freshness.evaluated_at) }}</template></el-table-column>
        </el-table>
      </section>

      <section class="panel table-panel">
        <header class="panel-heading"><div><h2>质量运行历史</h2><p>门禁绑定运行和最近诊断运行分别展示，避免误判当前研究可读性。</p></div></header>
        <div style="display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap">
          <el-select v-model="qualityStatus" clearable placeholder="运行状态" style="width:150px"><el-option label="PASSED" value="PASSED" /><el-option label="FAILED" value="FAILED" /></el-select>
          <el-select v-model="qualityDataset" clearable placeholder="数据集" style="width:190px"><el-option v-for="item in datasets.data.value?.items ?? []" :key="item.dataset" :label="item.dataset" :value="item.dataset" /></el-select>
          <el-select v-model="qualitySeverity" clearable placeholder="严重级别" style="width:150px"><el-option v-for="severity in ['FATAL', 'SEVERE', 'WARNING', 'INFO']" :key="severity" :label="severity" :value="severity" /></el-select>
          <el-input v-model="qualityRule" clearable placeholder="规则 ID" style="width:190px" />
        </div>
        <el-table data-testid="quality-runs-table" :data="qualityRuns.data.value?.items ?? []" height="300" @row-click="(row: QualityRun) => selectedQualityRun = row.run_id">
          <el-table-column label="门禁绑定" width="100"><template #default="scope">{{ scope.row.run_id === gateRunId ? '是' : '—' }}</template></el-table-column>
          <el-table-column label="运行" width="120"><template #default="scope"><span class="hash">{{ scope.row.run_id.slice(0, 8) }}</span></template></el-table-column>
          <el-table-column label="范围" prop="scope" min-width="150" />
          <el-table-column label="状态" width="110"><template #default="scope"><StatusBadge :status="scope.row.status" /></template></el-table-column>
          <el-table-column label="问题 / 阻断" width="120"><template #default="scope">{{ scope.row.issue_count }} / {{ scope.row.blocking_issue_count }}</template></el-table-column>
          <el-table-column label="完成时间" min-width="150"><template #default="scope">{{ formatTime(scope.row.completed_at) }}</template></el-table-column>
        </el-table>
      </section>
    </template>

    <el-drawer v-model="datasetDrawerOpen" :title="selectedDataset ?? '数据集详情'" size="62%">
      <template v-if="datasetDetail.data.value">
        <p>分区策略：{{ datasetDetail.data.value.contract.partitioning }} · 频率：{{ datasetDetail.data.value.contract.cadence }} · 复用：{{ datasetDetail.data.value.contract.reuse }}</p>
        <h3>Schema</h3>
        <el-table :data="datasetDetail.data.value.contract.schema" max-height="240"><el-table-column prop="name" label="字段" /><el-table-column prop="type" label="类型" /></el-table>
        <h3>供应端点</h3>
        <p v-for="source in datasetDetail.data.value.contract.sources" :key="source.source">{{ source.source }}：{{ source.endpoints.join(', ') }}</p>
        <h3>分区哈希</h3>
        <el-table :data="datasetDetail.data.value.partitions" max-height="320"><el-table-column prop="partition_key" label="分区" /><el-table-column label="内容哈希"><template #default="scope">{{ shortHash(scope.row.content_hash) }}</template></el-table-column><el-table-column label="输入哈希"><template #default="scope">{{ shortHash(scope.row.input_hash) }}</template></el-table-column></el-table>
      </template>
    </el-drawer>

    <el-drawer v-model="qualityDrawerOpen" title="质量运行详情" size="65%">
      <template v-if="qualityDetail.data.value">
        <p>Scope：{{ qualityDetail.data.value.scope }} · Data hash：{{ shortHash(qualityDetail.data.value.input_hash) }}</p>
        <el-alert
          v-if="!qualityDetail.data.value.results_complete"
          title="该历史运行缺少完整规则执行证据；仅已保存的问题可确认为失败，其余规则显示 UNKNOWN。"
          type="warning"
          :closable="false"
          show-icon
        />
        <div class="quality-result-summary">
          <div v-for="status in qualityResultStatuses" :key="status">
            <StatusBadge :status="status" />
            <strong>{{ qualityDetail.data.value.result_counts[status] }}</strong>
          </div>
        </div>
        <div class="quality-result-filters">
          <el-select v-model="detailQualityDataset" data-testid="quality-result-dataset" clearable placeholder="数据集" style="width:180px">
            <el-option v-for="dataset in detailQualityDatasets" :key="dataset" :label="dataset" :value="dataset" />
          </el-select>
          <el-select v-model="detailQualityStatus" data-testid="quality-result-status" clearable placeholder="结果状态" style="width:150px">
            <el-option v-for="status in qualityResultStatuses" :key="status" :label="status" :value="status" />
          </el-select>
          <el-input v-model="detailQualityRule" clearable placeholder="规则 ID 或标题" style="width:220px" />
        </div>
        <el-table data-testid="quality-rule-results" :data="visibleQualityResults" max-height="560">
          <el-table-column type="expand">
            <template #default="scope: { row: QualityRuleResult }">
              <div class="quality-result-evidence">
                <p><strong>证据来源：</strong>{{ scope.row.evidence }}</p>
                <p><strong>实际 / 阈值：</strong><code>{{ JSON.stringify(scope.row.actual) }} / {{ JSON.stringify(scope.row.threshold) }}</code></p>
                <p><strong>Scope：</strong><code>{{ JSON.stringify(scope.row.scope) }}</code></p>
                <p v-if="scope.row.skip_reason"><strong>未执行原因：</strong>{{ scope.row.skip_reason }}</p>
                <div v-for="issue in scope.row.issues" :key="`${issue.dataset}-${issue.rule_id}-${issue.message}`" class="quality-issue">
                  <p><strong>问题：</strong>{{ issue.message }}</p>
                  <p><strong>修复建议：</strong>{{ issue.remediation }}</p>
                </div>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="结果" width="105"><template #default="scope"><StatusBadge :status="scope.row.status" /></template></el-table-column>
          <el-table-column label="失败级别" width="105">
            <template #default="scope">
              <StatusBadge v-if="scope.row.status === 'FAIL'" :status="scope.row.severity" />
              <span v-else>—</span>
            </template>
          </el-table-column>
          <el-table-column prop="dataset" label="数据集" width="165" />
          <el-table-column label="规则" min-width="210"><template #default="scope"><strong>{{ scope.row.title }}</strong><br><code>{{ scope.row.rule_id }}</code></template></el-table-column>
          <el-table-column prop="description" label="规则说明" min-width="260" />
          <el-table-column prop="pass_criterion" label="通过条件" min-width="220" />
        </el-table>
      </template>
    </el-drawer>

    <el-dialog v-model="bootstrapDialog" title="初始化 Tushare 数据" width="560">
      <p class="dialog-description">
        Bootstrap 会由 Worker 依次执行全市场采集、Canonical 清洗和全目录质量校验。首次执行可能耗时较长，可在运行中心查看进度和受控日志。
      </p>
      <el-form label-position="top">
        <el-form-item label="历史覆盖年数">
          <el-input-number
            v-model="bootstrapYears"
            data-testid="bootstrap-years"
            :min="1"
            :disabled="summary.data.value?.initialization.status === 'IN_PROGRESS'"
            controls-position="right"
            style="width:100%"
          />
        </el-form-item>
      </el-form>
      <el-alert
        v-if="summary.data.value?.initialization.status === 'IN_PROGRESS'"
        title="初始化窗口已经冻结；重试必须保持相同年数，已完成的 Raw 请求和 Canonical 输入会按内容身份复用。"
        type="info"
        :closable="false"
        show-icon
      />
      <el-alert
        v-else
        title="提交后历史窗口将冻结。请先确认 Worker 正在运行，并已在设置页配置数据源 Token。"
        type="warning"
        :closable="false"
        show-icon
      />
      <el-button
        v-if="summary.data.value?.initialization.status !== 'IN_PROGRESS'"
        link
        type="primary"
        style="margin-top:10px"
        @click="router.push('/settings')"
      >打开设置</el-button>
      <template #footer>
        <el-button @click="bootstrapDialog = false">取消</el-button>
        <el-button
          type="primary"
          :loading="bootstrap.isPending.value"
          :disabled="bootstrapYears < 1"
          @click="bootstrap.mutate()"
        >提交初始化任务</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="updateDialog" title="创建数据更新任务" width="780">
      <p style="color:var(--muted);font-size:12px;line-height:1.8">提交前会根据实时交易日历、Canonical 水位和季度披露截止日固化各数据集计划。财务指标和三张财务报表不使用水位；截止日尚未越过时自动判定为无需更新。</p>
      <div class="dataset-selector">
        <div class="dataset-selector-heading">
          <strong>目标数据集</strong>
          <span>
            <el-button link type="primary" @click="selectedUpdateDatasets = [...updateDatasetOptions]">全选</el-button>
            <el-button link @click="selectedUpdateDatasets = []">清空</el-button>
          </span>
        </div>
        <el-select
          v-model="selectedUpdateDatasets"
          data-testid="update-dataset-select"
          multiple
          filterable
          collapse-tags
          collapse-tags-tooltip
          placeholder="请选择至少一个数据集"
          style="width:100%;margin-bottom:16px"
        >
          <el-option
            v-for="dataset in updateDatasetOptions"
            :key="dataset"
            :label="dataset"
            :value="dataset"
          />
        </el-select>
      </div>
      <el-radio-group v-model="updateMode" style="margin-bottom:16px">
        <el-radio-button value="AUTO_INCREMENTAL">自动增量</el-radio-button>
        <el-radio-button value="EXPLICIT">指定日期</el-radio-button>
      </el-radio-group>
      <el-date-picker
        v-if="updateMode === 'EXPLICIT'"
        v-model="dateRange"
        type="daterange"
        value-format="YYYY-MM-DD"
        start-placeholder="开始日期"
        end-placeholder="结束日期"
        style="width:100%;margin-bottom:16px"
      />
      <div v-if="normalizedUpdateDatasets.length === 0" class="plan-state">请至少选择一个数据集。</div>
      <div v-else-if="updatePlan.isFetching.value" class="plan-state">正在读取供应商交易日历并生成计划…</div>
      <ErrorState v-else-if="updatePlan.isError.value" :message="String(updatePlan.error.value)" />
      <div v-else-if="updateMode === 'EXPLICIT' && !dateRange" class="plan-state">请选择完整的开始和结束日期。</div>
      <section v-else-if="updatePlan.data.value" class="update-plan" data-testid="update-plan-preview">
        <div class="plan-summary">
          <div><small>更新模式</small><strong>{{ updatePlan.data.value.window_mode === 'AUTO_INCREMENTAL' ? '自动增量' : '指定日期' }}</strong></div>
          <div><small>汇总范围</small><strong>{{ updatePlan.data.value.start }} 至 {{ updatePlan.data.value.end }}</strong></div>
          <div><small>执行 / 跳过</small><strong>{{ updatePlan.data.value.dataset_windows.length }} / {{ updatePlan.data.value.skipped_datasets.length }}</strong></div>
        </div>
        <el-alert
          v-if="updatePlan.data.value.skipped_datasets.length"
          title="季度披露截止日尚未越过"
          :description="updatePlan.data.value.skipped_datasets.map((item) => `${item.dataset}：${item.trigger_date} 截止，当前无需更新`).join('；')"
          type="info"
          :closable="false"
          show-icon
          style="margin:12px 0"
        />
        <el-collapse>
          <el-collapse-item title="查看各数据集执行窗口" name="windows">
            <el-table :data="updatePlan.data.value.dataset_windows" max-height="320" size="small">
              <el-table-column prop="dataset" label="数据集" min-width="155" />
              <el-table-column label="依据" width="105"><template #default="scope">{{ updateBasisLabel(scope.row.basis) }}</template></el-table-column>
              <el-table-column label="当前状态" width="155"><template #default="scope">{{ windowState(scope.row) }}</template></el-table-column>
              <el-table-column label="回看策略" width="145"><template #default="scope">{{ windowLookback(scope.row) }}</template></el-table-column>
              <el-table-column label="计划窗口" min-width="245"><template #default="scope">{{ windowRange(scope.row) }}</template></el-table-column>
            </el-table>
          </el-collapse-item>
        </el-collapse>
      </section>
      <template #footer>
        <el-button @click="updateDialog = false">取消</el-button>
        <el-button
          type="primary"
          :loading="update.isPending.value"
          :disabled="normalizedUpdateDatasets.length === 0 || !updatePlan.data.value || updatePlan.data.value.dataset_windows.length === 0 || updatePlan.isFetching.value || updatePlan.isError.value"
          @click="submitUpdate"
        >确认并提交计划</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="qualityRunDialog" title="创建质量运行" width="520">
      <p class="dialog-description">质量校验由 Worker 后台执行，可在运行中心查看进度、日志和重试记录。</p>
      <el-form label-position="top">
        <el-form-item label="校验范围">
          <el-select
            v-model="qualityRunDataset"
            data-testid="quality-run-dataset"
            filterable
            style="width:100%"
          >
            <el-option label="全部数据集（validate-all）" value="ALL" />
            <el-option
              v-for="dataset in updateDatasetOptions"
              :key="dataset"
              :label="dataset"
              :value="dataset"
            />
          </el-select>
        </el-form-item>
      </el-form>
      <el-alert
        v-if="qualityRunDataset === 'ALL'"
        title="全目录质量运行通过后会重新绑定研究门；发现阻断问题时任务失败，但质量运行证据仍会登记。"
        type="warning"
        :closable="false"
        show-icon
      />
      <el-alert
        v-else
        title="单数据集运行仅用于诊断，不改变研究门；任务成功表示校验已执行，质量结论以运行记录为准。"
        type="info"
        :closable="false"
        show-icon
      />
      <template #footer>
        <el-button @click="qualityRunDialog = false">取消</el-button>
        <el-button
          type="primary"
          :loading="createQualityRun.isPending.value"
          @click="createQualityRun.mutate()"
        >确认并创建</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.data-actions {
  display: flex;
  align-items: center;
}

.bootstrap-alert {
  margin-bottom: 16px;
}

.dialog-description {
  margin: 0 0 16px;
  color: var(--muted);
  font-size: 12px;
  line-height: 1.8;
}

.plan-state {
  padding: 24px;
  color: var(--muted);
  text-align: center;
  border: 1px dashed var(--border);
  border-radius: 10px;
}

.dataset-selector {
  margin-bottom: 4px;
}

.dataset-selector-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
  font-size: 13px;
}

.update-plan {
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
}

.plan-summary {
  display: grid;
  grid-template-columns: 1fr 2fr .7fr;
  gap: 12px;
  padding: 16px;
  background: var(--surface-raised);
}

.plan-summary div {
  display: grid;
  gap: 6px;
}

.plan-summary small {
  color: var(--muted);
}

.plan-summary strong {
  font-size: 13px;
}

.update-plan :deep(.el-collapse) {
  border-top: 1px solid var(--border);
  border-bottom: 0;
}

.update-plan :deep(.el-collapse-item__header),
.update-plan :deep(.el-collapse-item__wrap) {
  padding: 0 16px;
  border-bottom: 0;
}

.quality-result-summary,
.quality-result-filters {
  display: flex;
  gap: 12px;
  margin: 16px 0;
  align-items: center;
  flex-wrap: wrap;
}

.quality-result-summary div {
  display: flex;
  gap: 7px;
  align-items: center;
  padding: 7px 10px;
  border: 1px solid var(--border);
  border-radius: 8px;
}

.quality-result-evidence {
  padding: 8px 24px;
  color: var(--muted);
}

.quality-issue {
  margin-top: 10px;
  padding: 8px 12px;
  border-left: 3px solid var(--danger);
  background: var(--surface-raised);
}
</style>
