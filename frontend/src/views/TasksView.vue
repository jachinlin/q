<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatDuration, formatTime } from '../format'
import type { DataUpdatePlan, DataUpdateWindow, Task, TaskAttempt, TaskDetail, TaskDiagnostic, TaskLog, TaskPage } from '../types'

type RetryResult = { task_id: string; family_id?: string; execution_id?: string }

const client = useQueryClient()
const route = useRoute()
const page = ref(1)
const taskStatuses = ['QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCEL_REQUESTED', 'CANCELLED', 'ORPHANED'] as const
const requestedStatus = typeof route.query.status === 'string' && taskStatuses.some((item) => item === route.query.status)
  ? route.query.status
  : ''
const status = ref(requestedStatus)
const detailId = ref('')
const detailOpen = ref(false)
const activeTab = ref('overview')
const selectedAttempt = ref('')
const logData = ref<TaskLog | null>(null)
const logLoading = ref(false)
const logError = ref('')
const now = ref(Date.now())
let clockTimer: number | undefined
let logRequest = 0

const query = useQuery({
  queryKey: computed(() => ['tasks', page.value, status.value]),
  queryFn: () => api.get<TaskPage>(`/api/v1/tasks?page=${page.value}&page_size=25${status.value ? `&status=${status.value}` : ''}`),
  refetchInterval: 3000,
})
const detail = useQuery({
  queryKey: computed(() => ['task', detailId.value]),
  queryFn: () => api.get<TaskDetail>(`/api/v1/tasks/${detailId.value}`),
  enabled: computed(() => Boolean(detailId.value)),
  refetchInterval: 3000,
})
const cancel = useMutation({
  mutationFn: (id: string) => api.post<{ task_id: string; status: string }>(`/api/v1/tasks/${id}/cancel`),
  onSuccess: async (result) => {
    ElMessage.success(`取消请求已记录：${result.task_id.slice(0, 8)}`)
    await Promise.all([
      client.invalidateQueries({ queryKey: ['tasks'] }),
      client.invalidateQueries({ queryKey: ['task', result.task_id] }),
    ])
  },
})
const retry = useMutation({
  mutationFn: ({ id, orphaned }: { id: string; orphaned: boolean }) =>
    api.post<RetryResult>(`/api/v1/tasks/${id}/retry`, { confirm_orphaned: orphaned }),
  onSuccess: async (result) => {
    const execution = result.execution_id ? `，新 execution ${result.execution_id.slice(0, 8)}` : ''
    ElMessage.success(`已创建安全重试：任务 ${result.task_id.slice(0, 8)}${execution}`)
    await Promise.all([
      client.invalidateQueries({ queryKey: ['tasks'] }),
      client.invalidateQueries({ queryKey: ['task'] }),
    ])
  },
  onError: (error) => {
    if (error instanceof DashboardApiError) {
      ElMessage.error(error.remediation ?? error.message)
    }
  },
})
const remove = useMutation({
  mutationFn: (id: string) => api.delete<{ task_id: string; status: 'DELETED' }>(`/api/v1/tasks/${id}`),
  onSuccess: async (result) => {
    if (detailId.value === result.task_id) {
      detailOpen.value = false
      detailId.value = ''
      selectedAttempt.value = ''
      logData.value = null
    }
    ElMessage.success(`已删除任务记录：${result.task_id.slice(0, 8)}`)
    client.removeQueries({ queryKey: ['task', result.task_id] })
    await client.invalidateQueries({ queryKey: ['tasks'] })
  },
})

const statusCards = computed(() => {
  const counts = query.data.value?.status_counts ?? {}
  const total = Object.values(counts).reduce((sum, value) => sum + Number(value), 0)
  return [
    { label: '全部任务', value: total, status: '', tone: 'neutral' },
    { label: '运行中', value: counts.RUNNING ?? 0, status: 'RUNNING', tone: 'blue' },
    { label: '排队中', value: counts.QUEUED ?? 0, status: 'QUEUED', tone: 'cyan' },
    { label: '失败', value: counts.FAILED ?? 0, status: 'FAILED', tone: 'danger' },
    { label: '孤儿任务', value: counts.ORPHANED ?? 0, status: 'ORPHANED', tone: 'warning' },
  ]
})

const parameterEntries = computed(() =>
  Object.entries(detail.data.value?.payload ?? {})
    .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0),
)

const formattedPayload = computed(() => JSON.stringify(
  Object.fromEntries(parameterEntries.value),
  null,
  2,
))

const dataUpdatePlan = computed<DataUpdatePlan | null>(() => {
  const task = detail.data.value
  if (task?.task_type !== 'DATA_UPDATE') return null
  const payload = task.payload
  if (
    !['AUTO_INCREMENTAL', 'EXPLICIT'].includes(String(payload.window_mode))
    || typeof payload.start !== 'string'
    || typeof payload.end !== 'string'
    || typeof payload.planned_at !== 'string'
    || typeof payload.plan_hash !== 'string'
    || !Array.isArray(payload.dataset_windows)
    || !Array.isArray(payload.skipped_datasets)
  ) return null
  return payload as DataUpdatePlan
})

const legacyDataUpdate = computed(() =>
  detail.data.value?.task_type === 'DATA_UPDATE' && dataUpdatePlan.value === null,
)

function windowBasisLabel(value: string) {
  if (value === 'BOOTSTRAP') return '首次构建'
  if (value === 'INCREMENTAL') return '增量水位'
  if (value === 'SNAPSHOT_REFRESH') return '全量快照'
  if (value === 'DISCLOSURE_TRIGGER') return '季度披露'
  return '指定日期'
}

function windowState(window: DataUpdateWindow) {
  if (['financial_observation', 'instrument'].includes(window.dataset)) return '不适用'
  if (window.dataset === 'trade_calendar') return `覆盖至 ${window.current_watermark ?? '—'}`
  return window.current_watermark ?? '—'
}

function windowLookback(window: DataUpdateWindow) {
  if (['financial_observation', 'instrument'].includes(window.dataset)) return '不适用'
  return window.dataset === 'trade_calendar'
    ? `修订回看 ${window.overlap_days} 天`
    : `${window.overlap_days} 天`
}

function windowRange(window: DataUpdateWindow) {
  if (window.basis === 'SNAPSHOT_REFRESH') return `快照日期 ${window.start}`
  if (window.dataset === 'trade_calendar') return `抓取 ${window.start} 至 ${window.end}`
  const trigger = window.trigger_date ? ` · 截止 ${window.trigger_date}` : ''
  return `${window.start} 至 ${window.end}${trigger}`
}

const selectedAttemptRecord = computed(() =>
  detail.data.value?.attempts.find((attempt) => attempt.id === selectedAttempt.value) ?? null,
)

const diagnosis = computed<TaskDiagnostic | null>(() => {
  if (logData.value?.attempt_id === selectedAttempt.value && logData.value.diagnostic) {
    return logData.value.diagnostic
  }
  const task = detail.data.value
  if (!task?.error) return null
  return {
    code: textField(task.error, 'code'),
    message: textField(task.error, 'message'),
    exception_type: null,
    stage: taskStage(task),
    retryable: boolField(task.error, 'retryable'),
    remediation: textField(task.error, 'remediation') ?? fallbackRemediation(task),
    traceback: null,
  }
})

function textField(value: Record<string, unknown> | null, key: string) {
  const item = value?.[key]
  return typeof item === 'string' && item ? item : null
}

function boolField(value: Record<string, unknown> | null, key: string) {
  const item = value?.[key]
  return typeof item === 'boolean' ? item : null
}

function taskStage(task: Task) {
  const stage = task.progress.stage
  return typeof stage === 'string' && stage ? stage : null
}

function errorCode(task: { error: Record<string, unknown> | null }) {
  return textField(task.error, 'code')
}

function errorMessage(task: { error: Record<string, unknown> | null }) {
  return textField(task.error, 'message')
}

function fallbackRemediation(task: Task) {
  return task.status === 'ORPHANED'
    ? '确认 Worker 已停止并检查临时产物，再执行孤儿任务重试。'
    : '查看最新失败尝试的诊断和日志，修复原因后再安全重试。'
}

function progress(task: Task) {
  const completed = Number(task.progress.completed ?? 0)
  const total = Number(task.progress.total ?? 0)
  return total > 0 ? Math.round(completed / total * 100) : 0
}

function duration(startedAt: string | null, completedAt: string | null) {
  return formatDuration(startedAt, completedAt, now.value)
}

function parameterValue(value: unknown) {
  const serialized = typeof value === 'string' ? value : JSON.stringify(value)
  const text = serialized ?? String(value)
  return text.length > 160 ? `${text.slice(0, 157)}…` : text
}

function setStatus(value: string) {
  status.value = status.value === value ? '' : value
  page.value = 1
}

function openTask(taskId: string) {
  detailId.value = taskId
  detailOpen.value = true
  activeTab.value = 'overview'
  selectedAttempt.value = ''
  logData.value = null
  logError.value = ''
}

function openDetail(row: Task) {
  openTask(row.id)
}

async function loadLog(attemptId: string) {
  const taskId = detailId.value
  const requestId = ++logRequest
  selectedAttempt.value = attemptId
  logLoading.value = true
  logError.value = ''
  try {
    const value = await api.get<TaskLog>(`/api/v1/tasks/${taskId}/attempts/${attemptId}/log?tail_lines=500`)
    if (requestId === logRequest && taskId === detailId.value && attemptId === selectedAttempt.value) {
      logData.value = value
    }
  } catch (error) {
    if (requestId === logRequest) {
      logData.value = null
      logError.value = error instanceof Error ? error.message : String(error)
    }
  } finally {
    if (requestId === logRequest) logLoading.value = false
  }
}

async function cancelTask(row: Task) {
  try {
    await ElMessageBox.confirm('任务将在下一个安全边界停止。', '确认取消', { type: 'warning' })
  } catch {
    return
  }
  cancel.mutate(row.id)
}

async function retryTask(row: Task) {
  const prompt = row.status === 'ORPHANED'
    ? '孤儿任务可能留有未发布临时产物。请确认 Worker 已停止并已检查临时产物，再创建新的安全尝试。'
    : row.status === 'SUCCEEDED'
      ? '确认基于当前安全边界再次运行该任务？'
      : '确认创建新的安全任务尝试？'
  try {
    await ElMessageBox.confirm(prompt, row.status === 'SUCCEEDED' ? '确认再次运行' : '确认重试', { type: 'warning' })
  } catch {
    return
  }
  retry.mutate({ id: row.id, orphaned: row.status === 'ORPHANED' })
}

async function deleteTask(row: Task) {
  try {
    await ElMessageBox.confirm(
      '将从运行中心删除该终态任务及尝试索引。实验、研究产物、审计和日志文件仍会保留，此操作不可撤销。',
      '确认删除任务记录',
      { type: 'error', confirmButtonText: '删除', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  remove.mutate(row.id)
}

function rowClassName({ row }: { row: Task }) {
  if (row.status === 'FAILED') return 'runtime-row-failed'
  if (row.status === 'ORPHANED') return 'runtime-row-orphaned'
  if (row.status === 'RUNNING') return 'runtime-row-running'
  return ''
}

watch(
  () => detail.data.value,
  (task) => {
    if (!task || task.id !== detailId.value || task.attempts.length === 0) return
    const current = task.attempts.find((attempt) => attempt.id === selectedAttempt.value)
    const preferred = current
      ?? task.attempts.find((attempt) => ['FAILED', 'ORPHANED'].includes(attempt.status))
      ?? task.attempts[0]
    if (!preferred) return
    if (selectedAttempt.value !== preferred.id || logData.value?.attempt_id !== preferred.id) {
      void loadLog(preferred.id)
    }
  },
  { immediate: true },
)

watch(
  () => route.query.task,
  (taskId) => {
    if (typeof taskId === 'string' && taskId) openTask(taskId)
  },
  { immediate: true },
)

watch(
  () => route.query.status,
  (value) => {
    const next = typeof value === 'string' && taskStatuses.some((item) => item === value) ? value : ''
    if (status.value !== next) {
      status.value = next
      page.value = 1
    }
  },
)

onMounted(() => {
  clockTimer = window.setInterval(() => { now.value = Date.now() }, 1000)
})
onUnmounted(() => {
  if (clockTimer !== undefined) window.clearInterval(clockTimer)
})
</script>

<template>
  <div class="page-stack runtime-center">
    <ErrorState v-if="query.isError.value" :message="String(query.error.value)" />
    <template v-else>
      <section class="runtime-overview" aria-label="任务状态概览">
        <button
          v-for="card in statusCards"
          :key="card.label"
          class="runtime-stat"
          :class="[`tone-${card.tone}`, { active: status === card.status }]"
          type="button"
          @click="setStatus(card.status)"
        >
          <span>{{ card.label }}</span>
          <strong>{{ card.value }}</strong>
        </button>
      </section>

      <section class="panel table-panel">
        <header class="panel-heading">
          <div>
            <h2>任务运行与异常诊断</h2>
            <p>状态每 3 秒刷新；失败原因来自安全摘要，完整证据在任务详情中查看</p>
          </div>
          <el-select v-model="status" clearable placeholder="全部状态" style="width:170px" @change="page = 1">
            <el-option v-for="item in taskStatuses" :key="item" :label="item" :value="item" />
          </el-select>
        </header>
        <el-table :data="query.data.value?.items ?? []" :row-class-name="rowClassName" height="600">
          <el-table-column label="任务" min-width="135">
            <template #default="scope">
              <button class="task-link" type="button" @click="openDetail(scope.row)">
                <strong>{{ scope.row.task_type }}</strong>
                <span>{{ scope.row.id.slice(0, 8) }}</span>
              </button>
            </template>
          </el-table-column>
          <el-table-column label="关联" min-width="135">
            <template #default="scope">
              <div class="association-cell">
                <span v-if="scope.row.subject_kind" class="experiment-link">
                  {{ scope.row.subject_kind }} · {{ scope.row.subject_id?.slice(0, 8) }}
                </span>
                <span v-else class="muted-value">数据/系统任务</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="状态" width="118">
            <template #default="scope"><StatusBadge :status="scope.row.status" /></template>
          </el-table-column>
          <el-table-column label="阶段与进度" min-width="190">
            <template #default="scope">
              <div class="progress-cell">
                <span>{{ taskStage(scope.row) ?? '—' }}</span>
                <el-progress :percentage="progress(scope.row)" :stroke-width="5" :show-text="false" />
              </div>
            </template>
          </el-table-column>
          <el-table-column label="运行时长" width="112">
            <template #default="scope"><span class="tabular">{{ duration(scope.row.started_at, scope.row.completed_at) }}</span></template>
          </el-table-column>
          <el-table-column label="Worker / 心跳" min-width="165">
            <template #default="scope">
              <div class="runtime-meta"><span>{{ scope.row.worker_id ?? '—' }}</span><small>{{ formatTime(scope.row.heartbeat_at) }}</small></div>
            </template>
          </el-table-column>
          <el-table-column label="失败原因" min-width="235">
            <template #default="scope">
              <div v-if="errorCode(scope.row)" class="failure-cell">
                <code>{{ errorCode(scope.row) }}</code>
                <span>{{ errorMessage(scope.row) ?? (boolField(scope.row.error, 'retryable') ? '可安全重试' : '打开详情查看诊断') }}</span>
              </div>
              <span v-else class="muted-value">—</span>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="190" fixed="right">
            <template #default="scope">
              <el-button v-if="['QUEUED','RUNNING'].includes(scope.row.status)" text type="danger" @click="cancelTask(scope.row)">取消</el-button>
              <el-button v-if="['SUCCEEDED','FAILED','CANCELLED','ORPHANED'].includes(scope.row.status)" text type="primary" @click="retryTask(scope.row)">{{ scope.row.status === 'SUCCEEDED' ? '再次运行' : '重试' }}</el-button>
              <el-button v-if="['SUCCEEDED','FAILED','CANCELLED','ORPHANED'].includes(scope.row.status)" text type="danger" @click="deleteTask(scope.row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
        <div class="pagination-row">
          <el-pagination v-model:current-page="page" :page-size="25" :total="query.data.value?.total ?? 0" layout="prev, pager, next" />
        </div>
      </section>
    </template>

    <el-drawer v-model="detailOpen" title="任务运行详情" size="820px">
      <el-skeleton v-if="detail.isLoading.value" :rows="8" animated />
      <ErrorState v-else-if="detail.isError.value" :message="String(detail.error.value)" />
      <template v-else-if="detail.data.value">
        <div class="drawer-task">
          <div>
            <span class="eyebrow">{{ detail.data.value.task_type }}</span>
            <h2>{{ detail.data.value.id }}</h2>
            <p>{{ detail.data.value.subject_kind ? `${detail.data.value.subject_kind} · ${detail.data.value.subject_id}` : '数据或系统后台任务' }}</p>
          </div>
          <div class="drawer-task-actions">
            <StatusBadge :status="detail.data.value.status" />
          </div>
        </div>

        <section v-if="diagnosis" class="diagnostic-card">
          <div class="diagnostic-heading">
            <div>
              <span class="eyebrow">FAILURE DIAGNOSTIC</span>
              <h3>{{ diagnosis.message ?? diagnosis.code ?? '任务未提供失败消息' }}</h3>
            </div>
            <el-tag :type="diagnosis.retryable ? 'warning' : 'danger'" effect="light">
              {{ diagnosis.retryable ? '可重试' : diagnosis.retryable === false ? '需先处理原因' : '需人工确认' }}
            </el-tag>
          </div>
          <dl class="diagnostic-grid">
            <div><dt>错误码</dt><dd><code>{{ diagnosis.code ?? '—' }}</code></dd></div>
            <div><dt>失败阶段</dt><dd>{{ diagnosis.stage ?? '—' }}</dd></div>
            <div><dt>异常类型</dt><dd class="hash">{{ diagnosis.exception_type ?? '—' }}</dd></div>
            <div class="wide"><dt>处理建议</dt><dd>{{ diagnosis.remediation ?? fallbackRemediation(detail.data.value) }}</dd></div>
          </dl>
          <details v-if="diagnosis.traceback" class="traceback-details">
            <summary>展开完整 traceback</summary>
            <pre>{{ diagnosis.traceback }}</pre>
          </details>
        </section>

        <el-tabs v-model="activeTab" class="runtime-tabs">
          <el-tab-pane label="运行概况" name="overview">
            <el-descriptions :column="2" border size="small">
              <el-descriptions-item label="Worker">{{ detail.data.value.worker_id ?? '—' }}</el-descriptions-item>
              <el-descriptions-item label="当前阶段">{{ taskStage(detail.data.value) ?? '—' }}</el-descriptions-item>
              <el-descriptions-item label="开始时间">{{ formatTime(detail.data.value.started_at) }}</el-descriptions-item>
              <el-descriptions-item label="运行时长">{{ duration(detail.data.value.started_at, detail.data.value.completed_at) }}</el-descriptions-item>
              <el-descriptions-item label="最近心跳">{{ formatTime(detail.data.value.heartbeat_at) }}</el-descriptions-item>
              <el-descriptions-item label="完成时间">{{ formatTime(detail.data.value.completed_at) }}</el-descriptions-item>
              <el-descriptions-item label="关联对象" :span="2"><span class="hash">{{ detail.data.value.subject_kind ? `${detail.data.value.subject_kind}/${detail.data.value.subject_id}` : '—' }}</span></el-descriptions-item>
            </el-descriptions>
            <section class="parameter-panel">
              <header>
                <div>
                  <span class="eyebrow">TASK PAYLOAD</span>
                  <h3>任务参数</h3>
                </div>
                <span class="readonly-pill">READ ONLY</span>
              </header>
              <template v-if="dataUpdatePlan">
                <div class="update-parameter-summary">
                  <div><dt>更新模式</dt><dd>{{ dataUpdatePlan.window_mode === 'AUTO_INCREMENTAL' ? '自动增量' : '指定日期' }}</dd></div>
                  <div><dt>计划生成时间</dt><dd>{{ formatTime(dataUpdatePlan.planned_at) }}</dd></div>
                  <div class="wide"><dt>汇总范围</dt><dd>{{ dataUpdatePlan.start }} 至 {{ dataUpdatePlan.end }}</dd></div>
                </div>
                <el-table :data="dataUpdatePlan.dataset_windows" max-height="320" size="small" class="update-window-table">
                  <el-table-column prop="dataset" label="数据集" min-width="145" />
                  <el-table-column label="依据" width="100"><template #default="scope">{{ windowBasisLabel(scope.row.basis) }}</template></el-table-column>
                  <el-table-column label="当前状态" width="155"><template #default="scope">{{ windowState(scope.row) }}</template></el-table-column>
                  <el-table-column label="回看策略" width="145"><template #default="scope">{{ windowLookback(scope.row) }}</template></el-table-column>
                  <el-table-column label="执行窗口" min-width="245"><template #default="scope">{{ windowRange(scope.row) }}</template></el-table-column>
                </el-table>
                <el-alert
                  v-if="dataUpdatePlan.skipped_datasets.length"
                  title="自动计划跳过了尚未越过披露截止日的数据集"
                  :description="dataUpdatePlan.skipped_datasets.map((item) => `${item.dataset}：${item.trigger_date}`).join('；')"
                  type="info"
                  :closable="false"
                  show-icon
                  style="margin-top:12px"
                />
                <details class="parameter-json">
                  <summary>展开完整 JSON</summary>
                  <pre>{{ formattedPayload }}</pre>
                </details>
              </template>
              <template v-else-if="legacyDataUpdate">
                <el-alert
                  title="旧版动态自动窗口"
                  description="该历史任务没有固化各数据集执行窗口，原始参数保持不变；请从数据中心创建新任务，不要直接重试。"
                  type="warning"
                  :closable="false"
                  show-icon
                  style="margin-top:14px"
                />
                <details class="parameter-json">
                  <summary>展开历史 JSON</summary>
                  <pre>{{ formattedPayload }}</pre>
                </details>
              </template>
              <template v-else-if="parameterEntries.length">
                <dl class="parameter-grid">
                  <div v-for="([key, value]) in parameterEntries" :key="key" class="parameter-item">
                    <dt>{{ key }}</dt>
                    <dd><code>{{ parameterValue(value) }}</code></dd>
                  </div>
                </dl>
                <details class="parameter-json">
                  <summary>展开完整 JSON</summary>
                  <pre>{{ formattedPayload }}</pre>
                </details>
              </template>
              <div v-else class="parameter-empty">该任务没有显式参数。</div>
            </section>
          </el-tab-pane>
          <el-tab-pane label="尝试与日志" name="logs">
            <div v-if="detail.data.value.attempts.length" class="attempt-list">
              <button
                v-for="attempt in detail.data.value.attempts"
                :key="attempt.id"
                type="button"
                class="attempt-card"
                :class="{ selected: selectedAttempt === attempt.id }"
                @click="loadLog(attempt.id)"
              >
                <span><strong>#{{ attempt.attempt_no }}</strong><StatusBadge :status="attempt.status" /></span>
                <small>{{ attempt.worker_id ?? '无 Worker' }} · {{ duration(attempt.started_at, attempt.completed_at) }}</small>
                <code v-if="errorCode(attempt)">{{ errorCode(attempt) }}</code>
                <em>{{ attempt.has_log ? '有诊断日志' : '未生成日志' }}</em>
              </button>
            </div>
            <div v-else class="log-empty">任务尚未产生执行尝试。</div>

            <div v-if="selectedAttemptRecord" class="log-section">
              <div class="log-toolbar">
                <div>
                  <strong>尝试 #{{ selectedAttemptRecord.attempt_no }} 的尾部日志</strong>
                  <span v-if="logData">{{ logData.lines.length }} / {{ logData.total_lines }} 行</span>
                </div>
                <el-button size="small" :loading="logLoading" @click="loadLog(selectedAttempt)">刷新日志</el-button>
              </div>
              <el-alert v-if="logData?.truncated" title="当前仅显示尾部 500 行" type="warning" :closable="false" show-icon />
              <div v-if="logLoading && !logData" class="log-empty">正在读取受控任务日志…</div>
              <ErrorState v-else-if="logError" :message="logError" />
              <div v-else-if="!selectedAttemptRecord.has_log" class="log-empty">本次尝试没有生成诊断日志。</div>
              <div v-else-if="logData && !logData.available" class="log-empty">日志已登记，但文件当前不存在或不可用。</div>
              <pre v-else-if="logData?.lines.length" class="log-viewer">{{ logData.lines.join('\n') }}</pre>
              <div v-else class="log-empty">日志文件为空，暂无可显示内容。</div>
            </div>
          </el-tab-pane>
        </el-tabs>
      </template>
    </el-drawer>
  </div>
</template>

<style scoped>
.runtime-overview{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px}.runtime-stat{position:relative;min-height:92px;padding:16px 18px;border:1px solid var(--border);border-radius:12px;background:var(--surface);color:var(--muted);text-align:left;cursor:pointer;overflow:hidden}.runtime-stat::after{content:"";position:absolute;width:62px;height:62px;right:-18px;bottom:-24px;border-radius:50%;background:var(--accent,var(--dim));opacity:.11}.runtime-stat span{display:block;font-size:11px}.runtime-stat strong{display:block;margin-top:10px;color:var(--text);font-size:26px}.runtime-stat.active{border-color:var(--accent,var(--blue));box-shadow:0 0 0 2px color-mix(in srgb,var(--accent,var(--blue)) 12%,transparent)}.tone-blue{--accent:var(--blue)}.tone-cyan{--accent:var(--cyan)}.tone-danger{--accent:var(--danger)}.tone-warning{--accent:var(--warning)}
.task-link{display:flex;flex-direction:column;gap:4px;padding:0;border:0;background:none;color:var(--text);text-align:left;cursor:pointer}.task-link span{color:var(--dim);font:10px ui-monospace,Consolas,monospace}.task-link:hover strong{color:var(--cyan)}.association-cell{display:flex;flex-direction:column;align-items:flex-start;gap:4px}.experiment-link{display:inline-block;color:var(--blue);font-size:10px;text-decoration:none}.progress-cell span{width:76px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--dim);font-size:10px}.runtime-meta,.failure-cell{display:flex;flex-direction:column;gap:4px}.runtime-meta small,.failure-cell span{color:var(--dim);font-size:10px}.failure-cell code{color:var(--danger);font-size:10px;overflow-wrap:anywhere}.muted-value{color:var(--dim)}.pagination-row{display:flex;justify-content:flex-end;padding:14px 0}.runtime-center :deep(.runtime-row-failed td.el-table__cell){background:rgba(214,59,86,.045)}.runtime-center :deep(.runtime-row-orphaned td.el-table__cell){background:rgba(169,109,10,.06)}.runtime-center :deep(.runtime-row-running td.el-table__cell:first-child){box-shadow:inset 3px 0 var(--blue)}
.drawer-task{display:flex;justify-content:space-between;gap:18px;padding:18px;border:1px solid var(--border);border-radius:12px;background:var(--surface-raised)}.drawer-task h2{margin:6px 0 0;font:16px ui-monospace,Consolas,monospace;overflow-wrap:anywhere}.drawer-task p{margin:8px 0 0;color:var(--dim);font-size:11px}.drawer-task-actions{display:flex;flex-direction:column;align-items:flex-end;gap:12px}.drawer-task-actions a{text-decoration:none}.diagnostic-card{margin-top:14px;padding:18px;border:1px solid rgba(214,59,86,.28);border-radius:12px;background:rgba(214,59,86,.045)}.diagnostic-heading{display:flex;justify-content:space-between;gap:16px}.diagnostic-heading h3{margin:7px 0 0;font-size:15px;line-height:1.5}.diagnostic-grid{display:grid;grid-template-columns:1fr 1fr 1.4fr;gap:12px;margin:16px 0 0}.diagnostic-grid div{min-width:0}.diagnostic-grid .wide{grid-column:1/-1}.diagnostic-grid dt{color:var(--dim);font-size:10px}.diagnostic-grid dd{margin:5px 0 0;color:var(--muted);font-size:12px;overflow-wrap:anywhere}.diagnostic-grid code{color:var(--danger)}.traceback-details{margin-top:14px;border-top:1px solid rgba(214,59,86,.18);padding-top:12px}.traceback-details summary{color:var(--blue);font-size:11px;cursor:pointer}.traceback-details pre{max-height:320px;overflow:auto;margin:12px 0 0;padding:12px;border-radius:8px;background:#fff;color:#3c4858;font:11px/1.65 ui-monospace,Consolas,monospace;white-space:pre-wrap;overflow-wrap:anywhere}.runtime-tabs{margin-top:18px}.attempt-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-bottom:14px}.attempt-card{display:flex;flex-direction:column;align-items:stretch;gap:8px;padding:12px;border:1px solid var(--border);border-radius:9px;background:var(--surface);color:var(--muted);text-align:left;cursor:pointer}.attempt-card.selected{border-color:var(--blue);background:rgba(37,99,235,.045)}.attempt-card>span{display:flex;align-items:center;justify-content:space-between}.attempt-card small,.attempt-card em{color:var(--dim);font-size:10px;font-style:normal}.attempt-card code{color:var(--danger);font-size:10px}.log-section{display:flex;flex-direction:column;gap:10px}.log-toolbar{display:flex;align-items:center;justify-content:space-between}.log-toolbar>div{display:flex;flex-direction:column;gap:4px}.log-toolbar strong{font-size:12px}.log-toolbar span{color:var(--dim);font-size:10px}.log-empty{display:grid;min-height:120px;place-items:center;border:1px dashed var(--border);border-radius:9px;color:var(--dim);font-size:12px;background:var(--surface-raised)}
.parameter-panel{margin-top:14px;padding:16px;border:1px solid var(--border);border-radius:10px;background:var(--surface-raised)}.parameter-panel>header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px}.parameter-panel h3{margin:5px 0 0;font-size:14px}.readonly-pill{padding:5px 7px;border-radius:5px;color:#708198;background:#edf1f6;font-size:8px;letter-spacing:.1em}.parameter-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;margin:14px 0 0}.parameter-item{min-width:0;padding:10px 12px;border:1px solid var(--border);border-radius:8px;background:var(--surface)}.parameter-item dt{color:var(--dim);font-size:10px}.parameter-item dd{margin:6px 0 0;overflow:hidden;color:var(--muted);font-size:11px;overflow-wrap:anywhere}.parameter-item code{white-space:pre-wrap}.parameter-json{margin-top:12px;border-top:1px solid var(--border);padding-top:11px}.parameter-json summary{color:var(--blue);font-size:11px;cursor:pointer}.parameter-json pre{max-height:320px;overflow:auto;margin:10px 0 0;padding:12px;border-radius:8px;background:#fff;color:#3c4858;font:11px/1.65 ui-monospace,Consolas,monospace;white-space:pre-wrap;overflow-wrap:anywhere}.parameter-empty{display:grid;min-height:92px;place-items:center;margin-top:12px;border:1px dashed var(--border);border-radius:8px;color:var(--dim);font-size:11px;background:var(--surface)}
.update-parameter-summary{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:14px}.update-parameter-summary>div{padding:10px 12px;border:1px solid var(--border);border-radius:8px;background:var(--surface)}.update-parameter-summary .wide{grid-column:1/-1}.update-parameter-summary dt{color:var(--dim);font-size:10px}.update-parameter-summary dd{margin:6px 0 0;color:var(--muted);font-size:11px}.update-window-table{margin-top:12px}
@media (max-width:1360px){.runtime-overview{grid-template-columns:repeat(3,minmax(0,1fr))}}
</style>
