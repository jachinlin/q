<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed } from 'vue'

import { api } from '../api'
import ErrorState from '../components/ErrorState.vue'
import MetricCard from '../components/MetricCard.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatDate, formatNumber, formatTime, shortHash } from '../format'
import type { Overview, Task } from '../types'

type AttentionItem = {
  key: string
  title: string
  description: string
  to: string
  tone: 'danger' | 'warning'
}

const query = useQuery({
  queryKey: ['overview'],
  queryFn: () => api.get<Overview>('/api/v1/overview'),
  refetchInterval: 5_000,
})
const data = computed(() => query.data.value)

const gateReasonLabel: Record<string, string> = {
  VALIDATED: '当前 Catalog 已通过全局校验，可以提交和运行研究任务。',
  NEVER_VALIDATED: '当前数据目录从未通过全局校验，请先完成 validate-all。',
  CATALOG_CHANGED: 'Canonical 内容已变化，必须重新完成全局校验。',
  VALIDATION_FAILED: '最近一次全局校验未通过，请处理阻断问题后重试。',
}
const freshnessLabel = {
  CURRENT: '当前',
  STALE: '滞后',
  MISSING: '缺失',
  UNKNOWN: '未知',
} as const
const taskTypeLabel: Record<string, string> = {
  DATA_BOOTSTRAP: '数据初始化',
  DATA_UPDATE: '数据更新',
  DATA_VALIDATION: '数据校验',
  EXPERIMENT_RUN: '实验运行',
}

const taskCounts = computed(() => data.value?.tasks.status_counts)
const activeTaskCount = computed(() =>
  (taskCounts.value?.QUEUED ?? 0)
  + (taskCounts.value?.RUNNING ?? 0)
  + (taskCounts.value?.CANCEL_REQUESTED ?? 0),
)
const failureCount = computed(() => (taskCounts.value?.FAILED ?? 0) + (taskCounts.value?.ORPHANED ?? 0))
const freshnessTone = computed<'blue' | 'cyan' | 'red' | 'green'>(() => {
  const status = data.value?.freshness.status
  if (status === 'CURRENT') return 'green'
  if (status === 'MISSING') return 'red'
  if (status === 'STALE') return 'cyan'
  return 'blue'
})
const gateDescription = computed(() => {
  const reason = data.value?.gate.reason
  return reason ? (gateReasonLabel[reason] ?? `研究门状态：${reason}`) : '正在读取研究门证据。'
})
const workerEvidence = computed(() => {
  const worker = data.value?.worker
  if (worker) return `${worker.worker_id ?? '未命名 Worker'} · 最近心跳 ${formatTime(worker.heartbeat_at)}`
  const update = data.value?.last_successful_update
  return update ? `最近数据更新完成于 ${formatTime(update.completed_at)}` : '尚无 Worker 心跳或成功更新证据'
})
const attentionItems = computed<AttentionItem[]>(() => {
  const overview = data.value
  if (!overview) return []
  const items: AttentionItem[] = []
  if (overview.gate.status === 'BLOCKED') {
    items.push({
      key: 'gate',
      title: '研究门未开放',
      description: gateReasonLabel[overview.gate.reason] ?? overview.gate.reason,
      to: '/data',
      tone: 'danger',
    })
  }
  const blocking = overview.latest_quality_run?.blocking_issue_count ?? 0
  if (blocking > 0) {
    items.push({
      key: 'quality',
      title: `${blocking} 个质量阻断项`,
      description: '打开最近一次质量运行，定位数据集与规则。',
      to: '/data',
      tone: 'danger',
    })
  }
  const missing = overview.freshness.counts.MISSING ?? 0
  if (missing > 0) {
    items.push({
      key: 'missing',
      title: `${missing} 个数据集缺失`,
      description: '数据资产尚未形成完整 Canonical 水位。',
      to: '/data',
      tone: 'danger',
    })
  }
  const stale = overview.freshness.counts.STALE ?? 0
  if (stale > 0) {
    items.push({
      key: 'stale',
      title: `${stale} 个数据集滞后`,
      description: '新鲜度告警不阻断读取，但应安排数据更新。',
      to: '/data',
      tone: 'warning',
    })
  }
  const failed = overview.tasks.status_counts.FAILED ?? 0
  if (failed > 0) {
    items.push({
      key: 'failed',
      title: `${failed} 个失败任务`,
      description: '查看诊断和日志，确认原因后再安全重试。',
      to: '/tasks?status=FAILED',
      tone: 'danger',
    })
  }
  const orphaned = overview.tasks.status_counts.ORPHANED ?? 0
  if (orphaned > 0) {
    items.push({
      key: 'orphaned',
      title: `${orphaned} 个孤儿任务`,
      description: '确认 Worker 已停止并检查临时产物。',
      to: '/tasks?status=ORPHANED',
      tone: 'warning',
    })
  }
  return items
})

function taskProgress(task: Task) {
  const completed = Number(task.progress.completed ?? 0)
  const total = Number(task.progress.total ?? 0)
  return total > 0 ? Math.max(0, Math.min(100, Math.round(completed / total * 100))) : 0
}

function taskStage(task: Task) {
  const stage = task.progress.stage
  return typeof stage === 'string' && stage ? stage : '等待执行'
}

function taskName(task: Task) {
  return taskTypeLabel[task.task_type] ?? task.task_type
}

</script>

<template>
  <div class="page-stack overview-page">
    <section v-if="query.isLoading.value" class="panel overview-loading" aria-label="正在加载研究工作台">
      <el-skeleton :rows="9" animated />
    </section>
    <ErrorState v-else-if="query.isError.value" :message="String(query.error.value)" />

    <template v-else-if="data">
      <section class="overview-hero" :class="{ blocked: data.gate.status === 'BLOCKED' }">
        <div class="readiness-copy">
          <span class="eyebrow">SYSTEM READINESS</span>
          <div class="readiness-title">
            <h2>{{ data.gate.status === 'READY' ? '研究环境已就绪' : '研究环境需要处理' }}</h2>
            <StatusBadge :status="data.gate.status" />
          </div>
          <p>{{ gateDescription }}</p>
          <div class="readiness-evidence">
            <span>最新交易日 <strong>{{ formatDate(data.latest_trade_date) }}</strong></span>
            <span>最近验证 <strong>{{ formatTime(data.gate.validated_at) }}</strong></span>
            <span>Catalog <strong class="hash">{{ shortHash(data.gate.catalog_hash) }}</strong></span>
          </div>
        </div>
      </section>

      <div class="metrics-grid overview-metrics">
        <MetricCard
          label="数据新鲜度"
          :value="freshnessLabel[data.freshness.status]"
          :hint="`${data.freshness.counts.CURRENT} 当前 · ${data.freshness.counts.STALE} 滞后 · ${data.freshness.counts.MISSING} 缺失`"
          :tone="freshnessTone"
        />
        <MetricCard
          label="活动任务"
          :value="formatNumber(activeTaskCount)"
          :hint="`${taskCounts?.RUNNING ?? 0} 运行 · ${taskCounts?.QUEUED ?? 0} 排队`"
          tone="cyan"
        />
        <MetricCard
          label="异常任务"
          :value="formatNumber(failureCount)"
          :hint="`${taskCounts?.FAILED ?? 0} 失败 · ${taskCounts?.ORPHANED ?? 0} 孤儿`"
          :tone="failureCount > 0 ? 'red' : 'green'"
        />
        <MetricCard
          label="Canonical 数据集"
          :value="formatNumber(data.dataset_count)"
          hint="研究运行只通过统一 Repository 读取"
        />
      </div>

      <div class="overview-operations-grid">
        <section class="panel activity-panel">
          <header class="panel-heading">
            <div><h2>正在执行</h2><p>活动任务每 5 秒刷新，点击可直接打开运行详情</p></div>
            <RouterLink to="/tasks"><el-button text type="primary">运行中心</el-button></RouterLink>
          </header>
          <div v-if="data.tasks.active.length" class="activity-list">
            <RouterLink
              v-for="task in data.tasks.active"
              :key="task.id"
              class="activity-row"
              :to="`/tasks?task=${encodeURIComponent(task.id)}`"
            >
              <div class="activity-identity">
                <strong>{{ taskName(task) }}</strong>
                <span class="hash">{{ task.id.slice(0, 8) }}</span>
              </div>
              <StatusBadge :status="task.status" />
              <div class="activity-progress">
                <div><span>{{ taskStage(task) }}</span><strong>{{ taskProgress(task) }}%</strong></div>
                <el-progress :percentage="taskProgress(task)" :stroke-width="5" :show-text="false" />
              </div>
              <time>{{ formatTime(task.updated_at) }}</time>
            </RouterLink>
          </div>
          <div v-else class="overview-empty compact">
            <strong>当前没有活动任务</strong>
            <span>新的数据更新或研究任务链会显示在这里。</span>
          </div>
          <footer class="worker-evidence"><i />{{ workerEvidence }}</footer>
        </section>

        <section class="panel attention-panel">
          <header class="panel-heading"><div><h2>待处理事项</h2><p>只呈现会影响研究可信度或运行稳定性的事项</p></div></header>
          <div v-if="attentionItems.length" class="attention-list">
            <RouterLink v-for="item in attentionItems" :key="item.key" :to="item.to" class="attention-item" :class="`tone-${item.tone}`">
              <i />
              <span><strong>{{ item.title }}</strong><small>{{ item.description }}</small></span>
              <b aria-hidden="true">›</b>
            </RouterLink>
          </div>
          <div v-else class="overview-empty healthy">
            <span class="healthy-mark">✓</span>
            <strong>当前没有阻断事项</strong>
            <span>研究门、数据新鲜度和任务状态均无需立即处理。</span>
          </div>
        </section>
      </div>

      <section class="panel research-launch-panel">
        <header class="panel-heading"><div><h2>研究入口</h2><p>策略实验与独立因子研究分别冻结配置、任务和不可变产物。</p></div><span class="hash">{{ shortHash(data.gate.catalog_hash) }}</span></header>
        <div class="three-grid">
          <article class="benchmark-card"><h3>股票多因子</h3><strong>CROSS SECTION</strong><p>动态股票池 · 价值/动量 · Alpha-Risk-Cost 优化</p></article>
          <article class="benchmark-card"><h3>双均线趋势</h3><strong>LONG / FLAT</strong><p>固定标的 · 时序方向信号 · 状态变化调仓</p></article>
          <article class="benchmark-card"><h3>ETF 轮动</h3><strong>ALLOCATION</strong><p>多周期动量 · 趋势过滤 · 月频约束投影</p></article>
        </div>
        <div class="toolbar launch-action"><RouterLink to="/factor-studies/new"><el-button>创建因子研究</el-button></RouterLink><span class="spacer" /><RouterLink to="/experiments/new"><el-button type="primary">使用模板创建策略实验</el-button></RouterLink></div>
      </section>
    </template>

    <section v-else class="panel overview-empty">
      <strong>暂时无法读取总览</strong><span>Overview API 未返回可用数据。</span>
    </section>
  </div>
</template>
