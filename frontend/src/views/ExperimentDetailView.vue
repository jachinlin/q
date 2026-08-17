<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { api } from '../api'
import ExperimentBacktestPanel from '../components/ExperimentBacktestPanel.vue'
import ErrorState from '../components/ErrorState.vue'
import MetricCard from '../components/MetricCard.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatNumber, formatPercent, formatTime, shortHash } from '../format'
import type { ExperimentDetail } from '../types'

const route = useRoute()
const router = useRouter()
const client = useQueryClient()
const experimentId = computed(() => String(route.params.experimentId ?? ''))
const researchOpen = ref(false)
const research = reactive({ mark: 'UNREVIEWED', tags: '', note: '' })
const validTabs = new Set(['overview', 'backtest', 'artifacts', 'audit'])
const activeTab = computed({
  get: () => {
    const requested = String(route.query.tab ?? 'overview')
    return validTabs.has(requested) ? requested : 'overview'
  },
  set: (value: string) => { void router.replace({ query: { ...route.query, tab: value } }) },
})

const detail = useQuery({
  queryKey: computed(() => ['experiment', experimentId.value]),
  queryFn: () => api.get<ExperimentDetail>(`/api/v1/experiments/${experimentId.value}`),
  enabled: computed(() => Boolean(experimentId.value)),
  refetchInterval: (query) => ['QUEUED', 'RUNNING'].includes(String(query.state.data?.status)) ? 3000 : false,
})
const updateResearch = useMutation({
  mutationFn: () => api.patch(`/api/v1/experiments/${experimentId.value}/research`, {
    mark: research.mark,
    tags: research.tags.split(',').map((item) => item.trim()).filter(Boolean),
    note: research.note,
  }),
  onSuccess: async () => {
    researchOpen.value = false
    ElMessage.success('研究记录已更新')
    await client.invalidateQueries({ queryKey: ['experiment', experimentId.value] })
    await client.invalidateQueries({ queryKey: ['experiments'] })
  },
})
const clone = useMutation({
  mutationFn: () => api.post<{ experiment_id: string; task_id: string | null }>(`/api/v1/experiments/${experimentId.value}/clone`, { submit: true, priority: 0 }),
  onSuccess: async (result) => {
    ElMessage.success(`已创建新实验 · ${result.experiment_id.slice(0, 8)}`)
    await client.invalidateQueries({ queryKey: ['experiments'] })
    await router.push({ name: 'experiment-detail', params: { experimentId: result.experiment_id }, query: { tab: 'overview' } })
  },
})

const metricMap = computed(() => new Map((detail.data.value?.metrics ?? []).map((item) => [item.name, item.value])))
const metric = (name: string) => {
  const value = metricMap.value.get(name)
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}
const manifestHash = computed(() => detail.data.value?.artifacts.find((item) => item.name === 'manifest.json')?.content_hash ?? '')
const isSuccessful = computed(() => detail.data.value?.status === 'SUCCEEDED')
const isActive = computed(() => ['QUEUED', 'RUNNING'].includes(detail.data.value?.status ?? ''))
const durationText = computed(() => {
  const value = detail.data.value
  if (!value?.started_at) return '—'
  const end = value.completed_at ? new Date(value.completed_at).getTime() : Date.now()
  const seconds = Math.max(0, Math.floor((end - new Date(value.started_at).getTime()) / 1000))
  if (seconds < 60) return `${seconds} 秒`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} 分 ${seconds % 60} 秒`
  return `${Math.floor(minutes / 60)} 时 ${minutes % 60} 分`
})
const statusDescription = computed(() => {
  const status = detail.data.value?.status
  if (status === 'SUCCEEDED') return '结果已验证并发布，可进入回测分析。'
  if (status === 'RUNNING') return '计算任务正在独立 Worker 中推进。'
  if (status === 'QUEUED') return '实验已进入队列，等待 Worker 接管。'
  if (status === 'FAILED') return '本次运行未发布结果，请检查任务日志。'
  if (status === 'CANCELLED') return '实验已取消，登记身份继续保留。'
  return '实验身份已登记，等待后续处理。'
})
watch(() => detail.data.value?.status, (status) => {
  if (status && status !== 'SUCCEEDED' && activeTab.value === 'backtest') activeTab.value = 'overview'
})

function openResearch() {
  const value = detail.data.value
  if (!value) return
  research.mark = value.research_mark
  research.tags = value.tags.join(', ')
  research.note = value.note ?? ''
  researchOpen.value = true
}

async function cloneExperiment() {
  await ElMessageBox.confirm('将复制参数并绑定当前已验证数据，生成新的实验和后台任务。', '复制并提交实验', { type: 'warning' })
  clone.mutate()
}

function strategyLabel(value: string) {
  if (value === 'etf_rotation') return 'ETF 轮动'
  if (value === 'stock_multifactor') return '股票多因子'
  return value.replaceAll('_', ' ')
}
</script>

<template>
  <div class="page-stack experiment-detail">
    <div class="detail-nav">
      <RouterLink to="/experiments"><span>←</span> 实验中心</RouterLink><i>/</i><strong>{{ detail.data.value ? strategyLabel(detail.data.value.strategy_id) : '实验详情' }}</strong>
    </div>
    <ErrorState v-if="detail.isError.value" :message="String(detail.error.value)" />
    <el-skeleton v-else-if="detail.isLoading.value" :rows="12" animated />
    <template v-else-if="detail.data.value">
      <section class="detail-hero" :class="`status-${detail.data.value.status.toLowerCase()}`">
        <div class="detail-hero-main">
          <div class="identity-topline"><span class="detail-kicker"><i /> IMMUTABLE EXPERIMENT</span><StatusBadge :status="detail.data.value.status" /></div>
          <h2>{{ strategyLabel(detail.data.value.strategy_id) }}</h2>
          <div class="experiment-code"><span>ID</span><code>{{ detail.data.value.id }}</code></div>
          <p>{{ statusDescription }}</p>
          <div class="detail-actions">
            <el-button size="large" @click="openResearch">更新研究记录</el-button>
            <el-button type="primary" size="large" :loading="clone.isPending.value" @click="cloneExperiment">复制并提交 <span>↗</span></el-button>
          </div>
        </div>
        <aside class="detail-hero-summary">
          <span class="summary-label">PERFORMANCE SNAPSHOT</span>
          <div class="hero-return"><span>累计收益</span><strong :class="(metric('cumulative_return') ?? 0) >= 0 ? 'up' : 'down'">{{ formatPercent(metric('cumulative_return')) }}</strong></div>
          <div class="hero-mini-metrics">
            <div><span>SHARPE</span><strong>{{ formatNumber(metric('sharpe_ratio')) }}</strong></div>
            <div><span>MAX DD</span><strong>{{ formatPercent(metric('max_drawdown')) }}</strong></div>
            <div><span>DURATION</span><strong>{{ durationText }}</strong></div>
          </div>
          <div class="hero-mark"><span>研究标记</span><StatusBadge :status="detail.data.value.research_mark" /></div>
        </aside>
      </section>

      <section v-if="isActive || ['FAILED', 'CANCELLED'].includes(detail.data.value.status)" class="experiment-state" :class="{ 'is-failed': detail.data.value.status === 'FAILED' }">
        <div class="state-symbol"><i /></div>
        <div class="state-copy"><strong>{{ isActive ? '实验正在后台执行' : detail.data.value.status === 'FAILED' ? '实验执行失败' : '实验已取消' }}</strong><small>{{ isActive ? '状态会每 3 秒自动刷新，关闭页面不会中断任务。' : '本次实验没有发布新的回测结果。' }}</small></div>
        <RouterLink v-if="detail.data.value.latest_task" :to="{ path: '/tasks', query: { task: detail.data.value.latest_task.id } }"><el-button :type="detail.data.value.status === 'FAILED' ? 'danger' : 'primary'" plain>查看任务详情</el-button></RouterLink>
      </section>

      <section class="detail-tabs">
        <el-tabs v-model="activeTab">
          <el-tab-pane name="overview">
            <template #label><span class="tab-label">概览 <i>01</i></span></template>
            <div class="overview-stack">
              <div class="detail-metrics metrics-grid">
                <MetricCard label="累计收益" :value="formatPercent(metric('cumulative_return'))" hint="策略完整区间" :tone="(metric('cumulative_return') ?? 0) >= 0 ? 'red' : 'green'" />
                <MetricCard label="Sharpe Ratio" :value="formatNumber(metric('sharpe_ratio'))" hint="风险调整后收益" />
                <MetricCard label="最大回撤" :value="formatPercent(metric('max_drawdown'))" hint="历史最大水下幅度" tone="red" />
                <MetricCard label="运行耗时" :value="durationText" :hint="isActive ? '仍在运行' : '从任务开始至完成'" tone="cyan" />
              </div>
              <div class="overview-grid">
                <section class="panel research-note-card">
                  <header class="panel-heading"><div><span class="section-kicker">RESEARCH MEMO</span><h2>研究结论</h2><p>结论、标签与标记均进入实验审计</p></div><button type="button" @click="openResearch">编辑 →</button></header>
                  <blockquote :class="{ empty: !detail.data.value.note }">{{ detail.data.value.note || '尚未记录研究结论。完成结果复核后，可在这里沉淀假设、异常与下一步动作。' }}</blockquote>
                  <div class="research-tags"><StatusBadge :status="detail.data.value.research_mark" /><i v-for="tag in detail.data.value.tags" :key="tag"># {{ tag }}</i><span v-if="!detail.data.value.tags.length">暂无自定义标签</span></div>
                </section>
                <section class="panel lifecycle-card">
                  <header class="panel-heading"><div><span class="section-kicker">LIFECYCLE</span><h2>实验时间线</h2><p>本机时区显示</p></div></header>
                  <div class="lifecycle-list">
                    <div class="complete"><i /><span><strong>身份已登记</strong><small>{{ formatTime(detail.data.value.created_at) }}</small></span></div>
                    <div :class="{ complete: detail.data.value.started_at }"><i /><span><strong>任务开始</strong><small>{{ formatTime(detail.data.value.started_at) }}</small></span></div>
                    <div :class="{ complete: detail.data.value.completed_at }"><i /><span><strong>结果发布</strong><small>{{ formatTime(detail.data.value.completed_at) }}</small></span></div>
                  </div>
                  <RouterLink v-if="detail.data.value.latest_task" class="task-link" :to="{ path: '/tasks', query: { task: detail.data.value.latest_task.id } }"><span>最新任务</span><code>{{ detail.data.value.latest_task.id }}</code><b>→</b></RouterLink>
                </section>
              </div>
            </div>
          </el-tab-pane>
          <el-tab-pane name="backtest" :disabled="!isSuccessful" lazy>
            <template #label><span class="tab-label">回测分析 <i>02</i></span></template>
            <ExperimentBacktestPanel v-if="isSuccessful && manifestHash" :experiment-id="experimentId" :manifest-hash="manifestHash" />
            <div v-else class="empty-state"><strong>回测结果尚不可用</strong><small>只有成功并登记产物的实验可以读取分析结果。</small></div>
          </el-tab-pane>
          <el-tab-pane name="artifacts" lazy>
            <template #label><span class="tab-label">参数与产物 <i>{{ detail.data.value.artifacts.length }}</i></span></template>
            <div class="artifact-grid">
              <section class="panel config-panel"><header class="panel-heading"><div><span class="section-kicker">LOCKED CONFIG</span><h2>解析后参数</h2><p>提交时绑定的只读配置</p></div><span class="readonly-pill">READ ONLY</span></header><pre class="log-viewer config-viewer">{{ JSON.stringify(detail.data.value.config, null, 2) }}</pre></section>
              <section class="panel table-panel artifact-panel"><header class="panel-heading"><div><span class="section-kicker">ARTIFACT INDEX</span><h2>登记产物</h2><p>仅展示已登记内容身份</p></div><strong>{{ detail.data.value.artifacts.length }}</strong></header><el-table :data="detail.data.value.artifacts" height="440"><el-table-column prop="name" label="名称" min-width="180" /><el-table-column prop="type" label="类型" width="130" /><el-table-column label="内容哈希" min-width="130"><template #default="scope"><span class="hash">{{ shortHash(scope.row.content_hash) }}</span></template></el-table-column></el-table></section>
            </div>
          </el-tab-pane>
          <el-tab-pane name="audit" lazy>
            <template #label><span class="tab-label">审计记录 <i>{{ detail.data.value.audit.length }}</i></span></template>
            <section class="panel audit-panel"><header class="panel-heading"><div><span class="section-kicker">AUDIT TRAIL</span><h2>不可变事件流</h2><p>研究操作与状态变化均可追溯</p></div></header><el-timeline v-if="detail.data.value.audit.length"><el-timeline-item v-for="(item, index) in detail.data.value.audit" :key="index" :timestamp="formatTime(item.created_at)"><strong>{{ item.event_type }}</strong><small>{{ item.actor }}</small><pre v-if="Object.keys(item.details).length">{{ JSON.stringify(item.details, null, 2) }}</pre></el-timeline-item></el-timeline><div v-else class="audit-empty"><span>◎</span><strong>暂无审计事件</strong><small>更新研究记录后，事件会出现在这里。</small></div></section>
          </el-tab-pane>
        </el-tabs>
      </section>
    </template>

    <el-dialog v-model="researchOpen" title="更新研究记录" width="520">
      <el-form label-position="top"><el-form-item label="研究标记"><el-select v-model="research.mark" style="width:100%"><el-option v-for="item in ['UNREVIEWED', 'BASELINE', 'CANDIDATE', 'DISCARDED']" :key="item" :label="item" :value="item" /></el-select></el-form-item><el-form-item label="标签（逗号分隔）"><el-input v-model="research.tags" /></el-form-item><el-form-item label="研究结论"><el-input v-model="research.note" type="textarea" :rows="5" /></el-form-item></el-form>
      <template #footer><el-button @click="researchOpen = false">取消</el-button><el-button type="primary" :loading="updateResearch.isPending.value" @click="updateResearch.mutate()">保存并审计</el-button></template>
    </el-dialog>
  </div>
</template>

<style scoped>
.experiment-detail { --violet: #6d5bd0; }
.detail-nav { display: flex; align-items: center; gap: 9px; min-height: 22px; color: #9aa7b7; font-size: 10px; }.detail-nav a { display: flex; align-items: center; gap: 7px; color: #52708f; text-decoration: none; }.detail-nav a span { font-size: 15px; }.detail-nav a:hover { color: var(--blue); }.detail-nav strong { color: var(--muted); font-weight: 500; }
.detail-hero { position: relative; min-height: 254px; overflow: hidden; display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(390px, .65fr); border: 1px solid rgba(37,99,235,.16); border-radius: 16px; background: linear-gradient(118deg, #f9fbff 0%, #edf4ff 58%, #f3f1ff 100%); box-shadow: 0 15px 42px rgba(43,68,112,.08); }.detail-hero::before { content: ""; position: absolute; inset: 0; opacity: .5; background-image: radial-gradient(circle at 1px 1px, rgba(37,99,235,.12) 1px, transparent 0); background-size: 22px 22px; mask-image: linear-gradient(90deg, #000, transparent 62%); }.detail-hero::after { content: ""; position: absolute; width: 310px; height: 310px; right: -75px; top: -155px; border: 1px solid rgba(109,91,208,.16); border-radius: 50%; box-shadow: 0 0 0 48px rgba(109,91,208,.035), 0 0 0 96px rgba(37,99,235,.025); }
.detail-hero-main { position: relative; z-index: 1; padding: 31px 36px; }.identity-topline { display: flex; align-items: center; gap: 12px; }.detail-kicker,.section-kicker { color: #416b9d; font-size: 8px; font-weight: 800; letter-spacing: .17em; }.detail-kicker { display: flex; align-items: center; gap: 7px; }.detail-kicker i { width: 17px; height: 1px; background: var(--cyan); }.detail-hero-main h2 { margin: 17px 0 7px; color: #15233b; font-size: 31px; line-height: 1.15; letter-spacing: -.045em; }.experiment-code { display: flex; align-items: center; gap: 7px; }.experiment-code span { padding: 2px 5px; border-radius: 4px; color: #fff; background: #7186a3; font-size: 7px; font-weight: 800; }.experiment-code code { max-width: 520px; overflow: hidden; color: #587594; font-size: 10px; text-overflow: ellipsis; white-space: nowrap; }.detail-hero-main > p { margin: 13px 0 0; color: #657891; font-size: 11px; }.detail-actions { display: flex; align-items: center; gap: 9px; margin-top: 24px; }.detail-actions span { margin-left: 4px; }
.detail-hero-summary { position: relative; z-index: 2; margin: 17px; padding: 20px 21px; border: 1px solid rgba(255,255,255,.76); border-radius: 13px; background: rgba(255,255,255,.72); box-shadow: 0 14px 35px rgba(49,71,114,.09); backdrop-filter: blur(10px); }.summary-label { color: #7185a0; font-size: 8px; font-weight: 800; letter-spacing: .14em; }.hero-return { display: flex; align-items: end; justify-content: space-between; padding: 18px 0 15px; border-bottom: 1px solid #e2e9f2; }.hero-return > span { color: var(--muted); font-size: 10px; }.hero-return strong { font-size: 31px; line-height: 1; letter-spacing: -.04em; font-variant-numeric: tabular-nums; }.hero-mini-metrics { display: grid; grid-template-columns: repeat(3, 1fr); gap: 9px; padding: 14px 0; }.hero-mini-metrics div { min-width: 0; display: grid; gap: 5px; }.hero-mini-metrics span { color: #8b99ab; font-size: 7px; letter-spacing: .08em; }.hero-mini-metrics strong { overflow: hidden; color: #293a53; font-size: 11px; text-overflow: ellipsis; white-space: nowrap; font-variant-numeric: tabular-nums; }.hero-mark { display: flex; align-items: center; justify-content: space-between; padding-top: 12px; border-top: 1px solid #e2e9f2; }.hero-mark > span { color: var(--muted); font-size: 9px; }
.experiment-state { min-height: 82px; display: flex; align-items: center; gap: 14px; padding: 16px 18px; border: 1px solid #cedcf2; border-radius: 12px; background: #f4f8ff; }.state-symbol { width: 38px; height: 38px; display: grid; place-items: center; border-radius: 11px; color: var(--blue); background: #e1ecff; }.state-symbol i { width: 9px; height: 9px; border-radius: 50%; background: currentColor; box-shadow: 0 0 0 6px rgba(37,99,235,.12); animation: state-pulse 1.8s infinite; }.state-copy { flex: 1; }.experiment-state strong,.experiment-state small { display: block; }.experiment-state small { margin-top: 5px; color: var(--dim); font-size: 10px; }.experiment-state.is-failed { border-color: rgba(214,59,86,.24); background: rgba(214,59,86,.05); }.experiment-state.is-failed .state-symbol { color: var(--danger); background: rgba(214,59,86,.1); }.experiment-state.is-failed .state-symbol i { box-shadow: 0 0 0 6px rgba(214,59,86,.1); animation: none; }
.detail-tabs { border: 1px solid var(--border); border-radius: 13px; padding: 0 18px 18px; background: var(--surface); box-shadow: 0 8px 24px rgba(42,64,99,.04); }.detail-tabs :deep(> .el-tabs > .el-tabs__header) { margin: 0 -18px 18px; padding: 0 20px; background: #fbfcfe; border-radius: 13px 13px 0 0; }.detail-tabs :deep(> .el-tabs > .el-tabs__header .el-tabs__nav-wrap::after) { height: 1px; background: #e3eaf2; }.detail-tabs :deep(> .el-tabs > .el-tabs__header .el-tabs__item) { height: 52px; }.tab-label { display: flex; align-items: center; gap: 7px; }.tab-label i { min-width: 17px; height: 17px; display: inline-grid; place-items: center; padding: 0 4px; border-radius: 5px; color: #8291a5; background: #edf1f6; font-size: 7px; font-style: normal; }.is-active .tab-label i { color: #fff; background: var(--cyan); }
.overview-stack { display: flex; flex-direction: column; gap: 14px; padding-top: 1px; }.detail-metrics .metric-card { min-height: 122px; }.overview-grid { display: grid; grid-template-columns: minmax(0, 1.5fr) minmax(310px, .7fr); gap: 14px; }.research-note-card,.lifecycle-card { min-height: 256px; }.panel-heading .section-kicker { display: block; margin-bottom: 7px; }.research-note-card .panel-heading button { border: 0; color: var(--blue); background: transparent; font-size: 9px; cursor: pointer; }.research-note-card blockquote { min-height: 105px; margin: 2px 0 16px; padding: 14px 16px; border: 0; border-left: 2px solid #93aee0; border-radius: 0 8px 8px 0; color: #43566f; background: #f7f9fc; font-size: 12px; line-height: 1.8; white-space: pre-wrap; }.research-note-card blockquote.empty { color: #8290a3; font-style: italic; }.research-tags { display: flex; align-items: center; gap: 7px; flex-wrap: wrap; }.research-tags i { padding: 4px 7px; border-radius: 6px; color: #4e6e96; background: #edf3fa; font-size: 8px; font-style: normal; }.research-tags > span:last-child { color: var(--dim); font-size: 9px; }
.lifecycle-list { position: relative; display: grid; gap: 16px; margin: 4px 0 17px; }.lifecycle-list::before { content: ""; position: absolute; left: 5px; top: 8px; bottom: 8px; width: 1px; background: #dfe6ef; }.lifecycle-list > div { position: relative; z-index: 1; display: flex; gap: 11px; }.lifecycle-list > div > i { width: 11px; height: 11px; margin-top: 3px; border: 3px solid #eef2f6; border-radius: 50%; background: #aab5c2; }.lifecycle-list > div.complete > i { border-color: #dcefe8; background: var(--success); }.lifecycle-list span { display: flex; flex: 1; justify-content: space-between; gap: 8px; }.lifecycle-list strong { font-size: 10px; }.lifecycle-list small { color: var(--dim); font-size: 9px; }.task-link { display: grid; grid-template-columns: auto 1fr auto; align-items: center; gap: 8px; padding: 10px; border: 1px solid #e1e8f1; border-radius: 8px; color: var(--muted); background: #f9fbfd; text-decoration: none; }.task-link span { font-size: 8px; }.task-link code { overflow: hidden; color: #4e6d91; font-size: 8px; text-overflow: ellipsis; white-space: nowrap; }.task-link b { color: var(--blue); }
.artifact-grid { display: grid; grid-template-columns: minmax(360px,.8fr) minmax(520px,1.2fr); gap: 14px; padding-top: 1px; }.config-viewer { max-height: 440px; }.readonly-pill { padding: 5px 7px; border-radius: 5px; color: #708198; background: #edf1f6; font-size: 7px; letter-spacing: .1em; }.artifact-panel .panel-heading > strong { color: #2d5d9b; font-size: 22px; }.audit-panel { min-height: 420px; margin-top: 1px; }.audit-panel small { display: block; margin-top: 4px; color: var(--dim); }.audit-panel pre { margin: 8px 0 0; color: var(--muted); font: 10px/1.6 ui-monospace,Consolas,monospace; white-space: pre-wrap; }.audit-empty { min-height: 300px; display: grid; place-content: center; text-align: center; }.audit-empty span { color: #8ba1bd; font-size: 29px; }.audit-empty strong { margin: 8px 0 5px; font-size: 12px; }.audit-empty small { color: var(--dim); font-size: 9px; }
@keyframes state-pulse { 50% { box-shadow: 0 0 0 10px rgba(37,99,235,0); } }
@media (max-width: 1360px) { .detail-hero { grid-template-columns: 1fr 350px; }.overview-grid,.artifact-grid { grid-template-columns: 1fr; } }
@media (prefers-reduced-motion: reduce) { .state-symbol i { animation: none; } }
</style>
