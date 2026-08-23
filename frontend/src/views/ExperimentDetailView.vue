<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import { computed, ref } from 'vue'
import { useRoute } from 'vue-router'
import VChart from 'vue-echarts'

import { api } from '../api'
import { axis, tooltip } from '../charts'
import ChartCard from '../components/ChartCard.vue'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import type { ExperimentAggregate, ExperimentRun, ResearchMark } from '../types'

type ArtifactRow = Record<string, unknown>
type ArtifactPayload = { items?: ArtifactRow[]; page?: number; page_size?: number; total?: number; value?: unknown }

const route = useRoute()
const queryClient = useQueryClient()
const experimentId = computed(() => String(route.params.experimentId))
const detail = useQuery({
  queryKey: ['experiment', experimentId],
  queryFn: () => api.get<ExperimentAggregate>(`/api/v1/experiments/${experimentId.value}`),
  refetchInterval: 3000,
})
const selectedRunId = ref('')
const tab = ref('protocol')
const runYaml = ref('')
const compareIds = ref<string[]>([])
const executionArtifact = ref('fills')
const selectedRun = computed<ExperimentRun | undefined>(() =>
  detail.data.value?.runs.find((item) => item.id === selectedRunId.value) ?? detail.data.value?.runs.at(-1),
)
const isFactorStudy = computed(() => detail.data.value?.experiment.definition.kind === 'FACTOR_STUDY')
const artifactType = computed(() => (isFactorStudy.value
  ? ({ signals: 'summary', portfolio: 'coverage', execution: 'correlation', performance: 'ic' } as Record<string, string>)[tab.value]
  : ({ signals: 'signals', portfolio: 'holdings', execution: executionArtifact.value, performance: 'performance', attribution: 'attribution' } as Record<string, string>)[tab.value]))
const artifact = useQuery({
  queryKey: ['run-artifact', selectedRunId, artifactType],
  queryFn: () => api.get<ArtifactPayload>(`/api/v1/runs/${selectedRun.value?.id}/artifacts/${artifactType.value}?page=1&page_size=500`),
  enabled: computed(() => Boolean(selectedRun.value?.manifest_hash && artifactType.value)),
})
const artifactRows = computed(() => artifact.data.value?.items ?? [])
const artifactColumns = computed(() => Object.keys(artifactRows.value[0] ?? {}).slice(0, 12))
const artifactTotal = computed(() => artifact.data.value?.total ?? artifactRows.value.length)
const formatCell = (value: unknown) => {
  if (value == null) return '—'
  if (typeof value === 'number') return Number.isInteger(value) ? String(value) : value.toFixed(6)
  return String(value)
}
const metricLabel = (metric: ExperimentRun['metrics'][number]) => {
  const suffix = metric.unit ? ` ${metric.unit}` : ''
  return `${metric.value.toFixed(6)}${suffix}`
}
const chartOption = computed(() => {
  const rows = artifactRows.value
  if (!rows.length) return null
  if (!isFactorStudy.value && tab.value === 'performance') {
    return {
      tooltip,
      legend: { data: ['累计收益', '回撤'] },
      grid: { left: 55, right: 24, top: 40, bottom: 42 },
      xAxis: { type: 'category', data: rows.map((row) => String(row.trade_date)), ...axis },
      yAxis: { type: 'value', ...axis, axisLabel: { formatter: (value: number) => `${(value * 100).toFixed(1)}%` } },
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18 }],
      series: [
        { name: '累计收益', type: 'line', symbol: 'none', data: rows.map((row) => row.cumulative_return) },
        { name: '回撤', type: 'line', symbol: 'none', data: rows.map((row) => row.drawdown) },
      ],
    }
  }
  if (!isFactorStudy.value && tab.value === 'execution') {
    const counts = new Map<string, number>()
    for (const row of rows) {
      const reason = String(row.reason_code ?? row.side ?? 'UNKNOWN')
      counts.set(reason, (counts.get(reason) ?? 0) + 1)
    }
    return {
      tooltip,
      grid: { left: 55, right: 24, top: 24, bottom: 70 },
      xAxis: { type: 'category', data: [...counts.keys()], ...axis, axisLabel: { rotate: 25 } },
      yAxis: { type: 'value', ...axis },
      series: [{ type: 'bar', data: [...counts.values()], itemStyle: { color: '#5279d8' } }],
    }
  }
  if (isFactorStudy.value && tab.value === 'signals') {
    return {
      tooltip,
      legend: { data: ['Rank IC', '多空收益'] },
      grid: { left: 55, right: 24, top: 40, bottom: 70 },
      xAxis: { type: 'category', data: rows.map((row) => `${row.factor_ref}/${row.horizon}`), ...axis, axisLabel: { rotate: 25 } },
      yAxis: { type: 'value', ...axis },
      series: [
        { name: 'Rank IC', type: 'bar', data: rows.map((row) => row.rank_ic_mean) },
        { name: '多空收益', type: 'bar', data: rows.map((row) => row.long_short_mean) },
      ],
    }
  }
  if (isFactorStudy.value && tab.value === 'portfolio') {
    return {
      tooltip,
      grid: { left: 55, right: 24, top: 24, bottom: 42 },
      xAxis: { type: 'category', data: rows.map((row) => String(row.signal_date)), ...axis },
      yAxis: { type: 'value', min: 0, max: 1, ...axis, axisLabel: { formatter: (value: number) => `${(value * 100).toFixed(0)}%` } },
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18 }],
      series: [{ type: 'line', symbol: 'none', data: rows.map((row) => row.coverage) }],
    }
  }
  if (isFactorStudy.value && tab.value === 'performance') {
    return {
      tooltip,
      legend: { data: ['Rank IC', 'Pearson IC'] },
      grid: { left: 55, right: 24, top: 40, bottom: 42 },
      xAxis: { type: 'category', data: rows.map((row) => String(row.signal_date)), ...axis },
      yAxis: { type: 'value', ...axis },
      dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18 }],
      series: [
        { name: 'Rank IC', type: 'line', symbol: 'none', data: rows.map((row) => row.rank_ic) },
        { name: 'Pearson IC', type: 'line', symbol: 'none', data: rows.map((row) => row.pearson_ic) },
      ],
    }
  }
  if (isFactorStudy.value && tab.value === 'execution') {
    const factors = [...new Set(rows.flatMap((row) => [String(row.factor_x), String(row.factor_y)]))].sort()
    return {
      tooltip,
      grid: { left: 100, right: 30, top: 20, bottom: 80 },
      xAxis: { type: 'category', data: factors, ...axis, axisLabel: { rotate: 25 } },
      yAxis: { type: 'category', data: factors, ...axis },
      visualMap: { min: -1, max: 1, calculable: true, orient: 'horizontal', left: 'center', bottom: 0 },
      series: [{
        type: 'heatmap',
        data: rows.map((row) => [factors.indexOf(String(row.factor_x)), factors.indexOf(String(row.factor_y)), row.correlation]),
      }],
    }
  }
  return null
})
const refresh = async () => { await queryClient.invalidateQueries({ queryKey: ['experiment', experimentId] }) }
const rerun = useMutation({ mutationFn: (id: string) => api.post(`/api/v1/runs/${id}/rerun`), onSuccess: async () => { ElMessage.success('已创建新 Run'); await refresh() } })
const cancel = useMutation({ mutationFn: (taskId: string) => api.post(`/api/v1/tasks/${taskId}/cancel`), onSuccess: async () => { ElMessage.success('已请求取消'); await refresh() } })
const mark = useMutation({ mutationFn: ({ id, value }: { id: string; value: ResearchMark }) => api.patch(`/api/v1/runs/${id}/research`, { mark: value }), onSuccess: refresh })
const addRun = useMutation({ mutationFn: () => api.post(`/api/v1/experiments/${experimentId.value}/runs`, { yaml: runYaml.value }), onSuccess: async () => { ElMessage.success('派生 Run 已入队'); runYaml.value = ''; await refresh() } })
const comparison = useMutation({ mutationFn: () => api.post('/api/v1/experiments/compare', { run_ids: compareIds.value }) })
function selectForCompare(rows: ExperimentRun[]) { compareIds.value = rows.map((item) => item.id) }
</script>

<template>
  <div class="page-stack">
    <ErrorState v-if="detail.error.value" :error="detail.error.value" />
    <template v-else-if="detail.data.value">
      <section class="panel detail-head">
        <div><span class="eyebrow">{{ detail.data.value.experiment.definition.kind }}</span><h2>{{ detail.data.value.experiment.definition.name }}</h2><p>{{ detail.data.value.experiment.definition.description }}</p></div>
        <div><strong>{{ detail.data.value.runs.length }}</strong><small> RUNS</small></div>
      </section>
      <section class="panel">
        <div class="panel-heading"><div><h2>Run 时间线</h2><p>TEST 使用只审计告警，不参与自动选型；预算 {{ detail.data.value.experiment.definition.governance.test_budget }} 次。</p></div><el-button :disabled="compareIds.length < 2" @click="comparison.mutate(); tab = 'compare'">比较所选 Run</el-button></div>
        <el-table :data="detail.data.value.runs" highlight-current-row @current-change="(row: ExperimentRun) => selectedRunId = row.id" @selection-change="selectForCompare">
          <el-table-column type="selection" width="46" /><el-table-column label="Run" width="150"><template #default="scope"><span class="hash">{{ scope.row.id.slice(0, 12) }}</span></template></el-table-column><el-table-column label="状态" width="115"><template #default="scope"><StatusBadge :status="scope.row.status" /></template></el-table-column><el-table-column prop="stage" label="阶段" width="150" /><el-table-column label="TEST" width="80"><template #default="scope">{{ scope.row.uses_test_region ? '是' : '否' }}</template></el-table-column><el-table-column prop="research_mark" label="标记" width="120" /><el-table-column label="操作" min-width="290"><template #default="scope"><el-button v-if="scope.row.task_id && ['QUEUED', 'RUNNING'].includes(scope.row.status)" size="small" type="danger" plain @click="cancel.mutate(scope.row.task_id)">取消</el-button><el-button size="small" @click="rerun.mutate(scope.row.id)">重跑</el-button><el-dropdown @command="(value: ResearchMark) => mark.mutate({ id: scope.row.id, value })"><el-button size="small">标记</el-button><template #dropdown><el-dropdown-menu><el-dropdown-item v-for="value in ['UNREVIEWED', 'BASELINE', 'CANDIDATE', 'DISCARDED']" :key="value" :command="value">{{ value }}</el-dropdown-item></el-dropdown-menu></template></el-dropdown></template></el-table-column>
        </el-table>
      </section>
      <section v-if="selectedRun?.metrics.length" class="metrics-grid">
        <article v-for="metric in selectedRun.metrics.slice(0, 8)" :key="metric.name" class="metric-card"><span class="metric-top">{{ metric.name }}<i /></span><strong class="metric-value metric-small">{{ metricLabel(metric) }}</strong><p v-if="metric.adjusted_p_value != null">校正 p={{ metric.adjusted_p_value.toFixed(6) }}</p><p v-else>Run 指标</p></article>
      </section>
      <section class="panel">
        <el-tabs v-model="tab"><el-tab-pane label="协议/配置" name="protocol" /><el-tab-pane :label="isFactorStudy ? '摘要' : '信号'" name="signals" /><el-tab-pane :label="isFactorStudy ? '覆盖率' : '持仓'" name="portfolio" /><el-tab-pane :label="isFactorStudy ? '相关性' : '成本/执行'" name="execution" /><el-tab-pane :label="isFactorStudy ? 'IC' : '绩效'" name="performance" /><el-tab-pane v-if="!isFactorStudy" label="归因" name="attribution" /><el-tab-pane label="产物" name="artifacts" /><el-tab-pane label="Run 比较" name="compare" /><el-tab-pane label="新增 Run" name="new-run" /></el-tabs>
        <pre v-if="tab === 'protocol'">{{ JSON.stringify({ protocol: detail.data.value.experiment.definition, run: selectedRun }, null, 2) }}</pre>
        <pre v-else-if="tab === 'compare'">{{ JSON.stringify(comparison.data.value ?? { message: '至少选择两个 Run 后比较' }, null, 2) }}</pre>
        <div v-else-if="tab === 'new-run'" class="run-editor"><el-input v-model="runYaml" type="textarea" :rows="18" placeholder="粘贴严格 Run YAML" /><el-button type="primary" :loading="addRun.isPending.value" @click="addRun.mutate()">创建 Run</el-button></div>
        <div v-else-if="tab === 'artifacts'" class="artifact-list"><el-table :data="selectedRun?.artifacts ?? []" empty-text="等待 Run 成功发布"><el-table-column prop="artifact_type" label="类型" width="180" /><el-table-column prop="relative_path" label="相对路径" min-width="240" /><el-table-column prop="row_count" label="行数" width="100" /><el-table-column prop="byte_count" label="字节数" width="120" /><el-table-column label="SHA-256" min-width="240"><template #default="scope"><span class="hash">{{ scope.row.content_hash }}</span></template></el-table-column></el-table></div>
        <template v-else>
          <div v-if="!isFactorStudy && tab === 'execution'" class="artifact-switch"><el-radio-group v-model="executionArtifact" size="small"><el-radio-button value="orders">订单</el-radio-button><el-radio-button value="fills">成交/拒绝</el-radio-button><el-radio-button value="costs">成本</el-radio-button></el-radio-group></div>
          <ErrorState v-if="artifact.error.value" :error="artifact.error.value" />
          <div v-else-if="artifact.isLoading.value" class="empty-state">正在读取并复核可信产物…</div>
          <template v-else>
            <ChartCard v-if="chartOption" :title="`${artifactType} 图表`" :subtitle="`可信 Manifest 分页读取 · 共 ${artifactTotal} 行`"><VChart class="artifact-chart" :option="chartOption" autoresize /></ChartCard>
            <el-table :data="artifactRows" max-height="560" :empty-text="selectedRun?.manifest_hash ? '该产物没有记录' : '等待 Run 成功发布'">
              <el-table-column v-for="column in artifactColumns" :key="column" :prop="column" :label="column" min-width="145" show-overflow-tooltip><template #default="scope">{{ formatCell(scope.row[column]) }}</template></el-table-column>
            </el-table>
            <p v-if="artifactTotal > artifactRows.length" class="page-note">当前展示前 {{ artifactRows.length }} / {{ artifactTotal }} 行。</p>
          </template>
        </template>
      </section>
    </template>
  </div>
</template>

<style scoped>
.detail-head{display:flex;justify-content:space-between;align-items:center}.detail-head h2{margin:8px 0}.detail-head p{color:var(--muted)}.detail-head>div:last-child strong{font-size:34px}.detail-head small{color:var(--dim)}
pre{max-height:620px;overflow:auto;padding:14px;border-radius:8px;background:var(--surface-raised);font:11px/1.55 ui-monospace,Consolas,monospace}.run-editor{display:flex;flex-direction:column;gap:12px;align-items:flex-end}.artifact-switch{display:flex;justify-content:flex-end;margin-bottom:12px}.artifact-chart{height:340px}.artifact-list,.page-note{margin-top:12px}.page-note{color:var(--dim);font-size:12px}.metric-small{font-size:20px;overflow-wrap:anywhere}
</style>
