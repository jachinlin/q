<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import { computed, defineComponent, h, ref } from 'vue'
import type { PropType } from 'vue'
import VChart from 'vue-echarts'
import { useRoute } from 'vue-router'
import { ElTable, ElTableColumn } from 'element-plus'

import { api } from '../api'
import { axis, tooltip } from '../charts'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime } from '../format'
import type { ResearchArtifactSeries, ResearchFamilyDetail, ResearchRun } from '../types'

const ArtifactTable = defineComponent({
  name: 'ArtifactTable',
  props: {
    series: { type: Object as PropType<ResearchArtifactSeries | undefined>, default: undefined },
    loading: { type: Boolean, default: false },
  },
  setup(props) {
    return () => {
      const rows = props.series?.items ?? []
      const columns = rows.length ? Object.keys(rows[0]).slice(0, 10) : []
      return h(ElTable, { data: rows.slice(0, 200), loading: props.loading, emptyText: '暂无已验证的 TEST 产物' }, () => columns.map((key) => h(ElTableColumn, { key, prop: key, label: key, minWidth: 120, showOverflowTooltip: true })))
    }
  },
})

const route = useRoute()
const client = useQueryClient()
const familyId = computed(() => String(route.params.familyId))
const executionId = ref('')
const activeTab = ref('protocol')

const detail = useQuery({
  queryKey: computed(() => ['research-family', familyId.value]),
  queryFn: () => api.get<ResearchFamilyDetail>(`/api/v1/research/families/${familyId.value}`),
  refetchInterval: 4000,
})
const execution = computed(() => {
  const items = detail.data.value?.executions ?? []
  return items.find((item) => item.id === executionId.value) ?? items[items.length - 1] ?? null
})
const selectedRun = computed<ResearchRun | null>(() => {
  const selected = execution.value?.selected_variant_id
  return execution.value?.runs.find((run) => run.phase === 'TEST' && run.variant_id === selected) ?? null
})

const seriesType = computed(() => {
  if (activeTab.value === 'signals') return 'signals'
  if (activeTab.value === 'risk') return 'portfolio'
  if (activeTab.value === 'cost') return 'execution'
  if (activeTab.value === 'performance') return 'performance'
  return ''
})
const series = useQuery({
  queryKey: computed(() => ['research-series', selectedRun.value?.id, seriesType.value]),
  queryFn: () => api.get<ResearchArtifactSeries>(`/api/v1/research/runs/${selectedRun.value?.id}/artifacts/${seriesType.value}?page=1&page_size=2000`),
  enabled: computed(() => Boolean(selectedRun.value?.manifest_hash && seriesType.value)),
})

const rerun = useMutation({
  mutationFn: () => api.post<{ execution_id: string }>(`/api/v1/research/families/${familyId.value}/executions`),
  onSuccess: async (result) => {
    executionId.value = result.execution_id
    ElMessage.success('已创建新的 execution')
    await client.invalidateQueries({ queryKey: ['research-family', familyId.value] })
  },
})

const validationRuns = computed(() => execution.value?.runs.filter((run) => run.phase === 'TRAIN_VALIDATION') ?? [])
const testMetrics = computed(() => selectedRun.value?.metrics.filter((metric) => metric.split === 'TEST') ?? [])
const selectedOrdinal = computed(() => execution.value?.variants.find((item) => item.id === execution.value?.selected_variant_id)?.ordinal)

const performanceOption = computed(() => {
  const items = series.data.value?.items ?? []
  const dateKey = items[0] && ('trade_date' in items[0] ? 'trade_date' : 'date')
  const valueKey = items[0] && (['nav', 'portfolio_nav', 'value'].find((key) => key in items[0]) ?? 'value')
  return {
    tooltip,
    grid: { left: 48, right: 18, top: 20, bottom: 40 },
    xAxis: { ...axis, type: 'category', data: items.map((item) => String(item[String(dateKey)] ?? '')) },
    yAxis: { ...axis, type: 'value', scale: true },
    dataZoom: [{ type: 'inside' }, { type: 'slider', height: 18, bottom: 4 }],
    series: [{ type: 'line', showSymbol: false, smooth: false, data: items.map((item) => Number(item[valueKey] ?? 0)), lineStyle: { color: '#2563eb', width: 1.5 }, areaStyle: { color: 'rgba(37,99,235,.08)' } }],
  }
})

function metric(run: ResearchRun | undefined, split: string, name: string) {
  return run?.metrics.find((item) => item.split === split && item.name === name)?.value ?? null
}

function number(value: number | null) {
  return value === null ? '—' : value.toFixed(4)
}
</script>

<template>
  <div class="page-stack research-detail">
    <ErrorState v-if="detail.error.value" :error="detail.error.value" />
    <template v-else-if="detail.data.value">
      <section class="detail-hero panel">
        <div>
          <RouterLink class="back-link" to="/research">← 研究中心</RouterLink>
          <h2>{{ detail.data.value.name }}</h2>
          <p>{{ detail.data.value.hypothesis }}</p>
          <div class="identity-line"><span>{{ detail.data.value.strategy_id }}</span><span>{{ detail.data.value.research_mode }}</span><span class="hash">{{ detail.data.value.config_hash.slice(0, 12) }}</span></div>
        </div>
        <div class="hero-actions">
          <el-select v-model="executionId" placeholder="最新 execution" style="width:220px">
            <el-option v-for="item in detail.data.value.executions" :key="item.id" :label="`${item.id.slice(0,8)} · ${item.status}`" :value="item.id" />
          </el-select>
          <el-button type="primary" :loading="rerun.isPending.value" @click="rerun.mutate()">创建新 execution</el-button>
        </div>
      </section>

      <section v-if="execution" class="metrics-grid research-metrics">
        <article class="metric-card"><span class="metric-top">执行状态<i /></span><strong class="metric-value status-value"><StatusBadge :status="execution.status" /></strong><p>{{ execution.id.slice(0, 12) }}</p></article>
        <article class="metric-card tone-cyan"><span class="metric-top">候选数量<i /></span><strong class="metric-value">{{ execution.variants.length }}</strong><p>TRAIN + VALIDATION</p></article>
        <article class="metric-card tone-green"><span class="metric-top">选中候选<i /></span><strong class="metric-value">{{ selectedOrdinal === undefined ? '—' : `#${selectedOrdinal + 1}` }}</strong><p>{{ execution.selection_reason ?? '等待选型' }}</p></article>
        <article class="metric-card"><span class="metric-top">TEST Sharpe<i /></span><strong class="metric-value">{{ number(testMetrics.find((item) => item.name === 'sharpe')?.value ?? null) }}</strong><p>不参与候选选择</p></article>
      </section>

      <section class="section-tabs">
        <el-tabs v-model="activeTab">
          <el-tab-pane label="研究协议" name="protocol">
            <div class="protocol-grid">
              <article><h3>规范配置</h3><pre>{{ JSON.stringify(detail.data.value.config, null, 2) }}</pre></article>
              <article v-if="execution"><h3>执行身份</h3><dl><dt>catalog_hash</dt><dd class="hash">{{ execution.catalog_hash }}</dd><dt>source_hash</dt><dd class="hash">{{ execution.source_hash }}</dd><dt>rulebook_hash</dt><dd class="hash">{{ execution.rulebook_hash }}</dd><dt>创建</dt><dd>{{ formatTime(execution.created_at) }}</dd></dl></article>
            </div>
          </el-tab-pane>
          <el-tab-pane label="候选与选型" name="variants">
            <el-table :data="execution?.variants ?? []" row-key="id">
              <el-table-column label="#" width="55"><template #default="scope">{{ scope.row.ordinal + 1 }}</template></el-table-column>
              <el-table-column label="候选" width="110"><template #default="scope"><span class="hash">{{ scope.row.id.slice(0,8) }}</span><el-tag v-if="scope.row.id === execution?.selected_variant_id" size="small" type="success">SELECTED</el-tag></template></el-table-column>
              <el-table-column label="参数" min-width="280"><template #default="scope"><code>{{ JSON.stringify(scope.row.parameters) }}</code></template></el-table-column>
              <el-table-column label="VALIDATION Sharpe" width="150"><template #default="scope">{{ number(metric(validationRuns.find((run) => run.variant_id === scope.row.id), 'VALIDATION', 'sharpe')) }}</template></el-table-column>
              <el-table-column label="校正 p-value" width="130"><template #default="scope">{{ number(validationRuns.find((run) => run.variant_id === scope.row.id)?.metrics.find((item) => item.split === 'VALIDATION')?.adjusted_p_value ?? null) }}</template></el-table-column>
              <el-table-column label="拒绝原因" min-width="160"><template #default="scope">{{ scope.row.rejection_reasons.join('；') || '—' }}</template></el-table-column>
            </el-table>
          </el-tab-pane>
          <el-tab-pane label="信号行为" name="signals"><ArtifactTable :series="series.data.value" :loading="series.isLoading.value" /></el-tab-pane>
          <el-tab-pane label="风险与组合" name="risk"><ArtifactTable :series="series.data.value" :loading="series.isLoading.value" /></el-tab-pane>
          <el-tab-pane label="成本与执行" name="cost"><ArtifactTable :series="series.data.value" :loading="series.isLoading.value" /></el-tab-pane>
          <el-tab-pane label="绩效与归因" name="performance">
            <div v-if="series.data.value?.items.length" class="performance-grid"><VChart class="chart" :option="performanceOption" autoresize /><ArtifactTable :series="series.data.value" :loading="series.isLoading.value" /></div>
            <div v-else class="empty-state"><strong>暂无 TEST 绩效</strong><small>仅展示锁定候选的独立 TEST 产物</small></div>
          </el-tab-pane>
          <el-tab-pane label="产物与审计" name="artifacts">
            <el-table :data="execution?.runs ?? []"><el-table-column label="运行" width="115"><template #default="scope"><span class="hash">{{ scope.row.id.slice(0,10) }}</span></template></el-table-column><el-table-column label="阶段" prop="phase" width="150" /><el-table-column label="状态" width="110"><template #default="scope"><StatusBadge :status="scope.row.status" /></template></el-table-column><el-table-column label="Manifest" min-width="220"><template #default="scope"><span class="hash">{{ scope.row.manifest_hash ?? '—' }}</span></template></el-table-column><el-table-column label="阶段审计" min-width="260"><template #default="scope"><code>{{ JSON.stringify(scope.row.stage_status) }}</code></template></el-table-column></el-table>
          </el-tab-pane>
        </el-tabs>
      </section>
    </template>
  </div>
</template>

<style scoped>
.detail-hero { display:flex; align-items:center; justify-content:space-between; gap:20px; padding:24px; }.detail-hero h2 { margin:10px 0 7px; font-size:24px; }.detail-hero p { max-width:800px; margin:0; color:var(--muted); font-size:12px; }.back-link { color:var(--cyan); font-size:11px; text-decoration:none; }.identity-line,.hero-actions { display:flex; align-items:center; gap:10px; }.identity-line { margin-top:14px; }.identity-line > span { padding:5px 8px; border:1px solid var(--border); border-radius:6px; color:var(--dim); font-size:10px; }.status-value { margin-top:21px; font-size:14px; }.protocol-grid { display:grid; grid-template-columns:minmax(0,1.6fr) minmax(320px,.7fr); gap:14px; }.protocol-grid article { padding:16px; border:1px solid var(--border); border-radius:9px; background:var(--surface-raised); }.protocol-grid h3 { margin:0 0 12px; font-size:12px; }.protocol-grid pre { max-height:540px; overflow:auto; margin:0; color:var(--muted); font:10px/1.6 ui-monospace,Consolas,monospace; }.protocol-grid dl { display:grid; grid-template-columns:105px 1fr; gap:10px; font-size:10px; }.protocol-grid dt { color:var(--dim); }.protocol-grid dd { overflow-wrap:anywhere; margin:0; }.performance-grid { display:grid; grid-template-columns:minmax(0,1fr); gap:14px; }.research-detail code { color:#41678f; font:10px/1.5 ui-monospace,Consolas,monospace; }
</style>
