<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import VChart from 'vue-echarts'

import { api } from '../api'
import { axis, tooltip } from '../charts'
import ChartCard from '../components/ChartCard.vue'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import StrategyStudyTaskProgress from '../components/StrategyStudyTaskProgress.vue'
import { formatDuration, formatTime } from '../format'
import type { ArtifactRow, StrategyStudy, StrategyStudyReport, TaskDetail } from '../types'

type ArtifactPayload = {
  [key: string]: unknown
  items?: ArtifactRow[]
  page?: number
  page_size?: number
  total?: number
  value?: unknown
}
type MetricGroupName = '收益' | '风险' | '基准相对表现' | '交易执行' | '组合暴露'
type MetricDefinition = { label: string; description: string; group: MetricGroupName }

const metricDefinitions: Record<string, MetricDefinition> = {
  cumulative_return: { label: '累计收益', description: '研究区间内策略净值的总增长率。', group: '收益' },
  annualized_return: { label: '年化收益', description: '按交易日折算的策略几何年化收益。', group: '收益' },
  gross_cumulative_return: { label: '毛累计收益', description: '扣除费用前的策略累计收益。', group: '收益' },
  gross_annualized_return: { label: '毛年化收益', description: '扣除费用前的几何年化收益。', group: '收益' },
  positive_month_rate: { label: '正收益月份占比', description: '月度策略收益大于零的月份比例。', group: '收益' },
  annualized_volatility: { label: '年化波动率', description: '日收益标准差按 252 个交易日年化。', group: '风险' },
  max_drawdown: { label: '最大回撤', description: '策略净值相对历史峰值的最大跌幅。', group: '风险' },
  max_drawdown_duration_sessions: { label: '最长回撤期', description: '最长水下阶段包含的交易日数。', group: '风险' },
  time_under_water_rate: { label: '水下时间占比', description: '净值低于历史峰值的交易日比例。', group: '风险' },
  historical_daily_var_95_loss: { label: '历史 95% 单日 VaR', description: '历史法估计、正数表示损失幅度的 95% 单日 VaR。', group: '风险' },
  historical_daily_expected_shortfall_95_loss: { label: '历史 95% 单日 ES', description: '超过 95% VaR 阈值后的平均单日损失。', group: '风险' },
  sharpe_ratio: { label: 'Sharpe 比率', description: '相对无风险利率的单位波动年化收益。', group: '风险' },
  sortino_ratio: { label: 'Sortino 比率', description: '相对无风险利率的单位下行风险年化收益。', group: '风险' },
  calmar_ratio: { label: 'Calmar 比率', description: '年化收益与最大回撤绝对值之比。', group: '风险' },
  benchmark_cumulative_return: { label: '基准累计收益', description: '同区间基准净值的累计收益。', group: '基准相对表现' },
  benchmark_annualized_return: { label: '基准年化收益', description: '同区间基准的几何年化收益。', group: '基准相对表现' },
  geometric_excess_return: { label: '几何超额收益', description: '策略净值与基准净值之比的累计增长。', group: '基准相对表现' },
  annualized_geometric_excess_return: { label: '年化几何超额', description: '策略相对基准净值增长比率的年化结果。', group: '基准相对表现' },
  relative_cumulative_return: { label: '累计收益差', description: '策略累计收益减去基准累计收益。', group: '基准相对表现' },
  tracking_error: { label: '跟踪误差', description: '日主动收益标准差按 252 个交易日年化。', group: '基准相对表现' },
  information_ratio: { label: '信息比率', description: '年化主动收益与跟踪误差之比。', group: '基准相对表现' },
  beta: { label: 'Beta', description: '策略收益对基准收益变动的敏感度。', group: '基准相对表现' },
  jensen_alpha: { label: 'Jensen Alpha', description: '扣除 Beta 对应基准收益后的年化超额。', group: '基准相对表现' },
  active_max_drawdown: { label: '主动净值最大回撤', description: '策略相对基准净值比率的最大回撤。', group: '基准相对表现' },
  one_way_turnover: { label: '单边换手率', description: '成交金额按平均净资产折算的单边换手。', group: '交易执行' },
  annualized_turnover: { label: '年化换手率', description: '单边换手率按研究期交易日数年化。', group: '交易执行' },
  fee_rate: { label: '费用率', description: '总费用占研究期平均净资产的比例。', group: '交易执行' },
  cumulative_cost_drag: { label: '累计成本拖累', description: '毛净值与净净值之间的累计差额。', group: '交易执行' },
  annualized_cost_drag: { label: '年化成本拖累', description: '交易成本对年化收益造成的拖累。', group: '交易执行' },
  failed_fill_rate: { label: '未成交订单占比', description: '未能形成成交的订单比例。', group: '交易执行' },
  notional_fill_rate: { label: '金额成交率', description: '已成交名义金额占可定价请求金额的比例。', group: '交易执行' },
  priced_order_coverage_rate: { label: '可定价订单覆盖率', description: '具备参考价格的订单比例。', group: '交易执行' },
  average_cash_weight: { label: '平均现金权重', description: '研究期每日现金权重的平均值。', group: '组合暴露' },
  average_receivable_weight: { label: '平均分红应收权重', description: '已在除权日确认、尚未支付的税前现金分红占净资产的平均比例。', group: '组合暴露' },
  gross_dividend_cash_fen: { label: '税前现金分红', description: '逐事件四舍五入到分后确认的税前现金分红总额。', group: '收益' },
  stock_distribution_quantity: { label: '送转新增股数', description: '按登记数量乘总送转比例并向下取整后的新增股票数量。', group: '组合暴露' },
  fund_split_quantity: { label: '基金拆分新增份额', description: '由基金复权因子变化确定、在生效会话开盘前增加的基金份额。', group: '组合暴露' },
  discarded_fractional_stock_quantity: { label: '舍弃零碎份额', description: '送转或拆分按 FLOOR 规则舍弃的小数股份/份额总量。', group: '组合暴露' },
  max_position_weight: { label: '最大单一持仓权重', description: '研究期任一证券出现过的最大权重。', group: '组合暴露' },
  observations: { label: '有效观测数', description: '用于绩效分析的交易日观测数量。', group: '组合暴露' },
}

const artifactLabels: Record<string, string> = {
  performance: '每日绩效',
  rolling_performance: '252 日滚动绩效',
  drawdown_episodes: '回撤事件',
  monthly_returns: '月度收益',
  annual_returns: '年度收益',
  exposure_summary: '暴露明细',
  attribution: '证券归因',
  execution_summary: '成交质量',
  nav: '账户净值',
  orders: '订单',
  fills: '成交',
  holdings: '持仓',
  costs: '成本',
  dividends: '分红送转与基金拆分明细',
  quality_disclosure: '质量披露',
  metrics: '指标',
  manifest: 'Manifest',
}

const route = useRoute()
const router = useRouter()
const client = useQueryClient()
const studyId = computed(() => String(route.params.strategyStudyId))
const tab = ref('research')
const artifactType = ref('performance')
const artifactDimension = ref('')
const artifactPage = ref(1)

const detail = useQuery({
  queryKey: computed(() => ['strategy-study', studyId.value]),
  queryFn: () => api.get<StrategyStudy>(`/api/v1/strategy-studies/${studyId.value}`),
  refetchInterval: (query) => ['QUEUED', 'RUNNING'].includes((query.state.data as StrategyStudy | undefined)?.status ?? '') ? 3_000 : false,
})
const task = useQuery({
  queryKey: computed(() => ['strategy-study-task', detail.data.value?.task_id ?? '']),
  queryFn: () => api.get<TaskDetail>(`/api/v1/tasks/${detail.data.value?.task_id}`),
  enabled: computed(() => Boolean(detail.data.value?.task_id)),
  refetchInterval: (query) => {
    const current = query.state.data as TaskDetail | undefined
    return current && ['SUCCEEDED', 'FAILED', 'CANCELLED', 'ORPHANED'].includes(current.status) ? false : 2_500
  },
})
const report = useQuery({
  queryKey: computed(() => ['strategy-study-report', studyId.value]),
  queryFn: () => api.get<StrategyStudyReport>(`/api/v1/strategy-studies/${studyId.value}/report`),
  enabled: computed(() => detail.data.value?.status === 'SUCCEEDED'),
})
const artifactUrl = computed(() => {
  const dimension = artifactDimension.value ? `&dimension=${encodeURIComponent(artifactDimension.value)}` : ''
  return `/api/v1/strategy-studies/${studyId.value}/artifacts/${artifactType.value}?page=${artifactPage.value}&page_size=100${dimension}`
})
const artifact = useQuery({
  queryKey: computed(() => ['strategy-study-artifact', studyId.value, artifactType.value, artifactDimension.value, artifactPage.value]),
  queryFn: () => api.get<ArtifactPayload>(artifactUrl.value),
  enabled: computed(() => detail.data.value?.status === 'SUCCEEDED' && tab.value === 'evidence'),
})

const cancel = useMutation({
  mutationFn: () => api.post(`/api/v1/tasks/${detail.data.value?.task_id}/cancel`),
  onSuccess: async () => {
    ElMessage.success('取消请求已记录')
    await Promise.all([detail.refetch(), task.refetch()])
  },
})
const remove = useMutation({
  mutationFn: () => api.delete<{ strategy_study_id: string; status: 'DELETED' }>(`/api/v1/strategy-studies/${studyId.value}`),
  onSuccess: async () => {
    client.removeQueries({ queryKey: ['strategy-study', studyId.value] })
    await client.invalidateQueries({ queryKey: ['strategy-studies'] })
    ElMessage.success('策略研究已删除')
    await router.push('/strategy-studies')
  },
})

const terminal = computed(() => ['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(detail.data.value?.status ?? ''))
const duration = computed(() => formatDuration(detail.data.value?.started_at, detail.data.value?.completed_at))
const metricMap = computed(() => new Map((detail.data.value?.metrics ?? []).map((item) => [item.name, item])))
const coreMetricNames = ['annualized_return', 'annualized_geometric_excess_return', 'max_drawdown', 'sharpe_ratio', 'annualized_turnover', 'annualized_cost_drag'] as const
const headlineMetrics = computed(() => coreMetricNames.map((name) => ({
  name,
  ...metricDefinitions[name],
  metric: metricMap.value.get(name),
})))
const metricGroups = computed(() => {
  const groupNames: MetricGroupName[] = ['收益', '风险', '基准相对表现', '交易执行', '组合暴露']
  return groupNames.map((name) => ({
    name,
    items: (detail.data.value?.metrics ?? [])
      .filter((metric) => (metricDefinitions[metric.name]?.group ?? '交易执行') === name)
      .map((metric) => ({
        ...metric,
        label: metricDefinitions[metric.name]?.label ?? metric.name,
        description: metricDefinitions[metric.name]?.description ?? '该指标由可信分析产物计算。',
      })),
  })).filter((group) => group.items.length > 0)
})
const artifactTypes = computed(() => {
  const published = detail.data.value?.artifacts.map((item) => item.artifact_type) ?? []
  return [...new Set([...published, 'manifest'])].map((value) => ({ value, label: artifactLabels[value] ?? value }))
})
const dimensionOptions = computed(() => {
  if (artifactType.value === 'attribution') return ['SECURITY', 'INDUSTRY', 'FACTOR', 'CASH', 'COST']
  if (artifactType.value === 'exposure_summary') return ['SECURITY', 'CASH', 'RECEIVABLE', 'INDUSTRY', 'FACTOR']
  return []
})
const artifactRows = computed(() => artifact.data.value?.items ?? [])
const artifactColumns = computed(() => Object.keys(artifactRows.value[0] ?? {}))
const artifactJson = computed(() => {
  const payload = artifact.data.value
  if (!payload || payload.items) return null
  return payload.value === undefined ? payload : payload.value
})
const reportRange = computed(() => {
  const rows = report.data.value?.performance ?? []
  return rows.length ? `${rows[0].trade_date} → ${rows[rows.length - 1].trade_date} · ${rows.length.toLocaleString('zh-CN')} 个交易日` : '暂无绩效序列'
})
const majorDrawdownEpisodes = computed(() => [...(report.data.value?.drawdown_episodes ?? [])]
  .sort((left, right) => left.max_drawdown - right.max_drawdown || left.episode_index - right.episode_index)
  .slice(0, 8))

const topLegend = { top: 2, left: 'center' }
const fullZoom = [{ type: 'inside' }, { type: 'slider', height: 16, bottom: 2 }]
const navOption = computed(() => {
  const rows = report.data.value?.performance ?? []
  return {
    tooltip,
    legend: { ...topLegend, data: ['策略净值', '毛净值', '基准净值'] },
    grid: { left: 54, right: 22, top: 52, bottom: 72, containLabel: true },
    dataZoom: fullZoom,
    xAxis: { ...axis, type: 'category', data: rows.map((row) => row.trade_date), boundaryGap: false },
    yAxis: { ...axis, type: 'value', scale: true },
    series: [
      { name: '策略净值', type: 'line', showSymbol: false, data: rows.map((row) => row.nav), lineStyle: { width: 2, color: '#2563eb' } },
      { name: '毛净值', type: 'line', showSymbol: false, data: rows.map((row) => row.gross_nav), lineStyle: { width: 1, color: '#a96d0a', type: 'dashed' } },
      { name: '基准净值', type: 'line', showSymbol: false, data: rows.map((row) => row.benchmark_nav), lineStyle: { width: 1.5, color: '#087f79' } },
    ],
  }
})
const drawdownOption = computed(() => {
  const rows = report.data.value?.performance ?? []
  return {
    tooltip,
    legend: { ...topLegend, data: ['策略回撤', '主动净值回撤'] },
    grid: { left: 54, right: 20, top: 52, bottom: 45, containLabel: true },
    dataZoom: [{ type: 'inside' }],
    xAxis: { ...axis, type: 'category', data: rows.map((row) => row.trade_date), boundaryGap: false },
    yAxis: { ...axis, type: 'value', axisLabel: { color: '#66778D', formatter: (value: number) => `${(value * 100).toFixed(0)}%` } },
    series: [
      { name: '策略回撤', type: 'line', showSymbol: false, areaStyle: { color: 'rgba(214,59,86,.16)' }, lineStyle: { color: '#d63b56' }, data: rows.map((row) => row.drawdown) },
      { name: '主动净值回撤', type: 'line', showSymbol: false, lineStyle: { color: '#a96d0a' }, data: rows.map((row) => row.active_drawdown) },
    ],
  }
})
const rollingReturnOption = computed(() => {
  const rows = report.data.value?.rolling_performance ?? []
  return {
    tooltip,
    legend: { ...topLegend, data: ['策略年化', '基准年化', '年化超额'] },
    grid: { left: 54, right: 20, top: 52, bottom: 45, containLabel: true },
    dataZoom: [{ type: 'inside' }],
    xAxis: { ...axis, type: 'category', data: rows.map((row) => row.trade_date), boundaryGap: false },
    yAxis: { ...axis, type: 'value', axisLabel: { color: '#66778D', formatter: (value: number) => `${(value * 100).toFixed(0)}%` } },
    series: [
      { name: '策略年化', type: 'line', showSymbol: false, data: rows.map((row) => row.annualized_return) },
      { name: '基准年化', type: 'line', showSymbol: false, data: rows.map((row) => row.benchmark_annualized_return) },
      { name: '年化超额', type: 'line', showSymbol: false, data: rows.map((row) => row.annualized_excess_return) },
    ],
  }
})
const rollingRiskOption = computed(() => {
  const rows = report.data.value?.rolling_performance ?? []
  return {
    tooltip,
    legend: { ...topLegend, data: ['Sharpe', '信息比率', 'Beta'] },
    grid: { left: 48, right: 20, top: 52, bottom: 45, containLabel: true },
    dataZoom: [{ type: 'inside' }],
    xAxis: { ...axis, type: 'category', data: rows.map((row) => row.trade_date), boundaryGap: false },
    yAxis: { ...axis, type: 'value' },
    series: [
      { name: 'Sharpe', type: 'line', showSymbol: false, data: rows.map((row) => row.sharpe_ratio) },
      { name: '信息比率', type: 'line', showSymbol: false, data: rows.map((row) => row.information_ratio) },
      { name: 'Beta', type: 'line', showSymbol: false, data: rows.map((row) => row.beta), lineStyle: { type: 'dashed' } },
    ],
  }
})
const rollingRiskScaleOption = computed(() => {
  const rows = report.data.value?.rolling_performance ?? []
  return {
    tooltip,
    legend: { ...topLegend, data: ['年化波动率', '跟踪误差', '窗口最大回撤'] },
    grid: { left: 54, right: 20, top: 52, bottom: 45, containLabel: true },
    dataZoom: [{ type: 'inside' }],
    xAxis: { ...axis, type: 'category', data: rows.map((row) => row.trade_date), boundaryGap: false },
    yAxis: { ...axis, type: 'value', axisLabel: { color: '#66778D', formatter: (value: number) => `${(value * 100).toFixed(0)}%` } },
    series: [
      { name: '年化波动率', type: 'line', showSymbol: false, data: rows.map((row) => row.annualized_volatility) },
      { name: '跟踪误差', type: 'line', showSymbol: false, data: rows.map((row) => row.tracking_error) },
      { name: '窗口最大回撤', type: 'line', showSymbol: false, data: rows.map((row) => row.max_drawdown), lineStyle: { color: '#d63b56' } },
    ],
  }
})
const monthlyOption = computed(() => {
  const rows = report.data.value?.monthly_returns ?? []
  const years = [...new Set(rows.map((row) => String(row.year)))]
  const values = rows.map((row) => [Number(row.month ?? 1) - 1, years.indexOf(String(row.year)), row.portfolio_return])
  const max = Math.max(...rows.map((row) => Math.abs(row.portfolio_return)), 0.01)
  return {
    tooltip: { position: 'top', formatter: (params: { value: [number, number, number] }) => `${years[params.value[1]]}年${params.value[0] + 1}月<br/>${formatPercent(params.value[2])}` },
    grid: { left: 54, right: 18, top: 12, bottom: 55 },
    xAxis: { ...axis, type: 'category', data: Array.from({ length: 12 }, (_, index) => `${index + 1}月`), splitArea: { show: true } },
    yAxis: { ...axis, type: 'category', data: years, splitArea: { show: true } },
    visualMap: { min: -max, max, calculable: false, orient: 'horizontal', left: 'center', bottom: 0, inRange: { color: ['#16825f', '#f5f8fc', '#d83a52'] }, textStyle: { color: '#66778D', fontSize: 9 } },
    series: [{ type: 'heatmap', data: values, label: { show: true, formatter: (params: { value: [number, number, number] }) => `${(params.value[2] * 100).toFixed(1)}%`, fontSize: 9 } }],
  }
})
const annualOption = computed(() => {
  const rows = report.data.value?.annual_returns ?? []
  return {
    tooltip,
    legend: { ...topLegend, data: ['策略', '基准', '超额'] },
    grid: { left: 54, right: 18, top: 52, bottom: 34, containLabel: true },
    xAxis: { ...axis, type: 'category', data: rows.map((row) => String(row.year)) },
    yAxis: { ...axis, type: 'value', axisLabel: { color: '#66778D', formatter: (value: number) => `${(value * 100).toFixed(0)}%` } },
    series: [
      { name: '策略', type: 'bar', data: rows.map((row) => row.portfolio_return), itemStyle: { color: '#2563eb' } },
      { name: '基准', type: 'bar', data: rows.map((row) => row.benchmark_return), itemStyle: { color: '#087f79' } },
      { name: '超额', type: 'bar', data: rows.map((row) => row.relative_return), itemStyle: { color: '#a96d0a' } },
    ],
  }
})
const exposureOption = computed(() => {
  const rows = report.data.value?.exposure ?? []
  const dates = [...new Set(rows.map((row) => row.trade_date))]
  const keys = [...new Set(rows.map((row) => row.key))]
  const values = new Map(rows.map((row) => [`${row.trade_date}|${row.key}`, row.weight]))
  return {
    tooltip,
    legend: { ...topLegend, type: 'scroll', data: keys.map((key) => key === 'CASH' ? '现金' : key === 'DIVIDEND_RECEIVABLE' ? '分红应收' : key) },
    grid: { left: 48, right: 18, top: 58, bottom: 48, containLabel: true },
    dataZoom: [{ type: 'inside' }],
    xAxis: { ...axis, type: 'category', data: dates, boundaryGap: false },
    yAxis: { ...axis, type: 'value', max: 1, axisLabel: { color: '#66778D', formatter: (value: number) => `${(value * 100).toFixed(0)}%` } },
    series: keys.map((key) => ({
      name: key === 'CASH' ? '现金' : key === 'DIVIDEND_RECEIVABLE' ? '分红应收' : key,
      type: 'line',
      stack: 'exposure',
      showSymbol: false,
      areaStyle: {},
      data: dates.map((date) => values.get(`${date}|${key}`) ?? 0),
    })),
  }
})
const costOption = computed(() => {
  const rows = report.data.value?.performance ?? []
  return {
    tooltip,
    grid: { left: 52, right: 18, top: 20, bottom: 45 },
    dataZoom: [{ type: 'inside' }],
    xAxis: { ...axis, type: 'category', data: rows.map((row) => row.trade_date), boundaryGap: false },
    yAxis: { ...axis, type: 'value', axisLabel: { color: '#66778D', formatter: (value: number) => `${(value * 100).toFixed(2)}%` } },
    series: [{ name: '累计成本拖累', type: 'line', showSymbol: false, areaStyle: { color: 'rgba(169,109,10,.16)' }, lineStyle: { color: '#a96d0a' }, data: rows.map((row) => row.cumulative_cost_drag) }],
  }
})
const attributionOption = computed(() => {
  const rows = (report.data.value?.attribution ?? []).slice(0, 12).reverse()
  return {
    tooltip,
    grid: { left: 88, right: 22, top: 18, bottom: 28 },
    xAxis: { ...axis, type: 'value', axisLabel: { color: '#66778D', formatter: (value: number) => `${(value * 100).toFixed(1)}%` } },
    yAxis: { ...axis, type: 'category', data: rows.map((row) => row.key) },
    series: [{ name: '收益贡献', type: 'bar', data: rows.map((row) => ({ value: row.contribution_return, itemStyle: { color: row.contribution_return >= 0 ? '#d83a52' : '#16825f' } })) }],
  }
})

watch(artifactType, () => {
  artifactPage.value = 1
  artifactDimension.value = ''
})
watch(artifactDimension, () => { artifactPage.value = 1 })

function formatMetric(value: number | undefined, unit: string | null | undefined) {
  if (value === undefined) return '—'
  if (unit === 'ratio' || unit === 'percent') return formatPercent(value)
  if (Number.isInteger(value) && Math.abs(value) >= 1) return value.toLocaleString('zh-CN')
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 4 })
}
function formatPercent(value: number | null | undefined) {
  return value === null || value === undefined ? '—' : `${(value * 100).toFixed(2)}%`
}
function formatCell(value: unknown) {
  if (typeof value === 'number') return value.toLocaleString('zh-CN', { maximumFractionDigits: 6 })
  if (typeof value === 'boolean') return value ? '是' : '否'
  if (value === null || value === undefined) return '—'
  return String(value)
}
async function deleteStudy() {
  const study = detail.data.value
  if (!study) return
  try {
    await ElMessageBox.confirm(`将删除“${study.definition.name}”及其研究产物，此操作不可撤销。`, '确认删除策略研究', { type: 'error', confirmButtonText: '删除', cancelButtonText: '取消' })
  } catch {
    return
  }
  remove.mutate()
}
</script>

<template>
  <div class="page-stack strategy-detail">
    <ErrorState v-if="detail.error.value" :error="detail.error.value" />
    <template v-else-if="detail.data.value">
      <section class="panel detail-hero">
        <div class="hero-copy">
          <RouterLink class="back-link" to="/strategy-studies">← 返回策略研究</RouterLink>
          <span class="eyebrow">STRATEGY STUDY · {{ detail.data.value.stage }}</span>
          <h2>{{ detail.data.value.definition.name }}</h2>
          <p>{{ detail.data.value.definition.description || '暂无研究说明' }}</p>
          <div class="evidence-line">
            <span>{{ detail.data.value.definition.start_date }} → {{ detail.data.value.definition.end_date }}</span>
            <span>{{ detail.data.value.definition.strategy.strategy_id }} · {{ detail.data.value.definition.benchmark }}</span>
            <span class="hash">DATA {{ detail.data.value.catalog_hash.slice(0, 12) }}</span>
            <span>耗时 {{ duration }}</span>
            <span>{{ formatTime(detail.data.value.completed_at ?? detail.data.value.created_at) }}</span>
          </div>
        </div>
        <div class="hero-actions">
          <div class="hero-status"><StatusBadge :status="detail.data.value.status" /><span>{{ detail.data.value.stage }}</span></div>
          <div class="toolbar hero-toolbar">
            <RouterLink :to="`/strategy-studies/new?from=${studyId}`"><el-button>复制研究</el-button></RouterLink>
            <el-button v-if="['QUEUED', 'RUNNING'].includes(detail.data.value.status)" type="danger" plain :loading="cancel.isPending.value" @click="cancel.mutate()">取消</el-button>
            <el-button v-if="terminal" type="danger" plain :loading="remove.isPending.value" @click="deleteStudy">删除</el-button>
          </div>
        </div>
      </section>

      <StrategyStudyTaskProgress v-if="task.data.value && detail.data.value.status !== 'SUCCEEDED'" :task="task.data.value" mode="detail" />
      <section v-if="detail.data.value.status === 'FAILED'" class="panel failure-state"><strong>策略研究执行失败</strong><pre>{{ JSON.stringify(detail.data.value.error, null, 2) }}</pre></section>
      <section v-else-if="detail.data.value.status === 'CANCELLED' && !task.data.value" class="panel running-state"><StatusBadge status="CANCELLED" /><div><strong>策略研究已取消</strong><p>任务进度暂不可用。</p></div></section>
      <section v-else-if="['QUEUED', 'RUNNING'].includes(detail.data.value.status) && !task.data.value" class="panel running-state"><StatusBadge :status="detail.data.value.status" /><div><strong>{{ detail.data.value.stage }}</strong><p>正在读取策略研究任务进度。</p></div></section>

      <template v-if="detail.data.value.status === 'SUCCEEDED'">
        <ErrorState v-if="report.error.value" :error="report.error.value" />
        <template v-else-if="report.data.value">
          <section class="metrics-grid headline-grid">
            <article v-for="(item, index) in headlineMetrics" :key="item.name" class="metric-card" :class="{ 'tone-cyan': index === 1, 'tone-red': index === 2, 'tone-green': index === 3 }">
              <span class="metric-top">{{ item.label }}<i /></span>
              <strong class="metric-value metric-small">{{ formatMetric(item.metric?.value, item.metric?.unit) }}</strong>
              <p>{{ item.description }}</p>
            </article>
          </section>

          <section class="panel detail-tabs">
            <el-tabs v-model="tab">
              <el-tab-pane label="研究结果" name="research" />
              <el-tab-pane label="配置与证据" name="evidence" />
            </el-tabs>

            <div v-if="tab === 'research'" class="research-stack">
              <ChartCard title="策略净值、毛净值与基准净值" :subtitle="`完整可信区间 · ${reportRange}`" :empty="report.data.value.performance.length === 0">
                <VChart class="study-chart study-chart-large" :option="navOption" autoresize data-chart="nav" />
              </ChartCard>

              <div class="chart-grid">
                <ChartCard title="回撤曲线" subtitle="策略回撤与相对基准主动净值回撤" :empty="report.data.value.performance.length === 0"><VChart class="study-chart" :option="drawdownOption" autoresize data-chart="drawdown" /></ChartCard>
                <ChartCard title="252 日滚动收益" subtitle="固定 252 个交易日窗口；策略、基准与几何超额" :empty="report.data.value.rolling_performance.length === 0"><VChart class="study-chart" :option="rollingReturnOption" autoresize data-chart="rolling-return" /></ChartCard>
                <ChartCard title="252 日滚动相对风险" subtitle="Sharpe、信息比率与 Beta" :empty="report.data.value.rolling_performance.length === 0"><VChart class="study-chart" :option="rollingRiskOption" autoresize data-chart="rolling-risk" /></ChartCard>
                <ChartCard title="252 日滚动风险幅度" subtitle="年化波动率、跟踪误差与窗口最大回撤" :empty="report.data.value.rolling_performance.length === 0"><VChart class="study-chart" :option="rollingRiskScaleOption" autoresize data-chart="rolling-risk-scale" /></ChartCard>
                <section class="panel episode-panel">
                  <header class="panel-heading"><div><h2>主要回撤事件</h2><p>按回撤幅度排序，包含峰值、谷底、恢复日和持续时间</p></div></header>
                  <el-table :data="majorDrawdownEpisodes" empty-text="研究区间没有回撤事件">
                    <el-table-column prop="peak_date" label="峰值日" min-width="104" />
                    <el-table-column prop="trough_date" label="谷底日" min-width="104" />
                    <el-table-column prop="recovery_date" label="恢复日" min-width="104"><template #default="scope">{{ scope.row.recovery_date ?? '尚未恢复' }}</template></el-table-column>
                    <el-table-column label="最大回撤" min-width="90"><template #default="scope"><span class="down">{{ formatPercent(scope.row.max_drawdown) }}</span></template></el-table-column>
                    <el-table-column prop="underwater_sessions" label="水下日数" min-width="82" />
                  </el-table>
                </section>
              </div>

              <div class="chart-grid">
                <ChartCard title="月度收益热力图" subtitle="红色为正收益，绿色为负收益" :empty="report.data.value.monthly_returns.length === 0"><VChart class="study-chart" :option="monthlyOption" autoresize data-chart="monthly" /></ChartCard>
                <ChartCard title="年度收益" subtitle="策略、基准与相对收益" :empty="report.data.value.annual_returns.length === 0"><VChart class="study-chart" :option="annualOption" autoresize data-chart="annual" /></ChartCard>
              </div>

              <div class="chart-grid">
                <ChartCard title="证券、现金与分红应收暴露" subtitle="每日权重堆叠；除权已确认但未支付的分红作为独立应收暴露" :empty="report.data.value.exposure.length === 0"><VChart class="study-chart" :option="exposureOption" autoresize data-chart="exposure" /></ChartCard>
                <ChartCard title="累计成本拖累" subtitle="毛净值与扣费后净值的累计差额" :empty="report.data.value.performance.length === 0"><VChart class="study-chart" :option="costOption" autoresize data-chart="cost" /></ChartCard>
                <section class="panel execution-panel">
                  <header class="panel-heading"><div><h2>成交质量</h2><p>按买卖方向和原因代码汇总请求、成交及未成交数量</p></div></header>
                  <el-table :data="report.data.value.execution" max-height="320" empty-text="暂无成交汇总">
                    <el-table-column prop="side" label="方向" width="72" />
                    <el-table-column prop="reason_code" label="原因" min-width="110" />
                    <el-table-column prop="order_count" label="订单" width="72" />
                    <el-table-column prop="filled_quantity" label="成交数量" min-width="96" />
                    <el-table-column prop="unfilled_quantity" label="未成交" min-width="86" />
                  </el-table>
                </section>
                <ChartCard title="证券收益归因" subtitle="按绝对贡献排序的主要证券" :empty="report.data.value.attribution.length === 0"><VChart class="study-chart" :option="attributionOption" autoresize data-chart="attribution" /></ChartCard>
              </div>

              <section class="metric-groups">
                <article v-for="group in metricGroups" :key="group.name" class="panel metric-group">
                  <header class="panel-heading"><div><h2>{{ group.name }}</h2><p>口径说明随指标展示</p></div></header>
                  <dl>
                    <div v-for="item in group.items" :key="item.name">
                      <dt>{{ item.label }}<small>{{ item.description }}</small></dt>
                      <dd>{{ formatMetric(item.value, item.unit) }}</dd>
                    </div>
                  </dl>
                </article>
              </section>
            </div>

            <div v-else class="evidence-stack">
              <section class="config-grid">
                <article class="config-block"><h3>冻结定义</h3><pre>{{ JSON.stringify(detail.data.value.definition, null, 2) }}</pre></article>
                <article class="config-block"><h3>执行身份</h3><pre>{{ JSON.stringify({ id: detail.data.value.id, task_id: detail.data.value.task_id, config_hash: detail.data.value.config_hash, catalog_hash: detail.data.value.catalog_hash, manifest_hash: detail.data.value.manifest_hash }, null, 2) }}</pre></article>
              </section>
              <section class="quality-grid">
                <article class="evidence-card"><span>计算模式</span><strong>{{ report.data.value.quality.calculation_mode }}</strong><small>滚动窗口 {{ report.data.value.quality.rolling_window_sessions }} 个交易日</small></article>
                <article class="evidence-card"><span>尾部风险</span><strong>{{ report.data.value.quality.tail_risk_method }}</strong><small>无风险年化 {{ formatPercent(report.data.value.quality.risk_free_rate_annual) }}</small></article>
                <article class="evidence-card warning-card"><span>质量警告</span><strong>{{ report.data.value.quality.warnings.length }}</strong><small>{{ report.data.value.quality.warnings.join('；') || '无警告' }}</small></article>
              </section>
              <section class="panel artifact-register">
                <header class="panel-heading"><div><h2>Manifest 产物登记</h2><p>产物类型、行数、字节数与内容哈希均来自已登记证据</p></div></header>
                <el-table :data="detail.data.value.artifacts" empty-text="没有已登记产物">
                  <el-table-column label="产物" min-width="150"><template #default="scope">{{ artifactLabels[scope.row.artifact_type] ?? scope.row.artifact_type }}</template></el-table-column>
                  <el-table-column prop="relative_path" label="相对路径" min-width="220" show-overflow-tooltip />
                  <el-table-column prop="row_count" label="行数" width="90" />
                  <el-table-column prop="byte_count" label="字节数" width="110" />
                  <el-table-column label="SHA-256" min-width="180"><template #default="scope"><span class="hash">{{ scope.row.content_hash }}</span></template></el-table-column>
                </el-table>
              </section>
              <section class="panel artifact-browser">
                <header class="panel-heading"><div><h2>原始产物</h2><p>列表由 Manifest 动态生成；表格支持分页和维度过滤，JSON 保持结构化展示</p></div></header>
                <div class="toolbar artifact-toolbar">
                  <el-select v-model="artifactType" aria-label="选择产物" style="width:240px">
                    <el-option v-for="item in artifactTypes" :key="item.value" :label="item.label" :value="item.value" />
                  </el-select>
                  <el-select v-if="dimensionOptions.length" v-model="artifactDimension" clearable placeholder="全部维度" aria-label="筛选维度" style="width:180px">
                    <el-option v-for="dimension in dimensionOptions" :key="dimension" :label="dimension" :value="dimension" />
                  </el-select>
                  <span class="artifact-count">{{ artifact.data.value?.total === undefined ? '' : `${artifact.data.value.total} 行` }}</span>
                </div>
                <ErrorState v-if="artifact.error.value" :error="artifact.error.value" />
                <pre v-else-if="artifactJson !== null" class="json-viewer" data-artifact-json>{{ JSON.stringify(artifactJson, null, 2) }}</pre>
                <el-table v-else :data="artifactRows" max-height="560" empty-text="该产物没有表格记录">
                  <el-table-column v-for="column in artifactColumns" :key="column" :prop="column" :label="column" min-width="145" show-overflow-tooltip>
                    <template #default="scope">{{ formatCell(scope.row[column]) }}</template>
                  </el-table-column>
                </el-table>
                <el-pagination v-if="(artifact.data.value?.total ?? 0) > 100" v-model:current-page="artifactPage" :page-size="100" :total="artifact.data.value?.total ?? 0" layout="prev, pager, next, total" />
              </section>
            </div>
          </section>
        </template>
      </template>
    </template>
  </div>
</template>

<style scoped>
.strategy-detail{max-width:1500px;margin:0 auto}.detail-hero{display:grid;grid-template-columns:minmax(0,1fr) 300px;align-items:stretch;gap:38px;padding:26px 28px;border-color:rgba(37,99,235,.18);background:linear-gradient(118deg,#fff 0%,#f4f8ff 62%,#edf8f7 100%)}.detail-hero::after{content:"";position:absolute;width:230px;height:230px;right:70px;top:-175px;border-radius:50%;background:radial-gradient(circle,rgba(8,127,121,.14),rgba(37,99,235,0));pointer-events:none}.hero-copy,.hero-actions{position:relative;z-index:1}.hero-copy{min-width:0}.back-link{display:flex;width:max-content;margin-bottom:22px;color:var(--dim);font-size:11px;text-decoration:none;transition:color 140ms ease}.back-link:hover{color:var(--blue)}.detail-hero h2{margin:9px 0 7px;font-size:27px;letter-spacing:-.035em}.detail-hero p{max-width:760px;margin:0;color:var(--muted);font-size:13px;line-height:1.65}.evidence-line{display:flex;flex-wrap:wrap;gap:8px;margin-top:18px}.evidence-line>span{padding:6px 9px;border:1px solid rgba(128,151,181,.18);border-radius:6px;color:var(--dim);background:rgba(255,255,255,.72);font-size:10px}.hero-actions{display:flex;flex-direction:column;padding-left:24px;border-left:1px solid rgba(128,151,181,.2)}.hero-status{display:flex;align-items:center;justify-content:space-between;gap:10px}.hero-status>span{color:var(--dim);font:10px ui-monospace,Consolas,monospace}.hero-toolbar{justify-content:flex-end;margin-top:auto;padding-top:20px}.headline-grid{grid-template-columns:repeat(6,minmax(0,1fr))}.headline-grid .metric-card{min-height:142px}.headline-grid .metric-value{font-size:24px}.detail-tabs{padding-top:8px}.research-stack,.evidence-stack{display:flex;flex-direction:column;gap:14px}.study-chart{height:330px}.study-chart-large{height:410px}.chart-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.episode-panel,.execution-panel{min-height:404px}.metric-groups{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.metric-group:last-child:nth-child(odd){grid-column:1/-1}.metric-group dl{margin:0}.metric-group dl>div{display:grid;grid-template-columns:minmax(0,1fr) 140px;align-items:center;gap:18px;padding:11px 2px;border-top:1px solid #e8eef5}.metric-group dt{display:flex;flex-direction:column;gap:4px;color:var(--text);font-size:12px}.metric-group dt small{color:var(--dim);font-size:10px;line-height:1.5}.metric-group dd{margin:0;text-align:right;font-size:15px;font-weight:700;font-variant-numeric:tabular-nums}.config-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.config-block{min-width:0;padding:16px;border:1px solid var(--border);border-radius:10px;background:#f7f9fc}.config-block h3{margin:0 0 12px;font-size:13px}.config-block pre{max-height:420px;overflow:auto;margin:0;font:11px/1.55 ui-monospace,Consolas,monospace;white-space:pre-wrap;overflow-wrap:anywhere}.quality-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.evidence-card{display:flex;min-height:112px;flex-direction:column;padding:16px;border:1px solid var(--border);border-radius:10px;background:#f9fbfd}.evidence-card span{color:var(--dim);font-size:10px}.evidence-card strong{margin:11px 0 8px;font-size:17px}.evidence-card small{color:var(--muted);font-size:10px;line-height:1.5}.warning-card{border-color:rgba(169,109,10,.25);background:#fffbf3}.artifact-toolbar{margin-bottom:14px}.artifact-count{margin-left:auto;color:var(--dim);font-size:11px}.artifact-register .hash{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.json-viewer{max-height:620px;overflow:auto;margin:0;padding:16px;border:1px solid var(--border);border-radius:9px;background:#f7f9fc;color:#33445a;font:11px/1.65 ui-monospace,Consolas,monospace;white-space:pre-wrap;overflow-wrap:anywhere}.artifact-browser .el-pagination{margin-top:14px}.running-state{display:flex;align-items:center;gap:17px;padding:30px}.running-state p{margin:6px 0 0;color:var(--muted)}.failure-state{border-color:rgba(214,59,86,.3)}.failure-state pre{max-height:320px;overflow:auto;font:11px/1.6 ui-monospace,Consolas,monospace;white-space:pre-wrap}@media(max-width:1360px){.headline-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.chart-grid{grid-template-columns:1fr}.study-chart-large{height:380px}}@media(max-width:1120px){.detail-hero{grid-template-columns:1fr;gap:20px}.hero-actions{padding:18px 0 0;border-top:1px solid rgba(128,151,181,.2);border-left:0}.hero-toolbar{justify-content:flex-start}.headline-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.metric-groups,.config-grid{grid-template-columns:1fr}.metric-group:last-child:nth-child(odd){grid-column:auto}.quality-grid{grid-template-columns:1fr 1fr}.study-chart{height:310px}}
</style>
