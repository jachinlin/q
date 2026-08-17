<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed } from 'vue'
import VChart from 'vue-echarts'

import { api } from '../api'
import { axis, tooltip } from '../charts'
import { formatNumber, formatPercent, shortHash } from '../format'
import type { Backtest } from '../types'
import ChartCard from './ChartCard.vue'
import ErrorState from './ErrorState.vue'
import MetricCard from './MetricCard.vue'

const props = defineProps<{ experimentId: string; manifestHash: string }>()
const result = useQuery({
  queryKey: computed(() => ['backtest', props.experimentId, props.manifestHash]),
  queryFn: () => api.get<Backtest>(`/api/v1/experiments/${props.experimentId}/backtest`),
})
const navOption = computed(() => ({
  tooltip,
  legend: { top: 0, right: 0, textStyle: { color: '#66778d', fontSize: 10 }, data: ['策略净值', '基准净值'] },
  grid: { left: 52, right: 20, top: 42, bottom: 35 },
  xAxis: { type: 'category', data: result.data.value?.nav.map((item) => item.trade_date) ?? [], boundaryGap: false, ...axis, splitLine: { show: false } },
  yAxis: { type: 'value', scale: true, ...axis },
  dataZoom: [{ type: 'inside' }],
  series: [
    { name: '策略净值', type: 'line', showSymbol: false, smooth: 0.18, lineStyle: { color: '#0f9f98', width: 2 }, areaStyle: { color: 'rgba(15,159,152,.10)' }, data: result.data.value?.nav.map((item) => item.portfolio_nav) ?? [] },
    { name: '基准净值', type: 'line', showSymbol: false, lineStyle: { color: '#64748b', width: 1.5 }, data: result.data.value?.nav.map((item) => item.benchmark_nav) ?? [] },
  ],
}))
const drawdownOption = computed(() => ({
  tooltip,
  grid: { left: 52, right: 20, top: 15, bottom: 35 },
  xAxis: { type: 'category', data: result.data.value?.drawdown.map((item) => item.trade_date) ?? [], boundaryGap: false, ...axis, splitLine: { show: false } },
  yAxis: { type: 'value', ...axis, axisLabel: { color: '#66778d', formatter: (value: number) => `${(value * 100).toFixed(0)}%` } },
  series: [{ type: 'line', showSymbol: false, lineStyle: { color: '#d63b56', width: 1.5 }, areaStyle: { color: 'rgba(214,59,86,.12)' }, data: result.data.value?.drawdown.map((item) => item.drawdown) ?? [] }],
}))
const metric = (name: string) => {
  const value = result.data.value?.metrics[name]
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}
const metricText = (name: string) => {
  const value = result.data.value?.metrics[name]
  return typeof value === 'string' && value ? value : '—'
}
const annualReturns = computed(() => {
  const value = result.data.value?.metrics.annual_returns
  return Array.isArray(value) ? value : []
})
const qualityWarnings = computed(() => {
  const value = result.data.value?.quality.warnings
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []
})
const warningLabels: Record<string, string> = {
  CORPORATE_ACTIONS_NOT_APPLIED: '回测未处理分红、送转等公司行动',
  FACTOR_EXPOSURE_NOT_AVAILABLE: '因子暴露暂不可用',
  STYLE_EXPOSURE_NOT_AVAILABLE: '风格暴露暂不可用',
}
const warningText = computed(() => qualityWarnings.value.map((item) => warningLabels[item] ?? item).join('；'))
const undefinedMetrics = computed(() => {
  const value = result.data.value?.quality.undefined_metrics
  return value && typeof value === 'object' ? Object.entries(value as Record<string, unknown>) : []
})
</script>

<template>
  <div class="backtest-stack">
    <ErrorState v-if="result.isError.value" :message="String(result.error.value)" />
    <el-skeleton v-else-if="result.isLoading.value" :rows="12" animated />
    <template v-else-if="result.data.value">
      <el-alert v-if="warningText" :title="warningText" type="warning" :closable="false" show-icon />
      <div class="metrics-grid">
        <MetricCard label="累计收益" :value="formatPercent(metric('cumulative_return'))" :hint="`基准 ${formatPercent(metric('benchmark_cumulative_return'))}`" :tone="(metric('cumulative_return') ?? 0) >= 0 ? 'red' : 'green'" />
        <MetricCard label="年化收益" :value="formatPercent(metric('annualized_return'))" :hint="`超额 ${formatPercent(metric('relative_cumulative_return'))}`" tone="cyan" />
        <MetricCard label="Sharpe Ratio" :value="formatNumber(metric('sharpe_ratio'))" :hint="`Sortino ${formatNumber(metric('sortino_ratio'))}`" />
        <MetricCard label="最大回撤" :value="formatPercent(metric('max_drawdown'))" :hint="`Calmar ${formatNumber(metric('calmar_ratio'))}`" tone="red" />
        <MetricCard label="几何超额" :value="formatPercent(metric('geometric_excess_return'))" :hint="`跟踪误差 ${formatPercent(metric('tracking_error'))}`" tone="cyan" />
        <MetricCard label="Alpha" :value="formatPercent(metric('jensen_alpha'))" :hint="`Beta ${formatNumber(metric('beta'))}`" />
        <MetricCard label="主动最大回撤" :value="formatPercent(metric('active_max_drawdown'))" :hint="`水下 ${formatPercent(metric('time_under_water_rate'))}`" tone="red" />
        <MetricCard label="年化波动率" :value="formatPercent(metric('annualized_volatility'))" :hint="`基准年化 ${formatPercent(metric('benchmark_annualized_return'))}`" />
      </div>
      <ChartCard title="策略与基准净值" :subtitle="`Manifest ${shortHash(result.data.value.manifest_hash)}`"><VChart class="chart" :option="navOption" autoresize /></ChartCard>
      <p class="drawdown-dates">回撤峰值 {{ metricText('max_drawdown_peak_date') }} · 谷值 {{ metricText('max_drawdown_trough_date') }} · 恢复 {{ metricText('max_drawdown_recovery_date') }}</p>
      <div class="dashboard-grid">
        <ChartCard title="组合回撤" subtitle="按日计算的水下曲线" :empty="!result.data.value.drawdown.length"><VChart class="chart small" :option="drawdownOption" autoresize /></ChartCard>
        <section class="panel"><header class="panel-heading"><div><h2>执行与组合质量</h2><p>成本、成交、现金和集中度</p></div></header><el-descriptions :column="1" border size="small"><el-descriptions-item label="年化换手">{{ formatPercent(metric('annualized_turnover')) }}</el-descriptions-item><el-descriptions-item label="累计成本拖累">{{ formatPercent(metric('cumulative_cost_drag')) }}</el-descriptions-item><el-descriptions-item label="成交额成交率">{{ formatPercent(metric('notional_fill_rate')) }}</el-descriptions-item><el-descriptions-item label="可定价订单覆盖率">{{ formatPercent(metric('priced_order_coverage_rate')) }}</el-descriptions-item><el-descriptions-item label="失败成交率">{{ formatPercent(metric('failed_fill_rate')) }}</el-descriptions-item><el-descriptions-item label="平均现金权重">{{ formatPercent(metric('average_cash_weight')) }}</el-descriptions-item><el-descriptions-item label="最大持仓权重">{{ formatPercent(metric('max_position_weight')) }}</el-descriptions-item><el-descriptions-item label="最长水下会话">{{ formatNumber(metric('max_drawdown_duration_sessions')) }}</el-descriptions-item></el-descriptions></section>
      </div>
      <div class="section-tabs"><el-tabs><el-tab-pane label="月度收益"><el-table :data="result.data.value.monthly_returns" height="310"><el-table-column prop="year" label="年度" /><el-table-column prop="month" label="月份" /><el-table-column label="策略收益" align="right"><template #default="scope">{{ formatPercent(Number(scope.row.portfolio_return)) }}</template></el-table-column><el-table-column label="基准收益" align="right"><template #default="scope">{{ formatPercent(Number(scope.row.benchmark_return)) }}</template></el-table-column><el-table-column label="超额" align="right"><template #default="scope">{{ formatPercent(Number(scope.row.relative_return)) }}</template></el-table-column></el-table></el-tab-pane><el-tab-pane label="年度收益"><el-table :data="annualReturns" height="310"><el-table-column prop="year" label="年度" /><el-table-column label="策略收益" align="right"><template #default="scope">{{ formatPercent(Number(scope.row.portfolio_return)) }}</template></el-table-column><el-table-column label="基准收益" align="right"><template #default="scope">{{ formatPercent(Number(scope.row.benchmark_return)) }}</template></el-table-column><el-table-column label="超额" align="right"><template #default="scope">{{ formatPercent(Number(scope.row.relative_return)) }}</template></el-table-column></el-table></el-tab-pane><el-tab-pane label="成交原因"><el-table :data="result.data.value.execution_summary" height="310"><el-table-column prop="side" label="方向" /><el-table-column prop="reason_code" label="原因" /><el-table-column prop="order_count" label="订单数" align="right" /><el-table-column prop="requested_quantity" label="请求数量" align="right" /><el-table-column prop="filled_quantity" label="成交数量" align="right" /><el-table-column prop="unpriced_order_count" label="无价订单" align="right" /></el-table></el-tab-pane><el-tab-pane label="风险暴露"><el-table :data="result.data.value.exposures" height="310"><el-table-column prop="trade_date" label="日期" /><el-table-column prop="dimension" label="维度" /><el-table-column prop="key" label="分类" /><el-table-column prop="weight" label="权重" align="right" /></el-table></el-tab-pane><el-tab-pane label="收益归因"><el-table :data="result.data.value.attribution" height="310"><el-table-column prop="trade_date" label="日期" /><el-table-column prop="dimension" label="维度" /><el-table-column prop="key" label="分类" /><el-table-column prop="contribution_return" label="贡献" align="right" /></el-table></el-tab-pane><el-tab-pane label="指标说明"><el-table :data="undefinedMetrics" height="310"><el-table-column prop="0" label="指标" /><el-table-column prop="1" label="未定义原因" /></el-table></el-tab-pane></el-tabs></div>
    </template>
  </div>
</template>

<style scoped>
.backtest-stack { display: flex; flex-direction: column; gap: 18px; padding-top: 4px; }
.drawdown-dates { margin: -8px 0 0; color: var(--text-secondary); font-size: 12px; }
</style>
