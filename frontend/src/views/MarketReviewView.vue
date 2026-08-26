<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import dayjs from 'dayjs'
import { computed, ref, watchEffect } from 'vue'
import VChart from 'vue-echarts'

import { api } from '../api'
import { axis, tooltip } from '../charts'
import ChartCard from '../components/ChartCard.vue'
import ErrorState from '../components/ErrorState.vue'
import MetricCard from '../components/MetricCard.vue'
import { formatNumber, formatPercent, shortHash } from '../format'
import type { MarketReview, MarketReviewDates, MarketReviewIndex } from '../types'

const selectedDate = ref('')
const excludeSt = ref(false)
const datesQuery = useQuery({
  queryKey: ['market-review-dates'],
  queryFn: () => api.get<MarketReviewDates>('/api/v1/market-review/dates'),
  refetchInterval: 60_000,
})

watchEffect(() => {
  if (!selectedDate.value && datesQuery.data.value?.latest_trade_date) {
    selectedDate.value = datesQuery.data.value.latest_trade_date
  }
})

const reviewQuery = useQuery({
  queryKey: computed(() => ['market-review', selectedDate.value, excludeSt.value]),
  queryFn: () => api.get<MarketReview>(
    `/api/v1/market-review?trade_date=${encodeURIComponent(selectedDate.value)}&exclude_st=${excludeSt.value}`,
  ),
  enabled: computed(() => Boolean(selectedDate.value)),
})

const review = computed(() => reviewQuery.data.value)
const allowedDates = computed(() => new Set(datesQuery.data.value?.dates ?? []))
const disabledDate = (value: Date) => !allowedDates.value.has(dayjs(value).format('YYYY-MM-DD'))
const indexById = (identifier: string) => review.value?.indexes.find((item) => item.index_id === identifier)
const metricClass = (value?: number | null) => Number(value) >= 0 ? 'up' : 'down'
const indexTone = (value?: number | null): 'blue' | 'red' | 'green' => {
  if (value == null || !Number.isFinite(value) || value === 0) return 'blue'
  return value > 0 ? 'red' : 'green'
}
const formatAmount = (value?: number | null) => value == null || !Number.isFinite(value)
  ? '—'
  : `${formatNumber(value / 100_000_000)} 亿`
const formatRatioPercent = (value?: number | null) => value == null || !Number.isFinite(value)
  ? '—'
  : `${(value * 100).toFixed(2)}%`
const formatIndexPercent = (value: number) => `${value > 0 ? '+' : ''}${value.toFixed(2)}%`

const indexOption = computed(() => ({
  tooltip,
  legend: { top: 0, textStyle: { color: '#66778d', fontSize: 10 } },
  grid: { left: 48, right: 20, top: 38, bottom: 34 },
  xAxis: {
    type: 'category',
    data: review.value?.indexes[0]?.series.map((item) => item.trade_date.slice(5)) ?? [],
    ...axis,
  },
  yAxis: { type: 'value', scale: true, ...axis, axisLabel: { formatter: formatIndexPercent } },
  series: (review.value?.indexes ?? []).map((item) => ({
    name: item.name,
    type: 'line',
    symbol: 'none',
    smooth: 0.18,
    tooltip: { valueFormatter: formatIndexPercent },
    data: item.series.map((point) => (point.value - 1) * 100),
  })),
}))

const liquidityOption = computed(() => ({
  tooltip,
  grid: { left: 55, right: 20, top: 24, bottom: 34 },
  xAxis: {
    type: 'category',
    data: review.value?.liquidity.series.map((item) => item.trade_date.slice(5)) ?? [],
    ...axis,
  },
  yAxis: { type: 'value', ...axis, axisLabel: { formatter: (value: number) => `${(value / 100_000_000).toFixed(0)}亿` } },
  series: [
    {
      name: '成交额', type: 'bar', itemStyle: { color: '#2563eb', borderRadius: [3, 3, 0, 0] },
      data: review.value?.liquidity.series.map((item) => item.value) ?? [],
    },
    {
      name: '5日均额', type: 'line', symbol: 'none', itemStyle: { color: '#087f79' },
      data: review.value?.liquidity.series.map((item) => item.auxiliary) ?? [],
    },
  ],
}))

const distributionOption = computed(() => ({
  tooltip,
  grid: { left: 45, right: 16, top: 20, bottom: 38 },
  xAxis: { type: 'category', data: review.value?.breadth.buckets.map((item) => item.label) ?? [], ...axis },
  yAxis: { type: 'value', ...axis },
  series: [{
    type: 'bar',
    data: review.value?.breadth.buckets.map((item, index) => ({
      value: item.count,
      itemStyle: { color: index < 4 ? '#16825f' : index === 4 ? '#7d8da3' : '#d83a52' },
    })) ?? [],
  }],
}))

const breadthOption = computed(() => ({
  tooltip: { trigger: 'item' },
  grid: { left: 55, right: 20, top: 18, bottom: 30 },
  xAxis: { type: 'category', data: ['上涨', '平盘', '下跌'], ...axis },
  yAxis: { type: 'value', ...axis },
  series: [{
    type: 'bar',
    data: [
      { value: review.value?.breadth.up_count ?? 0, itemStyle: { color: '#d83a52' } },
      { value: review.value?.breadth.flat_count ?? 0, itemStyle: { color: '#7d8da3' } },
      { value: review.value?.breadth.down_count ?? 0, itemStyle: { color: '#16825f' } },
    ],
  }],
}))

const industryStyle = (value: number | null) => {
  if (value == null) return { background: '#f5f8fc', borderColor: '#d8e2ee' }
  const alpha = Math.min(0.1 + Math.abs(value) * 10, 0.58)
  return value >= 0
    ? { background: `rgba(216,58,82,${alpha})`, borderColor: 'rgba(216,58,82,.32)' }
    : { background: `rgba(22,130,95,${alpha})`, borderColor: 'rgba(22,130,95,.32)' }
}

const valuationLabel: Record<string, string> = { pe_ttm: 'PE TTM', pb: 'PB', ps_ttm: 'PS TTM' }
const eventLabel: Record<string, string> = {
  LIMIT_UP: '涨停',
  LIMIT_DOWN: '跌停',
  BROKEN_LIMIT_UP: '炸板',
  ONE_PRICE_LIMIT_UP: '一字涨停',
}

const indexHint = (item?: MarketReviewIndex) =>
  item ? `5日 ${formatPercent(item.return_5d)} · 20日 ${formatPercent(item.return_20d)}` : '暂无指数行情'
</script>

<template>
  <div class="page-stack market-review-page">
    <ErrorState
      v-if="datesQuery.isError.value || reviewQuery.isError.value"
      :message="String(datesQuery.error.value ?? reviewQuery.error.value)"
    />

    <section class="panel review-toolbar">
      <div>
        <h2>日终市场快照</h2>
        <p>只读取通过 validate-all 的 Canonical 数据，所有历史行业均遵循 PIT。</p>
      </div>
      <div class="toolbar">
        <el-date-picker
          v-model="selectedDate"
          type="date"
          value-format="YYYY-MM-DD"
          format="YYYY-MM-DD"
          :disabled-date="disabledDate"
          :clearable="false"
          aria-label="市场全景交易日"
        />
        <span class="switch-label">剔除 ST</span>
        <el-switch v-model="excludeSt" aria-label="剔除 ST" />
        <el-tag type="success" effect="plain">已验证</el-tag>
        <span class="hash">{{ shortHash(review?.catalog_hash) }}</span>
      </div>
    </section>

    <template v-if="review">
      <div class="metrics-grid review-index-metrics">
        <MetricCard
          v-for="item in review.indexes"
          :key="item.index_id"
          class="index-metric-card"
          :label="item.name"
          :value="formatPercent(item.daily_return)"
          :hint="indexHint(item)"
          :tone="indexTone(item.daily_return)"
        />
      </div>
      <div class="metrics-grid review-market-metrics">
        <MetricCard label="全A成交额" :value="formatAmount(review.liquidity.amount)" :hint="`较前日 ${formatPercent(review.liquidity.change_vs_previous)}`" />
        <MetricCard label="个股涨跌中位数" :value="formatPercent(review.breadth.median_return)" :hint="`等权 ${formatPercent(review.breadth.equal_weight_return)}`" :tone="Number(review.breadth.median_return) >= 0 ? 'red' : 'green'" />
        <MetricCard label="上涨 / 下跌" :value="`${review.breadth.up_count} / ${review.breadth.down_count}`" :hint="`上涨率 ${formatPercent(review.breadth.advance_rate)}`" />
        <MetricCard label="涨停 / 跌停" :value="`${review.sentiment.limit_up_count} / ${review.sentiment.limit_down_count}`" :hint="`炸板 ${review.sentiment.broken_limit_up_count} · 未解析 ${review.sentiment.unresolved_count}`" tone="red" />
      </div>

      <section class="panel quality-strip">
        <span>有效定价 <strong>{{ review.data_quality.priced_count }}</strong> / {{ review.data_quality.expected_count }}</span>
        <span>覆盖率 <strong>{{ formatPercent(review.data_quality.coverage_rate) }}</strong></span>
        <span>停牌 <strong>{{ review.data_quality.suspended_count }}</strong></span>
        <span>ST <strong>{{ review.data_quality.st_count }}</strong></span>
        <span>缺失行情 <strong>{{ review.data_quality.missing_bar_count }}</strong></span>
        <span>成交额20日分位 <strong>{{ formatPercent(review.liquidity.percentile_20d) }}</strong></span>
      </section>

      <div class="dashboard-grid">
        <ChartCard title="核心指数走势" subtitle="最近21个有效交易日，首日为 0%" :empty="!review.indexes.some((item) => item.series.length)">
          <VChart class="chart" :option="indexOption" autoresize />
        </ChartCard>
        <ChartCard title="市场广度" :subtitle="`净上涨 ${review.breadth.net_advance_count} 家`">
          <VChart class="chart" :option="breadthOption" autoresize />
        </ChartCard>
      </div>

      <div class="dashboard-grid">
        <ChartCard title="全A成交额" :subtitle="`5日均额 ${formatAmount(review.liquidity.average_5d)} · 20日均额 ${formatAmount(review.liquidity.average_20d)}`">
          <VChart class="chart" :option="liquidityOption" autoresize />
        </ChartCard>
        <ChartCard title="个股收益分布" :subtitle="`P10 ${formatPercent(review.breadth.p10_return)} · P90 ${formatPercent(review.breadth.p90_return)}`">
          <VChart class="chart" :option="distributionOption" autoresize />
        </ChartCard>
      </div>

      <section class="panel">
        <header class="panel-heading">
          <div><h2>行业热力图</h2><p>{{ review.industries.taxonomy ?? review.industries.unavailable_reason }} · 覆盖 {{ formatPercent(review.industries.coverage_rate) }}</p></div>
        </header>
        <div v-if="review.industries.available" class="industry-heatmap">
          <article
            v-for="item in review.industries.items"
            :key="item.industry_code"
            class="industry-cell"
            :style="industryStyle(item.equal_weight_return)"
          >
            <strong>{{ item.industry_name }}</strong>
            <span :class="metricClass(item.equal_weight_return)">{{ formatPercent(item.equal_weight_return) }}</span>
            <small>股票 {{ item.instrument_count }} · 有效 {{ item.priced_count }}</small>
            <small>上涨 {{ formatRatioPercent(item.advance_rate) }} · 成交 {{ formatRatioPercent(item.amount_share) }}</small>
          </article>
        </div>
        <div v-else class="empty-state"><span>行业数据不可用</span><small>{{ review.industries.unavailable_reason }}</small></div>
      </section>

      <div class="dashboard-grid">
        <section class="panel table-panel">
          <header class="panel-heading"><div><h2>涨跌停情绪</h2><p>{{ review.sentiment.note }}</p></div><span>规则覆盖 {{ formatPercent(review.sentiment.coverage_rate) }}</span></header>
          <el-table :data="review.sentiment.events" height="360">
            <el-table-column label="证券" min-width="150"><template #default="scope"><strong>{{ scope.row.name }}</strong><small class="instrument-code">{{ scope.row.instrument_id }}</small></template></el-table-column>
            <el-table-column label="事件" width="105"><template #default="scope">{{ eventLabel[scope.row.event] }}</template></el-table-column>
            <el-table-column label="板块" prop="board" width="100" />
            <el-table-column label="涨跌幅" width="105" align="right"><template #default="scope"><span :class="metricClass(scope.row.pct_change)">{{ formatPercent(scope.row.pct_change) }}</span></template></el-table-column>
            <el-table-column label="成交额" width="130" align="right"><template #default="scope">{{ formatAmount(scope.row.amount) }}</template></el-table-column>
          </el-table>
        </section>

        <section class="panel">
          <header class="panel-heading"><div><h2>估值与换手</h2><p>仅统计正且有限的估值样本</p></div></header>
          <div class="valuation-stack">
            <article v-for="item in review.valuation.metrics" :key="item.metric" class="valuation-row">
              <div><strong>{{ valuationLabel[item.metric] }}</strong><small>{{ item.valid_count }} 个样本</small></div>
              <span>{{ formatNumber(item.median) }}</span>
              <small>P25 {{ formatNumber(item.p25) }} · P75 {{ formatNumber(item.p75) }}</small>
            </article>
            <article class="valuation-row">
              <div><strong>换手率中位数</strong><small>{{ review.valuation.turnover_valid_count }} 个样本</small></div>
              <span>{{ formatPercent(review.valuation.turnover_median) }}</span>
            </article>
          </div>
        </section>
      </div>
    </template>
  </div>
</template>

<style scoped>
.review-toolbar { display: flex; align-items: center; justify-content: space-between; gap: 20px; }
.review-toolbar h2 { margin: 0; font-size: 15px; }
.review-toolbar p { margin: 6px 0 0; color: var(--dim); font-size: 11px; }
.switch-label { color: var(--muted); font-size: 12px; }
.review-index-metrics { grid-template-columns: repeat(5, minmax(0, 1fr)); }
.review-market-metrics { grid-template-columns: repeat(4, minmax(0, 1fr)); }
.review-index-metrics :deep(.index-metric-card) { box-shadow: inset 4px 0 0 var(--accent), 0 8px 22px rgba(32, 48, 72, .04); }
.review-index-metrics :deep(.index-metric-card.tone-red) { border-color: rgba(216, 58, 82, .4); background: linear-gradient(135deg, rgba(216, 58, 82, .13), rgba(216, 58, 82, .025) 68%, #fff); }
.review-index-metrics :deep(.index-metric-card.tone-green) { border-color: rgba(22, 130, 95, .4); background: linear-gradient(135deg, rgba(22, 130, 95, .13), rgba(22, 130, 95, .025) 68%, #fff); }
.review-index-metrics :deep(.index-metric-card.tone-red .metric-value) { color: var(--up); }
.review-index-metrics :deep(.index-metric-card.tone-green .metric-value) { color: var(--down); }
.quality-strip { display: flex; align-items: center; flex-wrap: wrap; gap: 14px 24px; padding: 13px 18px; color: var(--muted); font-size: 11px; }
.quality-strip strong { color: var(--text); font-size: 12px; }
.industry-heatmap { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 9px; }
.industry-cell { min-height: 126px; display: flex; flex-direction: column; justify-content: space-between; gap: 7px; padding: 13px; border: 1px solid; border-radius: 9px; }
.industry-cell strong { font-size: 12px; }
.industry-cell span { font-size: 19px; font-weight: 750; }
.industry-cell small { color: var(--muted); font-size: 10px; }
.instrument-code { display: block; margin-top: 3px; color: var(--dim); font-family: ui-monospace, Consolas, monospace; }
.valuation-stack { display: flex; flex-direction: column; gap: 10px; }
.valuation-row { display: grid; grid-template-columns: 1fr auto; gap: 6px 16px; align-items: center; padding: 13px; border: 1px solid var(--border); border-radius: 9px; background: var(--surface-raised); }
.valuation-row div { display: flex; flex-direction: column; gap: 4px; }
.valuation-row strong { font-size: 12px; }
.valuation-row span { grid-row: span 2; font-size: 20px; font-weight: 750; }
.valuation-row small { color: var(--dim); font-size: 10px; }
@media (max-width: 1360px) {
  .review-toolbar { align-items: flex-start; flex-direction: column; }
  .review-index-metrics,
  .review-market-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .industry-heatmap { grid-template-columns: repeat(3, minmax(0, 1fr)); }
}
</style>
