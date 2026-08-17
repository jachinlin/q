import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import MarketReviewView from './views/MarketReviewView.vue'

const apiGet = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({ api: { get: apiGet } }))
vi.mock('vue-echarts', () => ({
  default: { name: 'VChart', props: ['option'], template: '<div class="chart-stub" />' },
}))

const review = {
  trade_date: '2026-08-14', catalog_hash: 'a'.repeat(64), validated_at: '2026-08-15T00:00:00Z', exclude_st: false,
  data_quality: { expected_count: 5000, priced_count: 4980, suspended_count: 10, st_count: 120, missing_bar_count: 10, coverage_rate: 0.998 },
  indexes: [
    { index_id: '399317.SZ', name: '国证A指', daily_return: 0.004, amplitude: 0.018, return_5d: 0.025, return_20d: 0.045, series: [{ trade_date: '2026-08-12', value: 1, auxiliary: null }, { trade_date: '2026-08-13', value: 1.05, auxiliary: null }, { trade_date: '2026-08-14', value: 0.98, auxiliary: null }] },
    { index_id: '000016.SH', name: '上证50', daily_return: 0.01, amplitude: 0.02, return_5d: 0.03, return_20d: 0.05, series: [{ trade_date: '2026-08-14', value: 1.05, auxiliary: null }] },
    { index_id: '000300.SH', name: '沪深300', daily_return: 0.012, amplitude: 0.02, return_5d: 0.04, return_20d: 0.06, series: [{ trade_date: '2026-08-14', value: 1.06, auxiliary: null }] },
    { index_id: '000905.SH', name: '中证500', daily_return: -0.005, amplitude: 0.03, return_5d: 0.01, return_20d: 0.02, series: [{ trade_date: '2026-08-14', value: 1.02, auxiliary: null }] },
    { index_id: '000852.SH', name: '中证1000', daily_return: -0.008, amplitude: 0.03, return_5d: -0.01, return_20d: 0.01, series: [{ trade_date: '2026-08-14', value: 1.01, auxiliary: null }] },
  ],
  liquidity: { amount: 1_100_000_000_000, change_vs_previous: 0.08, average_5d: 1_000_000_000_000, average_20d: 900_000_000_000, percentile_20d: 0.85, series: [{ trade_date: '2026-08-14', value: 1_100_000_000_000, auxiliary: 1_000_000_000_000 }] },
  breadth: { up_count: 3200, down_count: 1700, flat_count: 80, advance_rate: 0.642, net_advance_count: 1500, equal_weight_return: 0.01, median_return: 0.008, p10_return: -0.03, p25_return: -0.01, p75_return: 0.02, p90_return: 0.04, buckets: [{ label: '<-5%', count: 30 }, { label: '>5%', count: 60 }] },
  sentiment: { limit_up_count: 80, limit_down_count: 4, broken_limit_up_count: 15, one_price_limit_up_count: 3, eligible_count: 4900, unresolved_count: 10, coverage_rate: 0.998, note: '规则估算', events: [{ instrument_id: '600000.SH', name: '浦发银行', board: 'MAIN', is_st: false, pct_change: 0.1, amount: 1_000_000_000, event: 'LIMIT_UP' }] },
  industries: { available: true, taxonomy: '证监会行业分类', coverage_rate: 0.95, unavailable_reason: null, items: [{ industry_code: 'BANK', industry_name: '银行', equal_weight_return: 0.02, advance_rate: 0.8, amount_share: 0.1, instrument_count: 40, priced_count: 40, limit_up_count: 1, limit_down_count: 0 }] },
  valuation: { metrics: [{ metric: 'pe_ttm', median: 18, p25: 12, p75: 30, valid_count: 4000 }], turnover_median: 0.025, turnover_valid_count: 4800 },
}

describe('daily market review', () => {
  beforeEach(() => {
    apiGet.mockReset()
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/market-review/dates') {
        return Promise.resolve({ catalog_hash: 'a'.repeat(64), validated_at: '2026-08-15T00:00:00Z', latest_trade_date: '2026-08-14', dates: ['2026-08-13', '2026-08-14'] })
      }
      if (path.startsWith('/api/v1/market-review?')) return Promise.resolve(review)
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
  })

  it('loads the latest validated session and renders every review area', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/market', component: MarketReviewView }] })
    await router.push('/market')
    await router.isReady()
    const wrapper = mount(MarketReviewView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] } })

    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith(expect.stringContaining('trade_date=2026-08-14')))
    await flushPromises()

    expect(wrapper.text()).toContain('全A成交额')
    expect(wrapper.text()).toContain('国证A指')
    expect(wrapper.text()).toContain('行业热力图')
    expect(wrapper.text()).toContain('涨跌停情绪')
    expect(wrapper.text()).toContain('估值与换手')
    expect(wrapper.text()).toContain('浦发银行')
    expect(wrapper.text()).toContain('股票 40 · 有效 40')
    expect(wrapper.text()).toContain('上涨 80.00% · 成交 10.00%')
    expect(wrapper.text()).toContain('最近21个有效交易日，首日为 0%')
    expect(wrapper.find('[aria-label="市场全景交易日"]').exists()).toBe(true)
    expect(wrapper.find('[aria-label="剔除 ST"]').exists()).toBe(true)
    expect(wrapper.findAll('.index-metric-card')).toHaveLength(5)
    expect(wrapper.findAll('.review-market-metrics .metric-card')).toHaveLength(4)
    expect(wrapper.findAll('.index-metric-card.tone-red')).toHaveLength(3)
    expect(wrapper.findAll('.index-metric-card.tone-green')).toHaveLength(2)

    const indexChart = wrapper.findAllComponents({ name: 'VChart' })[0]
    const option = indexChart.props('option') as {
      yAxis: { axisLabel: { formatter: (value: number) => string } }
      series: Array<{
        data: number[]
        tooltip: { valueFormatter: (value: number) => string }
      }>
    }
    expect(option.series[0].data[0]).toBeCloseTo(0)
    expect(option.series[0].data[1]).toBeCloseTo(5)
    expect(option.series[0].data[2]).toBeCloseTo(-2)
    expect(option.yAxis.axisLabel.formatter(5)).toBe('+5.00%')
    expect(option.yAxis.axisLabel.formatter(0)).toBe('0.00%')
    expect(option.series[0].tooltip.valueFormatter(-2)).toBe('-2.00%')
  })
})
