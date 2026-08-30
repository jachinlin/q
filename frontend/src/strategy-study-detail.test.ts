import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import StrategyStudyDetailView from './views/StrategyStudyDetailView.vue'

const apiGet = vi.hoisted(() => vi.fn())
vi.mock('./api', () => ({ api: { get: apiGet, post: vi.fn(), delete: vi.fn() } }))
vi.mock('vue-echarts', () => ({ default: { name: 'VChart', inheritAttrs: true, props: ['option'], template: '<div class="chart-stub" />' } }))

const study = {
  id: 'study-1',
  definition: {
    name: '单项研究',
    description: '验证完整研究报告',
    tags: [],
    start_date: '2020-01-01',
    end_date: '2024-12-31',
    strategy: { strategy_id: 'dual_ma_trend', parameters: {} },
    benchmark: '000300.SH',
    initial_cash_fen: 100000000,
    execution: {},
  },
  config_hash: 'a'.repeat(64),
  catalog_hash: 'b'.repeat(64),
  status: 'SUCCEEDED',
  stage: 'PUBLISH',
  task_id: null,
  artifact_dir: 'trusted',
  manifest_hash: 'c'.repeat(64),
  error: null,
  created_at: '2026-08-01T00:00:00Z',
  started_at: '2026-08-01T00:00:01Z',
  completed_at: '2026-08-01T00:01:00Z',
  metrics: [
    { name: 'annualized_return', value: 0.12, unit: 'ratio' },
    { name: 'annualized_geometric_excess_return', value: 0.035, unit: 'ratio' },
    { name: 'max_drawdown', value: -0.08, unit: 'ratio' },
    { name: 'sharpe_ratio', value: 1.25, unit: 'number' },
    { name: 'annualized_turnover', value: 2.2, unit: 'ratio' },
    { name: 'annualized_cost_drag', value: 0.004, unit: 'ratio' },
    { name: 'positive_month_rate', value: 0.625, unit: 'ratio' },
    { name: 'tracking_error', value: 0.06, unit: 'ratio' },
    { name: 'failed_fill_rate', value: 0.02, unit: 'ratio' },
    { name: 'average_cash_weight', value: 0.15, unit: 'ratio' },
  ],
  artifacts: [
    { artifact_type: 'performance', relative_path: 'performance.parquet', content_hash: '1'.repeat(64), byte_count: 100, row_count: 1001, schema: {} },
    { artifact_type: 'quality_disclosure', relative_path: 'quality_disclosure.json', content_hash: '2'.repeat(64), byte_count: 80, row_count: null, schema: null },
  ],
}

const report = {
  performance: [
    { trade_date: '2020-01-02', return: 0, benchmark_return: 0, cumulative_return: 0, benchmark_cumulative_return: 0, active_return: 0, nav: 1, benchmark_nav: 1, gross_nav: 1, gross_cumulative_return: 0, cumulative_cost_drag: 0, drawdown: 0, active_drawdown: 0 },
    { trade_date: '2024-12-31', return: 0.01, benchmark_return: 0.005, cumulative_return: 0.42, benchmark_cumulative_return: 0.25, active_return: 0.005, nav: 1.42, benchmark_nav: 1.25, gross_nav: 1.44, gross_cumulative_return: 0.44, cumulative_cost_drag: 0.02, drawdown: -0.01, active_drawdown: -0.005 },
  ],
  rolling_performance: [
    { trade_date: '2024-12-31', window_sessions: 252, annualized_return: 0.12, benchmark_annualized_return: 0.08, annualized_excess_return: 0.04, annualized_volatility: 0.15, sharpe_ratio: 0.8, max_drawdown: -0.08, tracking_error: 0.06, information_ratio: 0.67, beta: 0.9 },
  ],
  monthly_returns: [
    { year: 2024, month: 12, period_start: '2024-12-01', period_end: '2024-12-31', portfolio_return: 0.03, benchmark_return: 0.02, relative_return: 0.01 },
  ],
  annual_returns: [
    { year: 2024, period_start: '2024-01-01', period_end: '2024-12-31', portfolio_return: 0.12, benchmark_return: 0.08, relative_return: 0.04 },
  ],
  drawdown_episodes: [
    { episode_index: 1, peak_date: '2024-05-01', trough_date: '2024-05-10', recovery_date: '2024-05-20', max_drawdown: -0.08, underwater_sessions: 13, recovery_sessions: 5, is_recovered: true },
  ],
  exposure: [
    { trade_date: '2024-12-31', dimension: 'SECURITY', key: '510300.SH', weight: 0.85 },
    { trade_date: '2024-12-31', dimension: 'CASH', key: 'CASH', weight: 0.15 },
  ],
  attribution: [{ key: '510300.SH', pnl_fen: 420000, contribution_return: 0.042 }],
  execution: [{ side: 'BUY', reason_code: 'FILLED', order_count: 3, requested_quantity: 300, filled_quantity: 290, unfilled_quantity: 10, priced_requested_notional_fen: 300000, priced_filled_notional_fen: 290000, unpriced_order_count: 0 }],
  quality: {
    calculation_mode: 'FULL_SAMPLE',
    rolling_window_sessions: 252,
    tail_risk_method: 'HISTORICAL_95',
    risk_free_rate_annual: 0,
    undefined_metrics: {},
    unavailable_dimensions: {},
    attribution_method: 'DAILY_PNL',
    warnings: ['成交价格为日频近似'],
  },
}

async function mountDetail() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/strategy-studies/new', component: { template: '<div />' } },
      { path: '/strategy-studies/:strategyStudyId', component: StrategyStudyDetailView },
    ],
  })
  await router.push('/strategy-studies/study-1')
  await router.isReady()
  return mount(StrategyStudyDetailView, { global: { plugins: [[VueQueryPlugin, { queryClient: client }], router] } })
}

describe('strategy study detail', () => {
  beforeEach(() => apiGet.mockReset())

  it('renders a research-first complete typed report with Chinese metric groups', async () => {
    apiGet.mockImplementation((path?: string) => {
      if (path === '/api/v1/strategy-studies/study-1/report') return Promise.resolve(report)
      if (path === '/api/v1/strategy-studies/study-1') return Promise.resolve(study)
      return Promise.resolve({ items: [], total: 0 })
    })
    const wrapper = await mountDetail()

    await vi.waitFor(() => expect(wrapper.text()).toContain('2020-01-02 → 2024-12-31 · 2 个交易日'))
    expect(apiGet).toHaveBeenCalledWith('/api/v1/strategy-studies/study-1/report')
    expect(apiGet.mock.calls.some(([path]) => String(path).includes('artifacts/performance?page=1&page_size=1000'))).toBe(false)
    expect(wrapper.findAll('.chart-stub')).toHaveLength(10)
    expect(wrapper.text()).toContain('收益')
    expect(wrapper.text()).toContain('风险')
    expect(wrapper.text()).toContain('基准相对表现')
    expect(wrapper.text()).toContain('交易执行')
    expect(wrapper.text()).toContain('组合暴露')
    expect(wrapper.text()).toContain('年化几何超额')
    expect(wrapper.text()).toContain('正收益月份占比')
    expect(wrapper.text()).toContain('主要回撤事件')
    expect(wrapper.text()).toContain('252 日滚动风险幅度')
    expect(wrapper.text()).toContain('证券与现金暴露')
    expect(wrapper.text()).toContain('成交质量')
    expect(wrapper.text()).toContain('复制研究')
    expect(wrapper.text()).not.toContain('Run 比较')

    const nav = wrapper.find('[data-chart="nav"]')
    const navOption = nav.attributes('data-chart') && nav.element
      ? wrapper.findComponent({ name: 'VChart' }).props('option')
      : null
    expect(navOption.xAxis.data).toEqual(['2020-01-02', '2024-12-31'])
    expect(navOption.series.map((series: { name: string }) => series.name)).toEqual(['策略净值', '毛净值', '基准净值'])
    wrapper.unmount()
  })

  it('builds the artifact browser from the manifest and renders JSON values', async () => {
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/strategy-studies/study-1/report') return Promise.resolve(report)
      if (path === '/api/v1/strategy-studies/study-1') return Promise.resolve(study)
      if (path?.includes('/artifacts/quality_disclosure')) return Promise.resolve({ value: { calculation_mode: 'FULL_SAMPLE', warnings: ['成交价格为日频近似'] } })
      if (path?.includes('/artifacts/performance')) return Promise.resolve({ items: report.performance, page: 1, page_size: 100, total: 1001 })
      return Promise.resolve({})
    })
    const wrapper = await mountDetail()
    await vi.waitFor(() => expect(wrapper.text()).toContain('配置与证据'))

    await wrapper.findAll('.el-tabs__item')[1].trigger('click')
    await vi.waitFor(() => expect(wrapper.text()).toContain('Manifest 产物登记'))
    expect(wrapper.text()).toContain('每日绩效')
    expect(wrapper.text()).toContain('质量披露')
    expect(wrapper.text()).toContain('1001 行')

    const selects = wrapper.findAllComponents({ name: 'ElSelect' })
    selects[0].vm.$emit('update:modelValue', 'quality_disclosure')
    await flushPromises()
    await vi.waitFor(() => expect(apiGet.mock.calls.some(([path]) => String(path).includes('/artifacts/quality_disclosure'))).toBe(true))
    expect(wrapper.find('[data-artifact-json]').text()).toContain('"calculation_mode": "FULL_SAMPLE"')
    expect(wrapper.find('[data-artifact-json]').text()).toContain('成交价格为日频近似')
    wrapper.unmount()
  })

  it('shows empty chart states and a report error state', async () => {
    const emptyReport = {
      ...report,
      performance: [],
      rolling_performance: [],
      monthly_returns: [],
      annual_returns: [],
      drawdown_episodes: [],
      exposure: [],
      attribution: [],
      execution: [],
    }
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/strategy-studies/study-1/report') return Promise.resolve(emptyReport)
      return Promise.resolve(study)
    })
    const emptyWrapper = await mountDetail()
    await vi.waitFor(() => expect(emptyWrapper.text()).toContain('暂无可用数据'))
    expect(emptyWrapper.findAll('.empty-state').length).toBeGreaterThan(0)
    emptyWrapper.unmount()

    apiGet.mockReset()
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/strategy-studies/study-1/report') return Promise.reject(new Error('报告读取失败'))
      return Promise.resolve(study)
    })
    const errorWrapper = await mountDetail()
    await vi.waitFor(() => expect(errorWrapper.find('.error-state').exists()).toBe(true))
    expect(errorWrapper.text()).toContain('报告读取失败')
    errorWrapper.unmount()
  })

  it('shows the same structured task progress as factor study detail', async () => {
    const runningStudy = {
      ...study,
      definition: { ...study.definition, name: 'ETF 轮动', description: '运行中的研究', tags: ['trend'], strategy: { strategy_id: 'etf_rotation', parameters: {} } },
      status: 'RUNNING',
      stage: 'BACKTEST',
      task_id: 'task-1',
      artifact_dir: null,
      manifest_hash: null,
      completed_at: null,
      metrics: [],
      artifacts: [],
    }
    const runningTask = {
      id: 'task-1', subject_kind: 'STRATEGY_STUDY', subject_id: 'study-1', task_type: 'STRATEGY_STUDY', status: 'RUNNING', priority: 0,
      progress: {
        stage: 'BACKTEST', completed: 1, total: 4, message: '正在执行策略回测（850/1699）',
        context: {
          substage: 'RUN_BACKTEST', substage_state: 'PROGRESS', item_completed: 850, item_total: 1699, trade_date: '2021-06-22',
          last_completed_substage: 'BUILD_STRATEGY', last_completed_evidence: { market_instrument_count: 3, fund_instrument_count: 3 },
        },
      },
      created_at: '2026-08-30T00:00:00Z', started_at: '2026-08-30T00:00:01Z', updated_at: '2026-08-30T00:00:02Z', heartbeat_at: '2026-08-30T00:00:02Z', completed_at: null,
      worker_id: 'worker-1', error: null, result: null, payload: { strategy_study_id: 'study-1' }, attempts: [],
    }
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/strategy-studies/study-1') return Promise.resolve(runningStudy)
      if (path === '/api/v1/tasks/task-1') return Promise.resolve(runningTask)
      return Promise.resolve({ items: [], total: 0 })
    })
    const wrapper = await mountDetail()

    await vi.waitFor(() => expect(wrapper.find('.strategy-task-progress--detail').exists()).toBe(true))
    const progress = wrapper.find('.strategy-task-progress--detail')
    expect(progress.attributes('data-task-stage')).toBe('BACKTEST')
    expect(progress.attributes('data-task-substage')).toBe('RUN_BACKTEST')
    expect(progress.text()).toContain('执行策略回测')
    expect(progress.text()).toContain('当前交易日 850/1699')
    expect(progress.text()).toContain('回测进度 50%')
    expect(progress.text()).toContain('最近完成：构造冻结策略')
    expect(progress.text()).toContain('ETF 3')
    expect(wrapper.text()).not.toContain('当前阶段：BACKTEST')
    wrapper.unmount()
  })
})
