import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import FactorsView from './views/FactorsView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiPost = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  api: {
    get: apiGet,
    post: apiPost,
  },
}))
vi.mock('vue-echarts', () => ({ default: { template: '<div class="chart-stub" />' } }))

describe('factor research workspace', () => {
  const config = {
    factor_refs: ['momentum_120_20'],
    start_date: '2025-01-01',
    end_date: '2025-12-31',
    horizons: [1, 5, 20],
    quantiles: 5,
    min_cross_section: 30,
    ic_rolling_window: 20,
    ic_rolling_min_valid: 10,
    ic_quantile_probabilities: [0.05, 0.25, 0.5, 0.75, 0.95],
    universe: 'CN_STOCK_STANDARD',
    return_definition: 'T1_OPEN_TO_TH_CLOSE',
    direction_adjusted: true,
    industry: { taxonomy: '证监会行业分类', unclassified_policy: 'EXCLUDE' },
  }
  const summary = (['DIRECTION_ADJUSTED', 'INDUSTRY_NEUTRALIZED'] as const).flatMap((signal_variant) => [1, 5, 20].map((horizon) => ({
    signal_variant, factor_ref: 'momentum_120_20', horizon,
    pearson_ic_mean: 0.02 * horizon, pearson_ic_sample_std: 0.1, pearson_icir_unannualized: 0.2,
    pearson_ic_positive_rate: 0.6, pearson_ic_p05: -0.1, pearson_ic_p25: -0.02, pearson_ic_p50: 0.03,
    pearson_ic_p75: 0.08, pearson_ic_p95: 0.2, pearson_ic_valid_date_count: 120,
    pearson_ic_max_positive_streak: 4, pearson_ic_positive_streak_start: '2025-01-02', pearson_ic_positive_streak_end: '2025-01-07',
    pearson_ic_max_negative_streak: 2, pearson_ic_negative_streak_start: '2025-02-03', pearson_ic_negative_streak_end: '2025-02-04',
    rank_ic_mean: 0.01 * horizon, rank_ic_sample_std: 0.08, rank_icir_unannualized: 0.125,
    rank_ic_positive_rate: 0.55, rank_ic_p05: -0.12, rank_ic_p25: -0.03, rank_ic_p50: 0.02,
    rank_ic_p75: 0.06, rank_ic_p95: 0.18, rank_ic_valid_date_count: 118,
    rank_ic_max_positive_streak: 3, rank_ic_positive_streak_start: '2025-01-02', rank_ic_positive_streak_end: '2025-01-06',
    rank_ic_max_negative_streak: 2, rank_ic_negative_streak_start: '2025-02-03', rank_ic_negative_streak_end: '2025-02-04',
    long_short_mean: 0.01,
  })))
  const run = (id: string) => ({
    id,
    study_id: 'study-1',
    task_id: `task-${id}`,
    status: 'SUCCEEDED',
    config,
    manifest_hash: 'abcdef123456',
    created_at: id === 'run-new' ? '2026-08-14T02:00:00Z' : '2026-08-13T02:00:00Z',
    started_at: '2026-08-14T02:00:01Z',
    completed_at: '2026-08-14T02:10:00Z',
    summary,
    error: null,
  })

  beforeEach(() => {
    apiGet.mockReset()
    apiPost.mockReset()
    apiPost.mockResolvedValue({ run_id: 'run-created' })
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/factors/catalog') {
        return Promise.resolve({ items: [{ factor_ref: 'momentum_120_20', name: '动量', direction: 1 }], horizons: [1, 5, 20], return_definition: 'T+1 open to T+h close', signal_variants: ['DIRECTION_ADJUSTED', 'INDUSTRY_NEUTRALIZED'], industry: { taxonomy: '证监会行业分类', unclassified_policies: ['EXCLUDE', 'UNCLASSIFIED'] } })
      }
      if (path.startsWith('/api/v1/factor-studies?')) {
        return Promise.resolve({ items: [{ id: 'study-1', name: '动量因子研究', config, created_at: '2026-08-14T01:00:00Z', latest_run: null }], page: 1, page_size: 100, total: 1 })
      }
      if (path === '/api/v1/factor-studies/study-1') {
        return Promise.resolve({ id: 'study-1', name: '动量因子研究', config, created_at: '2026-08-14T01:00:00Z', runs: [run('run-new'), run('run-old')] })
      }
      if (path === '/api/v1/factor-runs/run-new') return Promise.resolve(run('run-new'))
      if (path === '/api/v1/factor-runs/run-old') return Promise.resolve(run('run-old'))
      if (path === '/api/v1/factor-runs/run-created') {
        return Promise.resolve({ ...run('run-created'), status: 'RUNNING', manifest_hash: null, summary: undefined, completed_at: null })
      }
      if (path === '/api/v1/factor-runs/run-running') {
        return Promise.resolve({ ...run('run-running'), status: 'RUNNING', manifest_hash: null, summary: undefined, completed_at: null })
      }
      if (path === '/api/v1/factor-runs/run-failed') {
        return Promise.resolve({ ...run('run-failed'), status: 'FAILED', manifest_hash: null, summary: undefined, error: { code: 'FACTOR_ANALYSIS_FAILED' } })
      }
      if (path === '/api/v1/factor-runs/run-missing') return Promise.reject(new Error('factor run does not exist'))
      if (path.includes('/series?')) {
        return Promise.resolve({ run_id: 'run-new', manifest_hash: 'abcdef123456', factor_ref: 'momentum_120_20', horizon: 20, signal_variant: 'DIRECTION_ADJUSTED', ic: [{ signal_variant: 'DIRECTION_ADJUSTED', factor_ref: 'momentum_120_20', horizon: 20, signal_date: '2025-01-02', factor_valid_count: 100, sample_count: 98, pearson_ic: 0.2, rank_ic: 0.1, rolling_window: 20, rolling_valid_count: 10, pearson_ic_rolling_mean: 0.15, rank_ic_rolling_mean: 0.08, pearson_ic_cumulative_sum: 0.2, rank_ic_cumulative_sum: 0.1, is_valid: true, invalid_reason: null }], quantile_returns: [], long_short_returns: [], coverage: [] })
      }
      if (path.includes('/correlation?')) return Promise.resolve({ signal_variant: 'DIRECTION_ADJUSTED', data: [] })
      if (path.endsWith('/industry-coverage')) return Promise.resolve({ data: [{ signal_date: '2025-01-02', taxonomy: '证监会行业分类', unclassified_policy: 'EXCLUDE', eligible_count: 100, classified_count: 95, tombstone_count: 3, missing_state_count: 2, usable_count: 95, classified_coverage: 0.95, usable_coverage: 0.95 }] })
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
  })

  it('centers the latest study result and keeps run history secondary', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/factors', name: 'factors', component: { template: '<div />' } },
        { path: '/tasks', component: { template: '<div />' } },
      ],
    })
    await router.push('/factors')
    await router.isReady()

    const wrapper = mount(FactorsView, {
      global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
    })
    await flushPromises()
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith('/api/v1/factor-runs/run-new'))
    await flushPromises()

    expect(wrapper.text()).toContain('运行分析')
    expect(wrapper.text()).toContain('最新结果')
    expect(wrapper.text()).toContain('运行历史')
    expect(wrapper.text()).toContain('Rank IC 均值')
    expect(wrapper.text()).toContain('最长连续正 IC')
    expect(wrapper.find('[aria-label="选择IC类型"]').exists()).toBe(true)
    expect(wrapper.find('[aria-label="选择信号版本"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('PIT 行业覆盖')
    expect(wrapper.text()).toContain('无历史状态')
    expect(wrapper.text()).not.toContain('创建分析任务')
    expect(router.currentRoute.value.query.run).toBe('run-new')

    const historyButton = wrapper.findAll('button').find((item) => item.text() === '查看结果')
    expect(historyButton).toBeDefined()
    await historyButton?.trigger('click')
    await vi.waitFor(() => expect(router.currentRoute.value.query.run).toBe('run-old'))
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith('/api/v1/factor-runs/run-old'))

    const startButton = wrapper.findAll('button').find((item) => item.text() === '运行分析')
    expect(startButton).toBeDefined()
    await startButton?.trigger('click')
    await vi.waitFor(() => expect(apiPost).toHaveBeenCalledWith('/api/v1/factor-studies/study-1/runs'))
    await vi.waitFor(() => expect(router.currentRoute.value.query.run).toBe('run-created'))
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith('/api/v1/factor-runs/run-created'))
    wrapper.unmount()
  })

  it('keeps an exact historical run selected from a deep link', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/factors', name: 'factors', component: { template: '<div />' } },
        { path: '/tasks', component: { template: '<div />' } },
      ],
    })
    await router.push('/factors?run=run-old')
    await router.isReady()

    const wrapper = mount(FactorsView, {
      global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
    })
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith('/api/v1/factor-runs/run-old'))
    await flushPromises()

    expect(wrapper.text()).toContain('历史结果')
    expect(wrapper.find('.history-row.is-selected').text()).toContain('run-old')
    expect(router.currentRoute.value.query.run).toBe('run-old')

    await router.push('/factors')
    await vi.waitFor(() => expect(router.currentRoute.value.query.run).toBe('run-new'))
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith('/api/v1/factor-runs/run-new'))
    wrapper.unmount()
  })

  it.each([
    ['run-running', '正在分析'],
    ['run-failed', '本次分析失败'],
  ])('renders the exact non-successful run %s', async (requestedRun, expectedText) => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/factors', name: 'factors', component: { template: '<div />' } },
        { path: '/tasks', component: { template: '<div />' } },
      ],
    })
    await router.push(`/factors?run=${requestedRun}`)
    await router.isReady()

    const wrapper = mount(FactorsView, {
      global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
    })
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith(`/api/v1/factor-runs/${requestedRun}`))
    await flushPromises()

    expect(wrapper.text()).toContain(expectedText)
    expect(router.currentRoute.value.query.run).toBe(requestedRun)
    wrapper.unmount()
  })

  it('shows a deep-link error without falling back to the latest run', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/factors', name: 'factors', component: { template: '<div />' } },
        { path: '/tasks', component: { template: '<div />' } },
      ],
    })
    await router.push('/factors?run=run-missing')
    await router.isReady()

    const wrapper = mount(FactorsView, {
      global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
    })
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith('/api/v1/factor-runs/run-missing'))
    await flushPromises()

    expect(wrapper.text()).toContain('factor run does not exist')
    expect(apiGet).not.toHaveBeenCalledWith('/api/v1/factor-runs/run-new')
    expect(router.currentRoute.value.query.run).toBe('run-missing')
    wrapper.unmount()
  })
})
