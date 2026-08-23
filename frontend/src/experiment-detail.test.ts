import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import ExperimentDetailView from './views/ExperimentDetailView.vue'

const apiGet = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  api: { get: apiGet, post: vi.fn(), patch: vi.fn() },
}))
vi.mock('vue-echarts', () => ({
  default: { name: 'VChart', props: ['option'], template: '<div class="chart-stub" />' },
}))

const run = {
  id: '01JTEST000000000000000001', experiment_id: 'exp-1', config: {}, config_hash: 'a'.repeat(64), catalog_hash: 'b'.repeat(64),
  status: 'SUCCEEDED', stage: 'PERSIST', research_mark: 'BASELINE', uses_test_region: false, task_id: 'task-1',
  artifact_dir: 'trusted', manifest_hash: 'c'.repeat(64), error: null, created_at: '2026-08-01T00:00:00Z',
  started_at: '2026-08-01T00:00:01Z', completed_at: '2026-08-01T00:01:00Z', tags: [],
  metrics: [{ name: 'annualized_return', value: 0.12, unit: null, p_value: null, adjusted_p_value: null }],
  artifacts: [{ artifact_type: 'performance', relative_path: 'performance.parquet', content_hash: 'd'.repeat(64), byte_count: 100, row_count: 2, schema: {} }],
}

function aggregate(kind: 'STRATEGY_BACKTEST' | 'FACTOR_STUDY') {
  return {
    experiment: {
      id: 'exp-1', baseline_run_id: run.id, created_at: '2026-08-01T00:00:00Z',
      definition: {
        name: kind === 'STRATEGY_BACKTEST' ? '双均线实验' : '价值因子研究', description: 'test', kind, tags: [],
        sample_windows: { train: { start: '2020-01-01', end: '2020-12-31' }, validation: { start: '2021-01-01', end: '2021-12-31' }, test: { start: '2022-01-01', end: '2022-12-31' } },
        governance: { test_budget: 1, correction: 'BONFERRONI' }, initial_run: {},
      },
    },
    runs: [run], tags: [],
  }
}

async function mountDetail() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/experiments/:experimentId', component: ExperimentDetailView }] })
  await router.push('/experiments/exp-1')
  await router.isReady()
  return mount(ExperimentDetailView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] } })
}

async function clickTab(wrapper: ReturnType<typeof mount>, label: string) {
  await vi.waitFor(() => expect(wrapper.text()).toContain(label))
  const item = wrapper.findAll('.el-tabs__item').find((node) => node.text() === label)
  if (!item) throw new Error(`missing tab: ${label}`)
  await item.trigger('click')
  await flushPromises()
}

describe('unified experiment detail', () => {
  beforeEach(() => apiGet.mockReset())

  it('renders strategy performance as a chart and trusted artifact table', async () => {
    apiGet.mockImplementation((path: string) => {
      if (typeof path !== 'string') return Promise.resolve({})
      if (path === '/api/v1/experiments/exp-1') return Promise.resolve(aggregate('STRATEGY_BACKTEST'))
      if (path.includes('/artifacts/performance')) return Promise.resolve({ items: [
        { trade_date: '2026-01-02', return: 0, cumulative_return: 0, drawdown: 0 },
        { trade_date: '2026-01-05', return: 0.1, cumulative_return: 0.1, drawdown: -0.02 },
      ], total: 2 })
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    const wrapper = await mountDetail()
    await clickTab(wrapper, '绩效')
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith(expect.stringContaining('/artifacts/performance')))
    await flushPromises()
    expect(wrapper.findComponent({ name: 'VChart' }).exists()).toBe(true)
    expect(wrapper.text()).toContain('cumulative_return')
    expect(wrapper.text()).toContain('annualized_return')
  })

  it('renders factor summary independently from strategy artifacts', async () => {
    apiGet.mockImplementation((path: string) => {
      if (typeof path !== 'string') return Promise.resolve({})
      if (path === '/api/v1/experiments/exp-1') return Promise.resolve(aggregate('FACTOR_STUDY'))
      if (path.includes('/artifacts/summary')) return Promise.resolve({ items: [
        { signal_variant: 'raw', factor_ref: 'book_to_price_mrq', horizon: 5, rank_ic_mean: 0.06, long_short_mean: 0.02 },
      ], total: 1 })
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    const wrapper = await mountDetail()
    await clickTab(wrapper, '摘要')
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith(expect.stringContaining('/artifacts/summary')))
    await flushPromises()
    expect(wrapper.findComponent({ name: 'VChart' }).exists()).toBe(true)
    expect(wrapper.text()).toContain('book_to_price_mrq')
    expect(wrapper.text()).not.toContain('cumulative_return')
  })
})
