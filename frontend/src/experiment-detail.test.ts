import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { ElMessageBox } from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import ExperimentDetailView from './views/ExperimentDetailView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiDelete = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  api: { get: apiGet, post: vi.fn(), patch: vi.fn(), delete: apiDelete },
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

function aggregate(
  _kind: 'STRATEGY_BACKTEST',
  runs: Array<Record<string, unknown>> = [run],
) {
  return {
    experiment: {
      id: 'exp-1', baseline_run_id: run.id, created_at: '2026-08-01T00:00:00Z',
      definition: {
        name: '双均线实验', description: 'test', tags: [],
        sample_windows: { train: { start: '2020-01-01', end: '2020-12-31' }, validation: { start: '2021-01-01', end: '2021-12-31' }, test: { start: '2022-01-01', end: '2022-12-31' } },
        governance: { test_budget: 1, correction: 'BONFERRONI' }, initial_run: {},
      },
    },
    runs, tags: [],
  }
}

async function mountDetail() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/experiments/:experimentId', component: ExperimentDetailView }] })
  await router.push('/experiments/exp-1')
  await router.isReady()
  const wrapper = mount(ExperimentDetailView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] } })
  return { wrapper, router }
}

async function clickTab(wrapper: ReturnType<typeof mount>, label: string) {
  await vi.waitFor(() => expect(wrapper.text()).toContain(label))
  const item = wrapper.findAll('.el-tabs__item').find((node) => node.text() === label)
  if (!item) throw new Error(`missing tab: ${label}`)
  await item.trigger('click')
  await flushPromises()
}

describe('unified experiment detail', () => {
  beforeEach(() => {
    apiGet.mockReset()
    apiDelete.mockReset()
  })

  it('renders strategy performance as a chart and trusted artifact table', async () => {
    apiGet.mockImplementation((path: string) => {
      if (typeof path !== 'string') return Promise.resolve({})
      if (path === '/api/v1/experiments/exp-1') return Promise.resolve(aggregate('STRATEGY_BACKTEST'))
      if (path.includes('/artifacts/quality_disclosure')) return Promise.resolve({ value: { calculation_mode: 'CASH_EXACT', risk_free_rate_annual: 0, undefined_metrics: {}, unavailable_dimensions: {}, attribution_method: 'CASH_EXACT_SECURITY', warnings: [] } })
      if (path.includes('/artifacts/performance')) return Promise.resolve({ items: [
        { trade_date: '2026-01-02', return: 0, cumulative_return: 0, drawdown: 0 },
        { trade_date: '2026-01-05', return: 0.1, cumulative_return: 0.1, drawdown: -0.02 },
      ], total: 2 })
      if (path.includes('/artifacts/monthly_returns')) return Promise.resolve({ items: [
        { year: 2026, month: 1, portfolio_return: 0.1, benchmark_return: 0.08, relative_return: 0.02 },
      ], total: 1 })
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    const { wrapper } = await mountDetail()
    await clickTab(wrapper, '绩效时序')
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith(expect.stringContaining('/artifacts/performance')))
    await flushPromises()
    expect(wrapper.findComponent({ name: 'VChart' }).exists()).toBe(true)
    expect(wrapper.text()).toContain('cumulative_return')
    expect(wrapper.text()).toContain('年化收益')
    const monthly = wrapper.find('input[value="monthly_returns"]')
    if (!monthly.exists()) throw new Error('missing monthly returns selector')
    await monthly.setValue(true)
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith(expect.stringContaining('/artifacts/monthly_returns')))
  })

  it('defaults to the latest Run, marks it clearly, and persists explicit selection in the URL', async () => {
    const failedRun = { ...run, id: '01JFAILED0000000000000001', status: 'FAILED', stage: 'STRATEGY_RUN', manifest_hash: null, metrics: [], artifacts: [] }
    const latestRun = { ...run, id: '01JLATEST0000000000000002', tags: [`rerun-of:${failedRun.id}`] }
    apiGet.mockImplementation((path: string) => {
      if (typeof path !== 'string') return Promise.resolve({})
      if (path === '/api/v1/experiments/exp-1') return Promise.resolve(aggregate('STRATEGY_BACKTEST', [failedRun, latestRun]))
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })

    const { wrapper, router } = await mountDetail()
    await vi.waitFor(() => expect(wrapper.text()).toContain('01JLATEST000'))

    expect(router.currentRoute.value.query.run).toBe(latestRun.id)
    expect(wrapper.text()).toContain('默认比较当前 Run 与 baseline')
    expect(wrapper.text()).toContain('实验协议')
    expect(wrapper.text()).toContain('当前 Run 配置')
    expect(wrapper.findAll('.viewing-run')).toHaveLength(1)
    expect(wrapper.find('.viewing-run').text()).toContain('01JLATEST000')

    const failedRunButton = wrapper.findAll('button').find((button) => button.text().includes('01JFAILED000'))
    if (!failedRunButton) throw new Error('missing failed Run selector')
    await failedRunButton.trigger('click')
    await flushPromises()

    expect(router.currentRoute.value.query.run).toBe(failedRun.id)
    expect(wrapper.find('.viewing-run').text()).toContain('01JFAILED000')
  })

  it('renders a fixed strategy core metric set instead of the first eight API metrics', async () => {
    apiGet.mockImplementation((path: string) => {
      if (typeof path !== 'string') return Promise.resolve({})
      if (path === '/api/v1/experiments/exp-1') return Promise.resolve(aggregate('STRATEGY_BACKTEST'))
      if (path.includes('/artifacts/quality_disclosure')) return Promise.resolve({ value: { calculation_mode: 'CASH_EXACT', risk_free_rate_annual: 0, undefined_metrics: { sharpe_ratio: 'ZERO_VOLATILITY' }, unavailable_dimensions: {}, attribution_method: 'CASH_EXACT_SECURITY', warnings: [] } })
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    const { wrapper } = await mountDetail()
    await flushPromises()
    expect(wrapper.findAll('.core-metrics .metric-card')).toHaveLength(8)
    expect(wrapper.text()).toContain('几何超额')
    expect(wrapper.text()).toContain('年化成本拖累')
    expect(wrapper.text()).not.toContain('Run 核心指标')
  })

  it('confirms and deletes a terminal Run while keeping the experiment page', async () => {
    const failedRun = { ...run, status: 'FAILED', manifest_hash: null, metrics: [], artifacts: [] }
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/experiments/exp-1') return Promise.resolve(aggregate('STRATEGY_BACKTEST', [failedRun]))
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    apiDelete.mockResolvedValue({ experiment_id: 'exp-1', run_id: run.id, status: 'DELETED' })
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue({} as never)
    const { wrapper, router } = await mountDetail()
    await vi.waitFor(() => expect(wrapper.text()).toContain('Run 执行失败'))

    const remove = wrapper.findAll('button').find((button) => button.text() === '删除')
    if (!remove) throw new Error('missing Run delete button')
    await remove.trigger('click')
    await vi.waitFor(() => expect(apiDelete).toHaveBeenCalledWith(`/api/v1/runs/${run.id}`))

    expect(router.currentRoute.value.path).toBe('/experiments/exp-1')
  })
})
