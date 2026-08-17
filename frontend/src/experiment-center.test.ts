import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ElMessageBox } from 'element-plus'

import ExperimentDetailView from './views/ExperimentDetailView.vue'
import ExperimentsView from './views/ExperimentsView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiPost = vi.hoisted(() => vi.fn())
const apiDelete = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  api: {
    get: apiGet,
    post: apiPost,
    delete: apiDelete,
    patch: vi.fn(),
  },
}))
vi.mock('vue-echarts', () => ({ default: { template: '<div class="chart-stub" />' } }))

describe('experiment center workspace', () => {
  beforeEach(() => {
    apiGet.mockReset()
    apiPost.mockReset()
    apiDelete.mockReset()
  })

  afterEach(() => {
    document.body.innerHTML = ''
    vi.restoreAllMocks()
  })

  it('submits the selected YAML template and opens the new experiment', async () => {
    apiGet.mockResolvedValue({ items: [], page: 1, page_size: 25, total: 0 })
    apiPost.mockResolvedValue({ experiment_id: 'experiment-new', task_id: 'task-new', status: 'QUEUED' })
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/experiments', component: ExperimentsView },
        { path: '/experiments/:experimentId', name: 'experiment-detail', component: { template: '<div />' } },
      ],
    })
    await router.push('/experiments')
    await router.isReady()
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const wrapper = mount(ExperimentsView, {
      attachTo: document.body,
      global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
    })
    await flushPromises()

    expect(wrapper.text()).not.toContain('数据身份')
    expect(wrapper.find('.experiment-heading').exists()).toBe(true)
    expect(wrapper.find('.experiment-hero').exists()).toBe(false)
    expect(wrapper.findAll('.metrics-grid .metric-card')).toHaveLength(4)
    expect(wrapper.text()).not.toContain('比较已选')
    expect(wrapper.find('input[type="checkbox"]').exists()).toBe(false)

    const openButton = wrapper.findAll('button').find((button) => button.text().includes('提交实验'))
    expect(openButton).toBeTruthy()
    await openButton!.trigger('click')
    await flushPromises()
    const submitButton = Array.from(document.querySelectorAll('button')).find((button) => button.textContent?.includes('提交并运行'))
    expect(submitButton).toBeTruthy()
    submitButton!.click()
    await vi.waitFor(() => expect(apiPost).toHaveBeenCalledTimes(1))
    await flushPromises()

    expect(apiPost).toHaveBeenCalledWith('/api/v1/experiments', {
      config_yaml: expect.stringContaining('strategy_id: etf_rotation'),
    })
    expect(router.currentRoute.value.fullPath).toBe('/experiments/experiment-new?tab=overview')
    wrapper.unmount()
  })

  it('deletes a closed experiment only after explicit confirmation', async () => {
    apiGet.mockResolvedValue({ items: [experimentSummary('SUCCEEDED')], page: 1, page_size: 25, total: 1 })
    apiDelete.mockResolvedValue({ experiment_id: 'experiment-1', status: 'DELETED' })
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue({} as never)
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/experiments', component: ExperimentsView },
        { path: '/experiments/:experimentId', name: 'experiment-detail', component: { template: '<div />' } },
      ],
    })
    await router.push('/experiments')
    await router.isReady()
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const wrapper = mount(ExperimentsView, {
      attachTo: document.body,
      global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
    })
    await flushPromises()

    const deleteButton = wrapper.find('.row-delete')
    expect(deleteButton.attributes('disabled')).toBeUndefined()
    await deleteButton.trigger('click')
    await vi.waitFor(() => expect(apiDelete).toHaveBeenCalledWith('/api/v1/experiments/experiment-1'))
    expect(ElMessageBox.confirm).toHaveBeenCalledWith(
      expect.stringContaining('永久删除'),
      '确认删除实验',
      expect.objectContaining({ type: 'error' }),
    )
    wrapper.unmount()
  })

  it('keeps active experiments disabled and respects a cancelled confirmation', async () => {
    apiGet.mockResolvedValue({
      items: [experimentSummary('RUNNING'), { ...experimentSummary('FAILED'), id: 'experiment-2' }],
      page: 1,
      page_size: 25,
      total: 2,
    })
    vi.spyOn(ElMessageBox, 'confirm').mockRejectedValue(new Error('cancelled'))
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/experiments', component: ExperimentsView },
        { path: '/experiments/:experimentId', name: 'experiment-detail', component: { template: '<div />' } },
      ],
    })
    await router.push('/experiments')
    await router.isReady()
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const wrapper = mount(ExperimentsView, {
      attachTo: document.body,
      global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
    })
    await flushPromises()

    const deleteButtons = wrapper.findAll('.row-delete')
    expect(deleteButtons[0]?.attributes('disabled')).toBeDefined()
    await deleteButtons[1]?.trigger('click')
    await flushPromises()
    expect(apiDelete).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('loads immutable backtest artifacts only inside a successful experiment', async () => {
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/experiments/experiment-1') return Promise.resolve(experimentDetail())
      if (path === '/api/v1/experiments/experiment-1/backtest') return Promise.resolve(backtestResult())
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/experiments', component: { template: '<div />' } },
        { path: '/experiments/:experimentId', name: 'experiment-detail', component: ExperimentDetailView },
        { path: '/tasks', component: { template: '<div />' } },
      ],
    })
    await router.push('/experiments/experiment-1?tab=backtest')
    await router.isReady()
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const wrapper = mount(ExperimentDetailView, {
      global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
    })
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith('/api/v1/experiments/experiment-1/backtest'))
    await flushPromises()

    expect(wrapper.text()).toContain('策略与基准净值')
    expect(wrapper.text()).toContain('执行与组合质量')
    expect(wrapper.text()).not.toContain('运行身份')
    expect(wrapper.find('.el-alert').exists()).toBe(true)
    expect(wrapper.find('.el-alert').text()).toContain('公司行动')
    expect(wrapper.find('.el-alert').text()).not.toContain('PIT')
    expect(wrapper.find('.el-alert').text()).not.toContain('CORPORATE_ACTIONS_NOT_APPLIED')
    expect(wrapper.text()).toContain('2024-02-01')
    const tabs = wrapper.findAll('.section-tabs [role="tab"]')
    expect(tabs).toHaveLength(6)
    await tabs[1]!.trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('2024')
    await tabs[2]!.trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('SUSPENDED')
    await tabs[5]!.trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('beta')
    expect(apiGet).toHaveBeenCalledWith('/api/v1/experiments/experiment-1')
    wrapper.unmount()
  })

  it('keeps unpublished backtest artifacts closed while an experiment is queued', async () => {
    apiGet.mockResolvedValue({ ...experimentDetail(), status: 'QUEUED', completed_at: null, artifacts: [] })
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/experiments', component: { template: '<div />' } },
        { path: '/experiments/:experimentId', name: 'experiment-detail', component: ExperimentDetailView },
        { path: '/tasks', component: { template: '<div />' } },
      ],
    })
    await router.push('/experiments/experiment-1?tab=backtest')
    await router.isReady()
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const wrapper = mount(ExperimentDetailView, {
      global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
    })
    await vi.waitFor(() => expect(wrapper.text()).toContain('实验正在后台执行'))
    await flushPromises()

    expect(apiGet).not.toHaveBeenCalledWith('/api/v1/experiments/experiment-1/backtest')
    expect(router.currentRoute.value.query.tab).toBe('overview')
    expect(wrapper.text()).not.toContain('可复现身份')
    expect(wrapper.find('.identity-panel').exists()).toBe(false)
    wrapper.unmount()
  })
})

function experimentDetail() {
  return {
    id: 'experiment-1', strategy_id: 'etf_rotation', status: 'SUCCEEDED', research_mark: 'UNREVIEWED',
    data_hash: 'a'.repeat(64), config_hash: 'b'.repeat(64), fingerprint: 'c'.repeat(64),
    created_at: '2026-08-15T01:00:00Z', started_at: '2026-08-15T01:01:00Z', completed_at: '2026-08-15T01:10:00Z',
    tags: [], source_tree_hash: 'd'.repeat(64), git_commit_hash: null, lockfile_hash: 'e'.repeat(64), rulebook_hash: 'f'.repeat(64),
    note: null, latest_task: { id: 'task-1', status: 'SUCCEEDED' }, config: { strategy_id: 'etf_rotation' },
    metrics: [{ name: 'cumulative_return', value: 0.12, unit: null }],
    artifacts: [{ name: 'manifest.json', type: 'JSON', content_hash: '1'.repeat(64), metadata: {} }], audit: [],
  }
}

function experimentSummary(status: string) {
  return {
    id: 'experiment-1', strategy_id: 'etf_rotation', status, research_mark: 'UNREVIEWED',
    data_hash: 'a'.repeat(64), config_hash: 'b'.repeat(64), fingerprint: 'c'.repeat(64),
    created_at: '2026-08-15T01:00:00Z', completed_at: status === 'RUNNING' ? null : '2026-08-15T01:10:00Z',
    tags: [], metrics: { cumulative_return: 0.12, sharpe_ratio: 1.2, max_drawdown: -0.08 },
  }
}

function backtestResult() {
  return {
    experiment_id: 'experiment-1', manifest_hash: '1'.repeat(64),
    metrics: {
      cumulative_return: 0.12, benchmark_cumulative_return: 0.08, annualized_return: 0.1,
      relative_cumulative_return: 0.04, sharpe_ratio: 1.2, sortino_ratio: 1.4,
      max_drawdown: -0.08, calmar_ratio: 1.25, max_drawdown_peak_date: '2024-01-15',
      max_drawdown_trough_date: '2024-02-01', max_drawdown_recovery_date: '2024-03-01',
      annual_returns: [{ year: 2024, portfolio_return: 0.12, benchmark_return: 0.08, relative_return: 0.04 }],
    },
    nav: [{ trade_date: '2024-01-02', portfolio_nav: 1, benchmark_nav: 1 }], drawdown: [], monthly_returns: [], exposures: [], attribution: [],
    execution_summary: [{ side: 'SELL', reason_code: 'SUSPENDED', order_count: 1, requested_quantity: 100, filled_quantity: 0, unpriced_order_count: 1 }],
    quality: {
      warnings: ['CORPORATE_ACTIONS_NOT_APPLIED'],
      undefined_metrics: { beta: 'ZERO_BENCHMARK_VARIANCE' },
    },
  }
}
