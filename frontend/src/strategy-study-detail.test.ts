import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import StrategyStudyDetailView from './views/StrategyStudyDetailView.vue'

const apiGet = vi.hoisted(() => vi.fn())
vi.mock('./api', () => ({ api: { get: apiGet, post: vi.fn(), delete: vi.fn() } }))
vi.mock('vue-echarts', () => ({ default: { name: 'VChart', props: ['option'], template: '<div class="chart-stub" />' } }))

describe('strategy study detail', () => {
  beforeEach(() => apiGet.mockReset())

  it('shows one result without comparison or sample governance', async () => {
    apiGet.mockImplementation((path?: string) => path?.includes('/artifacts/performance')
      ? Promise.resolve({ items: [{ trade_date: '2024-01-01', cumulative_return: 0.1, benchmark_cumulative_return: 0.05 }] })
      : Promise.resolve({ id: 'study-1', definition: { name: '单项研究', description: '', tags: [], start_date: '2020-01-01', end_date: '2024-01-01', strategy: { strategy_id: 'dual_ma_trend', parameters: {} }, benchmark: '000300.SH', initial_cash_fen: 100000000, execution: {} }, config_hash: 'a'.repeat(64), catalog_hash: 'b'.repeat(64), status: 'SUCCEEDED', stage: 'PUBLISH', task_id: 'task-1', artifact_dir: 'trusted', manifest_hash: 'c'.repeat(64), error: null, created_at: '2026-08-01T00:00:00Z', started_at: '2026-08-01T00:00:01Z', completed_at: '2026-08-01T00:01:00Z', metrics: [], artifacts: [] }))
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/strategy-studies/new', component: { template: '<div />' } }, { path: '/strategy-studies/:strategyStudyId', component: StrategyStudyDetailView }] })
    await router.push('/strategy-studies/study-1'); await router.isReady()
    const wrapper = mount(StrategyStudyDetailView, { global: { plugins: [[VueQueryPlugin, { queryClient: client }], router] } })
    await vi.waitFor(() => expect(wrapper.text()).toContain('单项研究'))
    expect(wrapper.find('.chart-stub').exists()).toBe(true)
    expect(wrapper.text()).not.toContain('Run 比较')
    expect(wrapper.text()).not.toContain('TEST 预算')
    expect(wrapper.text()).toContain('复制研究')
    expect(wrapper.find('.detail-hero').exists()).toBe(true)
    expect(wrapper.find('.governance-grid').exists()).toBe(false)
    expect(wrapper.text()).not.toContain('状态 / 阶段')
    expect(wrapper.text()).toContain('dual_ma_trend · 000300.SH')
    wrapper.unmount()
  })

  it('shows the same structured task progress as factor study detail', async () => {
    const runningStudy = {
      id: 'study-1',
      definition: { name: 'ETF 轮动', description: '运行中的研究', tags: ['trend'], start_date: '2018-01-01', end_date: '2024-12-31', strategy: { strategy_id: 'etf_rotation', parameters: {} }, benchmark: '000300.SH', initial_cash_fen: 100000000, execution: {} },
      config_hash: 'a'.repeat(64), catalog_hash: 'b'.repeat(64), status: 'RUNNING', stage: 'BACKTEST', task_id: 'task-1',
      artifact_dir: null, manifest_hash: null, error: null, created_at: '2026-08-30T00:00:00Z', started_at: '2026-08-30T00:00:01Z', completed_at: null,
      metrics: [], artifacts: [],
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
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/strategy-studies/new', component: { template: '<div />' } }, { path: '/strategy-studies/:strategyStudyId', component: StrategyStudyDetailView }] })
    await router.push('/strategy-studies/study-1'); await router.isReady()
    const wrapper = mount(StrategyStudyDetailView, { global: { plugins: [[VueQueryPlugin, { queryClient: client }], router] } })

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
