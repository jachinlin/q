import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { describe, expect, it, vi } from 'vitest'

import StrategyStudyDetailView from './views/StrategyStudyDetailView.vue'

const apiGet = vi.hoisted(() => vi.fn())
vi.mock('./api', () => ({ api: { get: apiGet, post: vi.fn(), delete: vi.fn() } }))
vi.mock('vue-echarts', () => ({ default: { name: 'VChart', props: ['option'], template: '<div class="chart-stub" />' } }))

describe('strategy study detail', () => {
  it('shows one result without comparison or sample governance', async () => {
    apiGet.mockImplementation((path: string) => path.includes('/artifacts/performance')
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
  })
})
