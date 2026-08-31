import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { ElMessageBox } from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import StrategyStudiesView from './views/StrategyStudiesView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiDelete = vi.hoisted(() => vi.fn())
vi.mock('./api', () => ({ api: { get: apiGet, delete: apiDelete } }))

const study = {
  id: 'study-1',
  definition: { name: '双均线研究', description: 'test', strategy: { strategy_id: 'dual_ma_trend' }, start_date: '2020-01-01', end_date: '2024-12-31' },
  status: 'FAILED', stage: 'BACKTEST', created_at: '2026-08-23T00:00:00Z',
}

describe('strategy study list', () => {
  beforeEach(() => { apiGet.mockReset(); apiDelete.mockReset() })

  it('shows a single study lifecycle and deletes a terminal study', async () => {
    apiGet.mockResolvedValue({ items: [study] })
    apiDelete.mockResolvedValue({ strategy_study_id: study.id, status: 'DELETED' })
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue({} as never)
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/strategies', component: { template: '<div />' } }, { path: '/strategy-studies', component: StrategyStudiesView }, { path: '/strategy-studies/new', component: { template: '<div />' } }, { path: '/strategy-studies/:strategyStudyId', component: { template: '<div />' } }] })
    await router.push('/strategy-studies'); await router.isReady()
    const wrapper = mount(StrategyStudiesView, { global: { plugins: [[VueQueryPlugin, { queryClient: client }], router] } })
    await vi.waitFor(() => expect(wrapper.text()).toContain('双均线研究'))
    expect(wrapper.get('a[href="/strategies"]').text()).toContain('策略库')
    expect(wrapper.text()).not.toContain('Run')
    const button = wrapper.findAll('button').find((item) => item.text() === '删除')
    if (!button) throw new Error('missing delete button')
    await button.trigger('click'); await flushPromises()
    expect(apiDelete).toHaveBeenCalledWith('/api/v1/strategy-studies/study-1')
  })
})
