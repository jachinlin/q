import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { describe, expect, it, vi } from 'vitest'

import StrategyStudyComposerView from './views/StrategyStudyComposerView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiPost = vi.hoisted(() => vi.fn())
vi.mock('./api', () => ({ api: { get: apiGet, post: apiPost }, DashboardApiError: class DashboardApiError extends Error {} }))

describe('strategy study composer', () => {
  it('prefills a copied definition and requires validation again', async () => {
    apiGet.mockImplementation((path: string) => path === '/api/v1/strategies'
      ? Promise.resolve({ strategies: ['dual_ma_trend'], components: {}, component_schemas: {}, capability_rules: [] })
      : Promise.resolve({ definition: { name: '源研究', description: '', tags: [], start_date: '2020-01-01', end_date: '2024-01-01', strategy: { strategy_id: 'dual_ma_trend', parameters: {} }, benchmark: '000300.SH', initial_cash_fen: 100000000, execution: { reference_price: 'OPEN', slippage_bps: 0, max_volume_participation: 0.1, limit_order_policy: 'REJECT' } } }))
    apiPost.mockResolvedValue({ config_hash: 'a'.repeat(64), normalized: {} })
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/strategy-studies/new', component: StrategyStudyComposerView }, { path: '/strategy-studies/:strategyStudyId', component: { template: '<div />' } }] })
    await router.push('/strategy-studies/new?from=source-1'); await router.isReady()
    const wrapper = mount(StrategyStudyComposerView, { global: { plugins: [[VueQueryPlugin, { queryClient: client }], router] } })
    await vi.waitFor(() => expect((wrapper.get('.yaml-editor textarea').element as HTMLTextAreaElement).value).toContain('源研究（副本）'))
    const submit = wrapper.findAll('button').find((item) => item.text() === '提交')
    const validate = wrapper.findAll('button').find((item) => item.text() === '校验')
    if (!submit || !validate) throw new Error('missing actions')
    expect(submit.attributes('disabled')).toBeDefined()
    await validate.trigger('click'); await flushPromises()
    expect(apiPost).toHaveBeenCalledWith('/api/v1/strategy-studies/validate', expect.any(Object))
    expect(submit.attributes('disabled')).toBeUndefined()
  })
})
