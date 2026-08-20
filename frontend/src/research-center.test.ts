import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import ResearchCenterView from './views/ResearchCenterView.vue'
import ResearchComposerView from './views/ResearchComposerView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiPost = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  DashboardApiError: class extends Error { remediation = null },
  api: { get: apiGet, post: apiPost },
}))

function client() {
  return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
}

describe('unified research center', () => {
  beforeEach(() => {
    apiGet.mockReset()
    apiPost.mockReset()
  })

  it('lists immutable research families and opens one detail route', async () => {
    apiGet.mockResolvedValue({
      items: [{
        id: 'family-1', name: '双均线研究', hypothesis: '趋势延续', strategy_id: 'dual_ma_trend',
        research_mode: 'BACKTEST_EXPERIMENT', config_hash: 'a'.repeat(64), mark: 'CANDIDATE',
        created_at: '2026-08-18T00:00:00Z',
        latest_execution: { id: 'execution-1', status: 'SUCCEEDED', selected_variant_id: 'variant-1' },
      }], page: 1, page_size: 100, total: 1,
    })
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/research', component: ResearchCenterView },
        { path: '/research/new', component: { template: '<div />' } },
        { path: '/research/:familyId', component: { template: '<div />' } },
      ],
    })
    await router.push('/research')
    const wrapper = mount(ResearchCenterView, { global: { plugins: [[VueQueryPlugin, { queryClient: client() }], router] } })
    await flushPromises()

    expect(wrapper.text()).toContain('双均线研究')
    expect(wrapper.text()).toContain('SUCCEEDED')
    expect(wrapper.get('a[href="/research/family-1"]').attributes('href')).toBe('/research/family-1')
  })

  it('synchronizes the form and YAML, then uses backend candidate preview', async () => {
    const template = 'name: 双均线\nhypothesis: 趋势\nresearch_mode: BACKTEST_EXPERIMENT\nstrategy_id: dual_ma_trend\nresearch_protocol: {}\n'
    apiGet.mockImplementation((path: string) => {
      if (path.endsWith('/templates')) return Promise.resolve({ items: [{ strategy_id: 'dual_ma_trend', label: '双均线趋势', signal_kind: 'DIRECTIONAL', yaml: template }] })
      return Promise.resolve({ components: [{ component_id: 'dual_ma_directional' }], templates: [] })
    })
    apiPost.mockResolvedValue({
      config_hash: 'b'.repeat(64), normalized_yaml: template.replace('双均线', '双均线规范'),
      variant_count: 9, variants: [{ variant_id: 'variant-1', composition_hash: 'c'.repeat(64), parameters: { 'signal.short_window_sessions': 20 } }],
      required_datasets: ['daily_bar', 'trade_calendar'], signal_kind: 'DIRECTIONAL',
    })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/research/new', component: ResearchComposerView }, { path: '/research', component: { template: '<div />' } }] })
    await router.push('/research/new')
    const wrapper = mount(ResearchComposerView, { global: { plugins: [[VueQueryPlugin, { queryClient: client() }], router] } })
    await flushPromises()

    const name = wrapper.findAll('input')[0]
    await name.setValue('修改后的研究')
    await name.trigger('change')
    expect((wrapper.find('.yaml-editor textarea').element as HTMLTextAreaElement).value).toContain('name: 修改后的研究')
    const validateButton = wrapper.findAll('button').find((item) => item.text().includes('校验与预览'))
    await validateButton?.trigger('click')
    await flushPromises()
    expect(apiPost).toHaveBeenCalledWith('/api/v1/research/validate', expect.objectContaining({ config_yaml: expect.stringContaining('name: 修改后的研究') }))
    expect(wrapper.text()).toContain('9')
    expect(wrapper.text()).toContain('daily_bar · trade_calendar')
    expect(wrapper.text()).toContain('signal.short_window_sessions')
  })
})
