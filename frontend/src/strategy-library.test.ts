import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import MarkdownDocument from './components/MarkdownDocument.vue'
import StrategiesView from './views/StrategiesView.vue'
import StrategyDetailView from './views/StrategyDetailView.vue'

const apiGet = vi.hoisted(() => vi.fn())
vi.mock('./api', () => ({
  api: { get: apiGet },
  DashboardApiError: class DashboardApiError extends Error {},
}))

const summaries = [
  { strategy_id: 'dual_ma_trend', display_name: '双均线趋势', summary: '使用均线判断趋势。' },
  { strategy_id: 'etf_rotation', display_name: 'ETF 轮动', summary: '按月配置 ETF。' },
]

function queryPlugin(): [typeof VueQueryPlugin, { queryClient: QueryClient }] {
  return [VueQueryPlugin, {
    queryClient: new QueryClient({ defaultOptions: { queries: { retry: false } } }),
  }]
}

describe('strategy library', () => {
  beforeEach(() => {
    apiGet.mockReset()
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/strategies') {
        return Promise.resolve({ strategies: summaries, components: {}, component_schemas: {}, capability_rules: [] })
      }
      return Promise.resolve({
        ...summaries[0],
        documentation_markdown: '# 双均线趋势\n\n摘要。\n\n## 信号\n\n内容',
      })
    })
  })

  it('lists strategy summaries with stable detail links', async () => {
    const router = createRouter({ history: createMemoryHistory(), routes: [
      { path: '/strategies', component: StrategiesView },
      { path: '/strategies/:strategyId', component: StrategyDetailView },
      { path: '/strategy-studies/new', component: { template: '<div />' } },
    ] })
    await router.push('/strategies')
    await router.isReady()
    const wrapper = mount(StrategiesView, { global: { plugins: [queryPlugin(), router] } })
    await flushPromises()

    expect(wrapper.findAll('.strategy-card')).toHaveLength(2)
    expect(wrapper.text()).toContain('双均线趋势')
    expect(wrapper.get('a[href="/strategies/etf_rotation"]').text()).toContain('ETF 轮动')
  })

  it('loads and renders one full strategy document', async () => {
    const router = createRouter({ history: createMemoryHistory(), routes: [
      { path: '/strategies', component: StrategiesView },
      { path: '/strategies/:strategyId', component: StrategyDetailView },
      { path: '/strategy-studies/new', component: { template: '<div />' } },
    ] })
    await router.push('/strategies/dual_ma_trend')
    await router.isReady()
    const wrapper = mount(StrategyDetailView, { global: { plugins: [queryPlugin(), router] } })
    await flushPromises()

    expect(apiGet).toHaveBeenCalledWith('/api/v1/strategies/dual_ma_trend')
    expect(wrapper.text()).toContain('信号')
    expect(wrapper.text()).toContain('内容')
  })

  it('sanitizes dangerous markup and hardens external links', () => {
    const wrapper = mount(MarkdownDocument, {
      props: {
        markdown: '# 标题\n\n<img src=x onerror="alert(1)"><script>alert(1)</script>[危险](javascript:alert(1)) [外部](https://example.com)',
      },
    })

    expect(wrapper.find('script').exists()).toBe(false)
    expect(wrapper.get('img').attributes('onerror')).toBeUndefined()
    const links = wrapper.findAll('a')
    expect(links.find((item) => item.text() === '危险')?.attributes('href')).toBeUndefined()
    const external = links.find((item) => item.text() === '外部')
    expect(external?.attributes('target')).toBe('_blank')
    expect(external?.attributes('rel')).toBe('noopener noreferrer')
  })
})
