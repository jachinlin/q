import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import { createMemoryHistory, createRouter } from 'vue-router'

import App from './App.vue'
import MetricCard from './components/MetricCard.vue'
import { formatDuration, formatPercent, shortHash, statusType } from './format'
import router from './router'

describe('dashboard shell contract', () => {
  it('keeps six top-level research areas and nests backtests under experiments', () => {
    expect(router.getRoutes().map((route) => route.path).sort()).toEqual([
      '/',
      '/data',
      '/experiments',
      '/experiments/:experimentId',
      '/factors',
      '/market',
      '/notebook',
      '/tasks',
    ].sort())
    expect(router.hasRoute('backtest')).toBe(false)
    expect(router.getRoutes().find((route) => route.path === '/tasks')?.meta.title).toBe('运行中心')
  })

  it('renders compact metric cards with explicit hints', () => {
    const wrapper = mount(MetricCard, {
      props: { label: '累计收益', value: '+12.35%', hint: '基准 +8.20%', tone: 'red' },
    })
    expect(wrapper.text()).toContain('累计收益')
    expect(wrapper.text()).toContain('+12.35%')
    expect(wrapper.classes()).toContain('tone-red')
  })

  it('links the sidebar to the embedded Notebook workspace', async () => {
    const shellView = { template: '<div />' }
    const shellRouter = createRouter({
      history: createMemoryHistory(),
      routes: ['/', '/market', '/data', '/experiments', '/factors', '/tasks', '/notebook'].map(
        (path) => ({ path, component: shellView }),
      ),
    })
    await shellRouter.push('/')
    await shellRouter.isReady()

    const wrapper = mount(App, { global: { plugins: [shellRouter] } })
    const notebook = wrapper.get('a[href="/notebook"]')

    expect(notebook.text()).toContain('Notebook')
    expect(notebook.attributes('target')).toBeUndefined()
    expect(notebook.text()).not.toContain('↗')
  })

  it('never turns missing values into zero', () => {
    expect(formatPercent(null)).toBe('—')
    expect(formatPercent(Number.NaN)).toBe('—')
    expect(shortHash(null)).toBe('—')
    expect(statusType('UNKNOWN')).toBe('info')
    expect(formatDuration('2026-08-15T00:00:00Z', null, Date.parse('2026-08-15T01:02:03Z'))).toBe('1小时 2分')
  })
})
