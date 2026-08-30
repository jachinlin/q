import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import { createMemoryHistory, createRouter } from 'vue-router'

import App from './App.vue'
import { DashboardApiError } from './api'
import ErrorState from './components/ErrorState.vue'
import MetricCard from './components/MetricCard.vue'
import { formatDuration, formatPercent, shortHash, statusType } from './format'
import router from './router'

describe('dashboard shell contract', () => {
  it('keeps strategy studies and independent factor studies as separate routes', () => {
    expect(router.getRoutes().map((route) => route.path).sort()).toEqual([
      '/',
      '/data',
      '/market',
      '/notebook',
      '/settings',
      '/strategy-studies',
      '/strategy-studies/:strategyStudyId',
      '/strategy-studies/new',
      '/factor-studies',
      '/factor-studies/:factorStudyId',
      '/factor-studies/new',
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
      routes: ['/', '/market', '/data', '/strategy-studies', '/factor-studies', '/tasks', '/notebook', '/settings'].map(
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
    expect(wrapper.get('a[href="/settings"]').text()).toContain('设置')
  })

  it('never turns missing values into zero', () => {
    expect(formatPercent(null)).toBe('—')
    expect(formatPercent(Number.NaN)).toBe('—')
    expect(shortHash(null)).toBe('—')
    expect(statusType('UNKNOWN')).toBe('info')
    expect(formatDuration('2026-08-15T00:00:00Z', null, Date.parse('2026-08-15T01:02:03Z'))).toBe('1小时 2分')
  })

  it('uses distinct semantic colors for dataset freshness', () => {
    expect(statusType('CURRENT')).toBe('success')
    expect(statusType('STALE')).toBe('warning')
    expect(statusType('MISSING')).toBe('danger')
    expect(statusType('UNKNOWN')).toBe('info')
  })

  it('shows the structured API reason instead of a generic unavailable state', () => {
    const error = new DashboardApiError({
      code: 'DASHBOARD_INPUT_INVALID',
      message: 'max_positions must be positive',
      severity: 'SEVERE',
      retryable: false,
      remediation: '修改实验参数后重新校验。',
      request_id: 'request-1',
    }, 422)
    const wrapper = mount(ErrorState, { props: { error } })
    expect(wrapper.text()).toContain('DASHBOARD_INPUT_INVALID')
    expect(wrapper.text()).toContain('max_positions must be positive')
    expect(wrapper.text()).toContain('修改实验参数后重新校验。')
    expect(wrapper.text()).not.toContain('请检查本地服务状态后重试')
  })
})
