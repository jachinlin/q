import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { describe, expect, it, vi } from 'vitest'

import ExperimentComposerView from './views/ExperimentComposerView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiPost = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  api: { get: apiGet, post: apiPost },
  DashboardApiError: class DashboardApiError extends Error {},
}))

const schema = (properties: Record<string, unknown>) => ({ type: 'object', additionalProperties: false, properties })
const catalog = {
  strategies: ['dual_ma_trend', 'etf_rotation', 'stock_multifactor'],
  components: {
    alpha: ['multi_factor_composite', 'single_factor'], risk: ['none', 'sample_cov', 'shrinkage'],
    cost: ['fixed_bps', 'linear_impact', 'sqrt_impact'], construction: ['mean_variance', 'top_n_equal_weight'], constraint: ['long_only'],
  },
  component_schemas: {
    alpha: [{ model_id: 'multi_factor_composite', params_schema: schema({}) }, { model_id: 'single_factor', params_schema: schema({}) }],
    risk: [{ model_id: 'none', params_schema: schema({}) }, { model_id: 'sample_cov', params_schema: schema({ lookback: { type: 'integer', minimum: 2 } }) }, { model_id: 'shrinkage', params_schema: schema({ lookback: { type: 'integer', minimum: 2 }, shrinkage: { type: 'number', minimum: 0, maximum: 1 } }) }],
    cost: [{ model_id: 'fixed_bps', params_schema: schema({}) }],
    construction: [{ model_id: 'top_n_equal_weight', params_schema: schema({ top_n: { type: 'integer', minimum: 1 } }) }],
    constraint: [{ model_id: 'long_only', params_schema: schema({ max_positions: { type: 'integer', minimum: 1 }, max_turnover: { type: 'number', minimum: 0, maximum: 1 } }) }],
  },
  capability_rules: [],
}

describe('experiment composer schema synchronization', () => {
  it('derives fields from backend schema and follows direct YAML edits', async () => {
    apiGet.mockResolvedValue(catalog)
    apiPost.mockImplementation((path: string) => {
      if (path === '/api/v1/experiments/validate') return Promise.resolve({ config_hash: 'a'.repeat(64), normalized: {} })
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/experiments/new', component: ExperimentComposerView }, { path: '/experiments/:experimentId', component: { template: '<div />' } }] })
    await router.push('/experiments/new')
    await router.isReady()
    const wrapper = mount(ExperimentComposerView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] } })
    await vi.waitFor(() => expect(wrapper.text()).toContain('stock_multifactor'))
    const template = wrapper.findAll('.template-strip button').find((item) => item.text().includes('stock_multifactor'))
    if (!template) throw new Error('missing stock template')
    await template.trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('construction.top_n')
    expect(wrapper.text()).toContain('constraint.max_positions')

    const editor = wrapper.get('.yaml-editor textarea')
    const yaml = (editor.element as HTMLTextAreaElement).value
    await editor.setValue(yaml.replace('risk: {model_id: none}', 'risk: {model_id: shrinkage, params: {lookback: 120, shrinkage: 0.2}}'))
    await flushPromises()
    expect(wrapper.text()).toContain('risk.lookback')
    expect(wrapper.text()).toContain('risk.shrinkage')

    const validateButton = wrapper.findAll('button').find((item) => item.text() === '校验')
    const submitButton = wrapper.findAll('button').find((item) => item.text() === '提交')
    if (!validateButton || !submitButton) throw new Error('missing composer actions')
    await validateButton.trigger('click')
    await vi.waitFor(() => expect(submitButton.attributes('disabled')).toBeUndefined())
    await editor.setValue(`${(editor.element as HTMLTextAreaElement).value}\n`)
    await flushPromises()
    expect(submitButton.attributes('disabled')).toBeDefined()
  })
})
