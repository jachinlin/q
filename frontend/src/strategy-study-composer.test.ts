import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import type { VueWrapper } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { parse, stringify } from 'yaml'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import StrategyStudyComposerView from './views/StrategyStudyComposerView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiPost = vi.hoisted(() => vi.fn())
vi.mock('./api', () => ({ api: { get: apiGet, post: apiPost }, DashboardApiError: class DashboardApiError extends Error {} }))

const definition = {
  name: '源研究', description: '', tags: ['trend'], start_date: '2020-01-01', end_date: '2024-01-01',
  strategy: { strategy_id: 'dual_ma_trend', parameters: { instrument_id: '510300.SH', short_window: 20, long_window: 60, long_weight: 1, flat_weight: 0, target_tolerance: 0.001 } },
  benchmark: '000300.SH', initial_cash_fen: 100000000,
  execution: { reference_price: 'OPEN', slippage_bps: 0, max_volume_participation: 0.1, limit_order_policy: 'REJECT' },
}

async function mountComposer(path = '/strategy-studies/new') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const router = createRouter({ history: createMemoryHistory(), routes: [
    { path: '/strategies/:strategyId', component: { template: '<div />' } },
    { path: '/strategy-studies', component: { template: '<div />' } },
    { path: '/strategy-studies/new', component: StrategyStudyComposerView },
    { path: '/strategy-studies/:strategyStudyId', component: { template: '<div />' } },
  ] })
  await router.push(path)
  await router.isReady()
  const wrapper = mount(StrategyStudyComposerView, { global: { plugins: [[VueQueryPlugin, { queryClient: client }], router] } })
  await flushPromises()
  return { wrapper, router }
}

function selectByField(wrapper: VueWrapper, field: string) {
  const component = wrapper.findAllComponents({ name: 'ElSelect' }).find((item) => item.attributes('data-field') === field)
  if (!component) throw new Error(`missing select ${field}`)
  return component
}

describe('strategy study composer', () => {
  beforeEach(() => {
    apiGet.mockReset()
    apiPost.mockReset()
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/strategies') return Promise.resolve({ strategies: [
        { strategy_id: 'dual_ma_trend', display_name: '双均线趋势', summary: '双均线摘要' },
        { strategy_id: 'etf_rotation', display_name: 'ETF 轮动', summary: 'ETF 摘要' },
        { strategy_id: 'stock_multifactor', display_name: '股票多因子', summary: '多因子摘要' },
      ], components: {}, component_schemas: {}, capability_rules: [] })
      if (path === '/api/v1/factor-studies/catalog') return Promise.resolve({ factors: [{ factor_id: 'book_to_price_mrq' }], universes: [], corrections: [], industry_policies: [], label_kinds: [] })
      return Promise.resolve({ definition })
    })
    apiPost.mockImplementation((path: string) => path.endsWith('/validate')
      ? Promise.resolve({ config_hash: 'a'.repeat(64), normalized: definition })
      : Promise.resolve({ id: 'new-study' }))
  })

  it('prefills a copied definition into the form and requires validation again', async () => {
    const { wrapper } = await mountComposer('/strategy-studies/new?from=source-1')
    const name = wrapper.get('input[data-field="name"]')
    expect((name.element as HTMLInputElement).value).toBe('源研究（副本）')
    expect(wrapper.find('.yaml-editor').exists()).toBe(false)
    const submit = wrapper.findAll('button').find((item) => item.text() === '提交')
    const validate = wrapper.findAll('button').find((item) => item.text() === '校验')
    if (!submit || !validate) throw new Error('missing actions')
    expect(submit.attributes('disabled')).toBeDefined()
    await validate.trigger('click')
    await flushPromises()
    const body = apiPost.mock.calls.find(([path]) => path === '/api/v1/strategy-studies/validate')?.[1] as { yaml: string }
    expect(parse(body.yaml).name).toBe('源研究（副本）')
    expect(submit.attributes('disabled')).toBeUndefined()
    await name.setValue('再次修改')
    expect(submit.attributes('disabled')).toBeDefined()
  })

  it('synchronizes valid YAML back to the form and blocks invalid YAML from switching', async () => {
    const { wrapper } = await mountComposer()
    const yamlButton = wrapper.get('input[value="yaml"]')
    await yamlButton.setValue()
    const editor = wrapper.get('.yaml-editor textarea')
    await editor.setValue(stringify({ ...definition, name: 'YAML 研究' }))
    const formButton = wrapper.get('input[value="form"]')
    await formButton.setValue()
    expect((wrapper.get('input[data-field="name"]').element as HTMLInputElement).value).toBe('YAML 研究')
    await yamlButton.setValue()
    await wrapper.get('.yaml-editor textarea').setValue('name: [')
    await formButton.setValue()
    expect(wrapper.find('.yaml-editor').exists()).toBe(true)
    expect(wrapper.text()).toContain('无法切换到表单')
  })

  it('resets strategy parameters and enforces a risk model for mean variance', async () => {
    const { wrapper } = await mountComposer()
    expect(wrapper.get('[data-strategy-profile]').text()).toContain('双均线摘要')
    selectByField(wrapper, 'strategy-id').vm.$emit('update:modelValue', 'etf_rotation')
    await flushPromises()
    expect(wrapper.find('[data-strategy-form="etf_rotation"]').exists()).toBe(true)
    expect(wrapper.get('[data-strategy-profile]').text()).toContain('ETF 摘要')
    expect(wrapper.get('[data-strategy-profile] a').attributes('href')).toBe('/strategies/etf_rotation')
    selectByField(wrapper, 'construction-model').vm.$emit('change', 'mean_variance')
    await flushPromises()
    await wrapper.get('input[value="yaml"]').setValue()
    const value = parse((wrapper.get('.yaml-editor textarea').element as HTMLTextAreaElement).value)
    expect(value.strategy.parameters.pipeline.construction.model_id).toBe('mean_variance')
    expect(value.strategy.parameters.pipeline.risk.model_id).toBe('sample_cov')
  })

  it('submits the exact YAML that passed backend validation', async () => {
    const { wrapper } = await mountComposer()
    const validate = wrapper.findAll('button').find((item) => item.text() === '校验')
    const submit = wrapper.findAll('button').find((item) => item.text() === '提交')
    if (!validate || !submit) throw new Error('missing actions')
    await validate.trigger('click')
    await flushPromises()
    const validated = (apiPost.mock.calls.find(([path]) => path === '/api/v1/strategy-studies/validate')?.[1] as { yaml: string }).yaml
    await submit.trigger('click')
    await flushPromises()
    expect(apiPost).toHaveBeenCalledWith('/api/v1/strategy-studies', { yaml: validated })
  })
})
