import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import FactorStudiesView from './views/FactorStudiesView.vue'
import FactorStudyComposerView from './views/FactorStudyComposerView.vue'
import FactorStudyDetailView from './views/FactorStudyDetailView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiPost = vi.hoisted(() => vi.fn())
const apiPut = vi.hoisted(() => vi.fn())
const apiDelete = vi.hoisted(() => vi.fn())
vi.mock('./api', () => ({
  api: { get: apiGet, post: apiPost, put: apiPut, delete: apiDelete },
  DashboardApiError: class DashboardApiError extends Error {},
}))
vi.mock('vue-echarts', () => ({ default: { name: 'VChart', props: ['option'], template: '<div class="chart-stub" />' } }))

const definition = {
  name: '价值动量研究', description: '候选诊断', tags: ['factor'], start_date: '2018-01-01', end_date: '2022-12-31',
  correction: 'BH_FDR', factor_ids: ['book_to_price_mrq'], universe: { name: 'CN_STOCK_STANDARD' }, horizons: [5], quantiles: 5,
  industry: { taxonomy: 'SW2021', unclassified_policy: 'EXCLUDE' }, cost_bps_scenarios: [5, 10, 20],
}
const study = {
  id: 'study-1', definition, config_hash: 'a'.repeat(64), catalog_hash: 'b'.repeat(64), status: 'SUCCEEDED', stage: 'PUBLISH', task_id: 'task-1',
  artifact_dir: 'trusted', manifest_hash: 'c'.repeat(64), error: null, created_at: '2026-08-01T00:00:00Z', started_at: '2026-08-01T00:00:01Z', completed_at: '2026-08-01T00:01:00Z',
  metrics: [], artifacts: [], decisions: [], matrix_total: 1, candidate_count: 0, discarded_count: 0, unreviewed_count: 1,
}
const matrixRow = {
  signal_variant: 'DIRECTION_ADJUSTED', label_kind: 'THEORETICAL_FORWARD_RETURN', factor_ref: 'book_to_price_mrq', horizon: 5,
  rank_ic_mean: 0.06, rank_ic_hac_t_stat: 2.4, rank_ic_adjusted_p_value: 0.04, monotonicity_mean: 0.8,
  gross_spread_mean: 0.02, break_even_cost_bps: 18, total_turnover_mean: 0.7,
  summary_metrics: {
    rank_ic_mean: 0.06,
    pearson_ic_sample_std: 0.12,
    rank_ic_positive_streak_start: '2022-01-03',
    rank_ic_hac_t_stat: 2.4,
    pearson_ic_hac_hac_p_value: 0.00001234,
    rank_ic_hac_hac_invalid_reason: null,
    long_short_mean: 0.02,
    long_short_valid_count: 23,
    monotonicity_mean: 0.8,
    monotonic_day_rate: 0.75,
    break_even_cost_bps: 18,
    total_turnover_mean: 0.7,
    unexpected_summary_metric: 7.123456789,
  },
  decision: null,
}
const catalog = { factors: [{ factor_id: 'book_to_price_mrq' }, { factor_id: 'momentum_120_20' }], universes: ['CN_STOCK_STANDARD'], corrections: ['BONFERRONI', 'BH_FDR'], industry_policies: ['EXCLUDE', 'UNCLASSIFIED'], label_kinds: ['THEORETICAL_FORWARD_RETURN', 'EXECUTABLE_FORWARD_RETURN'] }

function client() { return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } }) }

describe('independent factor study dashboard', () => {
  beforeEach(() => { apiGet.mockReset(); apiPost.mockReset(); apiPut.mockReset(); apiDelete.mockReset() })

  it('shows workbench counts, review filters and explicit study dimensions', async () => {
    apiGet.mockResolvedValue({ items: [study] })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/factor-studies', component: FactorStudiesView }, { path: '/factor-studies/new', component: { template: '<div />' } }, { path: '/factor-studies/:factorStudyId', component: { template: '<div />' } }] })
    await router.push('/factor-studies'); await router.isReady()
    const wrapper = mount(FactorStudiesView, { global: { plugins: [[VueQueryPlugin, { queryClient: client() }], router] } })
    await vi.waitFor(() => expect(wrapper.text()).toContain('价值动量研究'))
    expect(wrapper.text()).toContain('待评审矩阵行')
    expect(wrapper.text()).toContain('1 因子 · 5D')
    expect(wrapper.find('[aria-label="人工结论筛选"]').exists()).toBe(true)
  })

  it('synchronizes form and YAML and invalidates the last backend validation', async () => {
    apiGet.mockResolvedValue(catalog)
    apiPost.mockImplementation((path: string) => path.endsWith('/validate') ? Promise.resolve({ config_hash: 'd'.repeat(64), normalized: definition }) : Promise.resolve(study))
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/factor-studies/new', component: FactorStudyComposerView }, { path: '/factor-studies/:factorStudyId', component: { template: '<div />' } }, { path: '/factor-studies', component: { template: '<div />' } }] })
    await router.push('/factor-studies/new'); await router.isReady()
    const wrapper = mount(FactorStudyComposerView, { global: { plugins: [[VueQueryPlugin, { queryClient: client() }], router] } })
    await vi.waitFor(() => expect(wrapper.text()).toContain('book_to_price_mrq'))
    const validateButton = wrapper.findAll('button').find((button) => button.text() === '校验')
    const submitButton = wrapper.findAll('button').find((button) => button.text() === '提交')
    if (!validateButton || !submitButton) throw new Error('missing factor study actions')
    await validateButton.trigger('click')
    await vi.waitFor(() => expect(submitButton.attributes('disabled')).toBeUndefined())
    await wrapper.get('input[value="yaml"]').setValue(true); await flushPromises()
    const editor = wrapper.get('textarea[aria-label="因子研究 YAML"]')
    expect((editor.element as HTMLTextAreaElement).value).toContain('factor_ids:')
    await editor.setValue(`${(editor.element as HTMLTextAreaElement).value}\n`)
    expect(submitButton.attributes('disabled')).toBeDefined()
  })

  it('deep-links global selectors, saves matrix decisions and maps IC chart data', async () => {
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/factor-studies/study-1') return Promise.resolve(study)
      if (path === '/api/v1/factor-studies/study-1/matrix') return Promise.resolve({ items: [matrixRow], total: 1 })
      if (path.includes('/artifacts/ic')) return Promise.resolve({ items: [{ signal_date: '2022-01-01', rank_ic: 0.1, rank_ic_rolling_mean: 0.08, pearson_ic: 0.09 }], total: 1 })
      return Promise.resolve({ items: [], total: 0 })
    })
    apiPut.mockResolvedValue(study)
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/factor-studies/:factorStudyId', component: FactorStudyDetailView }, { path: '/factor-studies', component: { template: '<div />' } }] })
    await router.push('/factor-studies/study-1'); await router.isReady()
    const wrapper = mount(FactorStudyDetailView, { global: { plugins: [[VueQueryPlugin, { queryClient: client() }], router] } })
    await vi.waitFor(() => expect(wrapper.text()).toContain('book_to_price_mrq'))
    expect(wrapper.text()).toContain('因子处理')
    expect(wrapper.text()).toContain('方向统一')
    expect(wrapper.text()).toContain('理论远期收益')
    expect(wrapper.text()).toContain('研究指标与曲线')
    expect(wrapper.text()).not.toContain('候选矩阵')
    expect(wrapper.text()).toContain('IC 与分层收益')
    expect(wrapper.text()).toContain('换手与成本')
    expect(wrapper.text()).toContain('质量与相关性')
    expect(wrapper.text()).toContain('Pearson IC 描述统计')
    expect(wrapper.text()).toContain('Rank IC HAC 推断')
    expect(wrapper.text()).toContain('多空收益 HAC 推断')
    expect(wrapper.text()).toContain('其他 Summary 指标')
    expect(wrapper.text()).toContain('1.234e-5')
    expect(wrapper.text()).toContain('7.123457')
    const summaryMetrics = wrapper.findAll('[data-summary-metric]')
    expect(summaryMetrics).toHaveLength(Object.keys(matrixRow.summary_metrics).length)
    expect(new Set(summaryMetrics.map((item) => item.attributes('data-summary-metric')))).toEqual(new Set(Object.keys(matrixRow.summary_metrics)))
    await vi.waitFor(() => expect(router.currentRoute.value.query).toMatchObject({ signal_variant: 'DIRECTION_ADJUSTED', label_kind: 'THEORETICAL_FORWARD_RETURN', factor: 'book_to_price_mrq', horizon: '5' }))
    const candidate = wrapper.findAll('button').find((button) => button.text() === 'Candidate')
    if (!candidate) throw new Error('missing Candidate action')
    await candidate.trigger('click')
    await vi.waitFor(() => expect(apiPut).toHaveBeenCalledWith('/api/v1/factor-studies/study-1/decisions', expect.objectContaining({ mark: 'CANDIDATE', horizon: 5 })))
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith(expect.stringContaining('/artifacts/ic')))
    await flushPromises()
    expect(wrapper.findAll('[data-curve-artifact]')).toHaveLength(10)
    for (const artifactType of ['ic', 'quantile_returns', 'long_short_returns', 'monotonicity', 'turnover', 'cost_scenarios', 'coverage', 'label_quality', 'industry_coverage', 'correlation']) {
      expect(apiGet).toHaveBeenCalledWith(expect.stringContaining(`/artifacts/${artifactType}`))
    }
    const chart = wrapper.findComponent({ name: 'VChart' })
    expect(chart.exists()).toBe(true)
    expect(chart.props('option')).toMatchObject({
      legend: { top: 2, left: 'center' },
      grid: { top: 50, bottom: 56, containLabel: true },
    })
  })

  it('shows the live task substage on a running factor study', async () => {
    const runningStudy = { ...study, status: 'RUNNING', stage: 'ANALYZE_FACTORS', completed_at: null }
    const runningTask = {
      id: 'task-1', subject_kind: 'FACTOR_STUDY', subject_id: 'study-1', task_type: 'FACTOR_STUDY', status: 'RUNNING', priority: 0,
      progress: {
        stage: 'ANALYZE_FACTORS', completed: 2, total: 4, message: '正在构建理论与可执行远期收益标签',
        context: { substage: 'BUILD_FORWARD_RETURNS', substage_state: 'STARTED', last_completed_substage: 'LOAD_LABEL_INPUTS', last_completed_evidence: { bar_row_count: 5000 } },
      },
      created_at: '2026-08-01T00:00:00Z', started_at: '2026-08-01T00:00:01Z', updated_at: '2026-08-01T00:00:02Z', heartbeat_at: '2026-08-01T00:00:02Z', completed_at: null,
      worker_id: 'worker-1', error: null, result: null, payload: { factor_study_id: 'study-1' }, attempts: [],
    }
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/factor-studies/study-1') return Promise.resolve(runningStudy)
      if (path === '/api/v1/tasks/task-1') return Promise.resolve(runningTask)
      return Promise.resolve({ items: [], total: 0 })
    })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/factor-studies/:factorStudyId', component: FactorStudyDetailView }, { path: '/factor-studies', component: { template: '<div />' } }] })
    await router.push('/factor-studies/study-1'); await router.isReady()
    const wrapper = mount(FactorStudyDetailView, { global: { plugins: [[VueQueryPlugin, { queryClient: client() }], router] } })

    await vi.waitFor(() => expect(wrapper.find('.factor-task-progress--detail').exists()).toBe(true))
    const progress = wrapper.find('.factor-task-progress--detail')
    expect(progress.attributes('data-task-substage')).toBe('BUILD_FORWARD_RETURNS')
    expect(progress.text()).toContain('构建远期收益')
    expect(progress.text()).toContain('最近完成：加载标签输入')
    expect(progress.text()).toContain('行情行 5,000')
    wrapper.unmount()
  })
})
