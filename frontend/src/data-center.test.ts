import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { ElMessageBox } from 'element-plus'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DashboardApiError } from './api'
import DataCenterView from './views/DataCenterView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiPost = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  DashboardApiError: class extends Error {
    code: string
    remediation: string | null

    constructor(payload: { code: string; message: string; remediation: string | null }) {
      super(payload.message)
      this.code = payload.code
      this.remediation = payload.remediation
    }
  },
  api: { get: apiGet, post: apiPost },
}))

describe('data center core loop', () => {
  const dataset = {
    dataset: 'daily_bar', source: 'baostock', start_date: '2026-01-01', end_date: '2026-08-13',
    partition_count: 2, row_count: 100, content_hash: 'a'.repeat(64), updated_at: '2026-08-14T10:00:00Z',
    partitioning: 'year', cadence: 'daily', fetch_granularity: 'trading_day', reuse: 'append_only', overlap_days: 0,
    freshness: { status: 'STALE', actual_watermark: '2026-08-13', expected_watermark: '2026-08-14', lag_days: 1, evaluated_at: '2026-08-15T10:00:00Z', reason: 'watermark is behind target' },
    operational: { last_localized_at: '2026-08-14T10:00:00Z', localized_through: '2026-08-14', last_curated_at: '2026-08-14T10:01:00Z', last_validated_at: '2026-08-14T10:02:00Z' },
    quality_issue_count: 1, blocking_issue_count: 0,
  }
  const calendarDataset = {
    ...dataset,
    dataset: 'trade_calendar',
    start_date: null,
    end_date: null,
    partitioning: 'all',
    freshness: { ...dataset.freshness, status: 'CURRENT' },
  }

  beforeEach(() => {
    apiGet.mockReset()
    apiPost.mockReset()
    apiGet.mockImplementation((path: string) => {
      if (path === '/api/v1/data/summary') return Promise.resolve({
        gate: { status: 'READY', reason: 'VALIDATED', catalog_hash: 'a'.repeat(64), validated_catalog_hash: 'a'.repeat(64), quality_run_id: 'run-1', updated_at: '2026-08-14T10:00:00Z', validated_at: '2026-08-14T10:02:00Z' },
        freshness: { status: 'STALE', counts: { CURRENT: 7, STALE: 1, MISSING: 0, UNKNOWN: 0 }, evaluated_at: '2026-08-15T10:00:00Z', latest_complete_session: '2026-08-14' },
        gate_quality_run: null, latest_quality_run: null, active_update: null, last_successful_update: null, worker: null, active_research_task_count: 2,
      })
      if (path === '/api/v1/data/datasets') return Promise.resolve({ items: [dataset, calendarDataset] })
      if (path.startsWith('/api/v1/data/quality-runs?')) return Promise.resolve({ items: [{ run_id: 'run-1', scope: 'all', input_hash: 'a'.repeat(64), status: 'PASSED', started_at: '2026-08-14T10:00:00Z', completed_at: '2026-08-14T10:02:00Z', issue_count: 1, blocking_issue_count: 0 }], page: 1, page_size: 50, total: 1 })
      if (path.startsWith('/api/v1/tasks/')) return Promise.resolve({
        id: path.split('/').at(-1), task_type: 'DATA_VALIDATION', status: 'SUCCEEDED',
        progress: { stage: 'COMPLETE', completed: 1, total: 1, percent: 100, message: 'data validation completed' },
      })
      if (path === '/api/v1/data/quality-runs/run-1') return Promise.resolve({
        run_id: 'run-1', scope: 'ALL', input_hash: 'a'.repeat(64), status: 'FAILED',
        started_at: '2026-08-14T10:00:00Z', completed_at: '2026-08-14T10:02:00Z',
        issue_count: 1, blocking_issue_count: 1, dataset_hashes: { daily_bar: 'b'.repeat(64) },
        results_complete: false, result_counts: { PASS: 1, FAIL: 1, SKIPPED: 1, UNKNOWN: 1 },
        rule_results: [
          { rule_id: 'canonical_schema', dataset: 'daily_bar', status: 'PASS', severity: 'FATAL', title: 'Canonical Schema 一致', description: '逐分区核对 Schema。', pass_criterion: '不匹配分区数为 0。', scope: {}, actual: 0, threshold: 0, skip_reason: null, evidence: 'RUN_SNAPSHOT', issues: [] },
          { rule_id: 'primary_key_duplicate', dataset: 'daily_bar', status: 'FAIL', severity: 'FATAL', title: '主键唯一', description: '检查重复主键。', pass_criterion: '重复主键数为 0。', scope: { partition: 'year=2026' }, actual: 2, threshold: 0, skip_reason: null, evidence: 'LEGACY_ISSUE', issues: [{ rule_id: 'primary_key_duplicate', severity: 'FATAL', dataset: 'daily_bar', scope: {}, actual: 2, threshold: 0, message: '发现重复主键', remediation: '重新清洗分区' }] },
          { rule_id: 'trading_day_coverage', dataset: 'daily_bar', status: 'SKIPPED', severity: 'SEVERE', title: '交易日覆盖完整', description: '检查交易日覆盖。', pass_criterion: '缺失交易日数为 0。', scope: {}, actual: 0, threshold: 0, skip_reason: '缺少交易日历', evidence: 'RUN_SNAPSHOT', issues: [] },
          { rule_id: 'negative_volume', dataset: 'daily_bar', status: 'UNKNOWN', severity: 'SEVERE', title: '成交量非负', description: '检查负成交量。', pass_criterion: '负成交量数为 0。', scope: {}, actual: null, threshold: null, skip_reason: '未保存执行证据', evidence: 'MISSING', issues: [] },
        ],
        issues: [],
      })
      if (path === '/api/v1/data/datasets/daily_bar') return Promise.resolve({ ...dataset, contract: { partitioning: 'year', fetch_granularity: 'trading_day', cadence: 'daily', reuse: 'append_only', overlap_days: 0, primary_key: ['trade_date', 'instrument_id'], sort_key: ['trade_date'], pit_fields: [], schema: [{ name: 'trade_date', type: 'Date' }], sources: [{ source: 'baostock', endpoints: ['query_history_k_data_plus'] }] }, partitions: [] })
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
    document.body.innerHTML = ''
  })

  const updatePlan = {
    window_mode: 'AUTO_INCREMENTAL', planned_at: '2026-08-15T10:00:00Z',
    start: '2026-08-10', end: '2026-11-12', plan_hash: 'b'.repeat(64),
    dataset_windows: [
      { dataset: 'daily_bar', basis: 'INCREMENTAL', start: '2026-08-10', end: '2026-08-14', overlap_days: 4, current_watermark: '2026-08-13' },
      { dataset: 'trade_calendar', basis: 'INCREMENTAL', start: '2026-08-13', end: '2026-11-12', overlap_days: 1, current_watermark: '2026-11-10' },
    ],
  }

  it('shows validated and stale independently and opens the dataset contract', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div />' } }, { path: '/tasks', component: { template: '<div />' } }] })
    await router.push('/')
    await router.isReady()
    const wrapper = mount(DataCenterView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] }, attachTo: document.body })
    await flushPromises()

    expect(wrapper.text()).toContain('VALIDATED')
    expect(wrapper.text()).toContain('STALE')
    expect(wrapper.text()).toContain('质量运行历史')
    const assetTable = wrapper.find('[data-testid="data-assets-table"]')
    expect(assetTable.text()).toContain('开始日期')
    expect(assetTable.text()).toContain('结束日期')
    const assetRows = assetTable.findAll('.el-table__body-wrapper tbody tr')
    const dailyCells = assetRows[0].findAll('td')
    const calendarCells = assetRows[1].findAll('td')
    expect(dailyCells[1].text()).toBe('2026-01-01')
    expect(dailyCells[2].text()).toBe('2026-08-13')
    expect(calendarCells[1].text()).toBe('—')
    expect(calendarCells[2].text()).toBe('—')
    expect(apiGet).not.toHaveBeenCalledWith('/api/v1/data/catalog')
    await wrapper.find('.el-table__row').trigger('click')
    await flushPromises()
    expect(apiGet).toHaveBeenCalledWith('/api/v1/data/datasets/daily_bar')
    expect(document.body.textContent).toContain('Schema')
    wrapper.unmount()
  })

  it('shows rule descriptions and every quality result status in run detail', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div />' } }, { path: '/tasks', component: { template: '<div />' } }] })
    await router.push('/')
    await router.isReady()
    const wrapper = mount(DataCenterView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] }, attachTo: document.body })
    await flushPromises()

    await wrapper.find('[data-testid="quality-runs-table"] .el-table__row').trigger('click')
    await flushPromises()
    expect(apiGet).toHaveBeenCalledWith('/api/v1/data/quality-runs/run-1')
    expect(document.body.textContent).toContain('缺少完整规则执行证据')
    expect(document.body.textContent).toContain('Canonical Schema 一致')
    expect(document.body.textContent).toContain('逐分区核对 Schema。')
    expect(document.body.textContent).toContain('重复主键数为 0。')
    for (const status of ['PASS', 'FAIL', 'SKIPPED', 'UNKNOWN']) {
      expect(document.body.textContent).toContain(status)
    }

    const resultTable = wrapper.find('[data-testid="quality-rule-results"]')
    const resultRows = resultTable.findAll('.el-table__body-wrapper tbody > tr')
    expect(resultRows[0]?.findAll('td')[2]?.text()).toBe('—')
    expect(resultRows[1]?.findAll('td')[2]?.text()).toBe('FATAL')
    expect(resultRows[2]?.findAll('td')[2]?.text()).toBe('—')
    const expandIcons = resultTable.findAll('.el-table__expand-icon')
    await expandIcons[1]?.trigger('click')
    await flushPromises()
    expect(document.body.textContent).toContain('重新清洗分区')

    const statusFilter = wrapper.findAllComponents({ name: 'ElSelect' })
      .find((item) => item.attributes('data-testid') === 'quality-result-status')
    statusFilter?.vm.$emit('update:modelValue', 'FAIL')
    await flushPromises()
    const filteredResults = wrapper.find('[data-testid="quality-rule-results"]')
    expect(filteredResults.text()).toContain('主键唯一')
    expect(filteredResults.text()).not.toContain('成交量非负')
    wrapper.unmount()
  })

  it('previews dataset windows before submitting the frozen update plan', async () => {
    apiPost.mockImplementation((path: string, body: Record<string, unknown>) => {
      if (path === '/api/v1/data/update-plans/preview') return Promise.resolve(updatePlan)
      if (path === '/api/v1/data/updates') {
        expect(body).toEqual({
          datasets: ['daily_bar', 'trade_calendar'],
          plan_hash: updatePlan.plan_hash,
        })
        return Promise.resolve({ task_id: 'task-plan-1', status: 'QUEUED', plan_hash: updatePlan.plan_hash })
      }
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue({} as never)
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div />' } }, { path: '/tasks', component: { template: '<div />' } }] })
    await router.push('/')
    await router.isReady()
    const wrapper = mount(DataCenterView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] }, attachTo: document.body })
    await flushPromises()

    const create = wrapper.findAll('button').find((item) => item.text() === '创建更新任务')
    await create?.trigger('click')
    await flushPromises()
    expect(apiPost).toHaveBeenCalledWith('/api/v1/data/update-plans/preview', {
      datasets: ['daily_bar', 'trade_calendar'],
    })
    expect(document.body.textContent).toContain('自动增量')
    expect(document.body.textContent).toContain('2026-08-10 至 2026-11-12')
    expect(document.body.textContent).toContain('2')

    const submit = Array.from(document.body.querySelectorAll('button'))
      .find((item) => item.textContent?.includes('确认并提交计划')) as HTMLButtonElement
    submit.click()
    await flushPromises()
    await vi.waitFor(() => expect(apiPost).toHaveBeenCalledWith(
      '/api/v1/data/updates', {
        datasets: ['daily_bar', 'trade_calendar'],
        plan_hash: updatePlan.plan_hash,
      },
    ))
    wrapper.unmount()
  })

  it('creates an all-dataset quality run from the action beside update', async () => {
    apiPost.mockImplementation((path: string, body: Record<string, unknown>) => {
      if (path === '/api/v1/data/quality-runs') {
        expect(body).toEqual({})
        return Promise.resolve({
          task_id: 'quality-task-1', request_id: 'request-1', status: 'QUEUED', scope: 'ALL',
        })
      }
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div />' } }, { path: '/tasks', component: { template: '<div />' } }] })
    await router.push('/')
    await router.isReady()
    const wrapper = mount(DataCenterView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] }, attachTo: document.body })
    await flushPromises()

    const actions = wrapper.find('.data-actions').findAll('button')
    expect(actions.map((item) => item.text())).toEqual(['创建更新任务', '质量运行'])
    await actions[1]?.trigger('click')
    await flushPromises()
    const selector = wrapper.findAllComponents({ name: 'ElSelect' })
      .find((item) => item.attributes('data-testid') === 'quality-run-dataset')
    expect(selector?.props('modelValue')).toBe('ALL')
    expect(document.body.textContent).toContain('全目录质量运行通过后会重新绑定研究门')

    const submit = Array.from(document.body.querySelectorAll('button'))
      .find((item) => item.textContent?.trim() === '确认并创建') as HTMLButtonElement
    submit.click()
    await flushPromises()
    await vi.waitFor(() => expect(apiPost).toHaveBeenCalledWith('/api/v1/data/quality-runs', {}))
    expect(wrapper.text()).toContain('质量运行任务 quality-')
    await vi.waitFor(() => expect(apiGet.mock.calls.filter(
      ([path]) => String(path).startsWith('/api/v1/data/quality-runs?'),
    ).length).toBeGreaterThan(1))
    wrapper.unmount()
  })

  it('creates a diagnostic quality run for one selected dataset', async () => {
    apiPost.mockResolvedValue({
      task_id: 'quality-task-2', request_id: 'request-2', status: 'QUEUED',
      scope: 'DATASET', dataset: 'daily_bar',
    })
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div />' } }, { path: '/tasks', component: { template: '<div />' } }] })
    await router.push('/')
    await router.isReady()
    const wrapper = mount(DataCenterView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] }, attachTo: document.body })
    await flushPromises()

    await wrapper.findAll('button').find((item) => item.text() === '质量运行')?.trigger('click')
    await flushPromises()
    const selector = wrapper.findAllComponents({ name: 'ElSelect' })
      .find((item) => item.attributes('data-testid') === 'quality-run-dataset')
    selector?.vm.$emit('update:modelValue', 'daily_bar')
    await flushPromises()
    expect(document.body.textContent).toContain('单数据集运行仅用于诊断')

    const submit = Array.from(document.body.querySelectorAll('button'))
      .find((item) => item.textContent?.trim() === '确认并创建') as HTMLButtonElement
    submit.click()
    await flushPromises()
    await vi.waitFor(() => expect(apiPost).toHaveBeenCalledWith(
      '/api/v1/data/quality-runs', { dataset: 'daily_bar' },
    ))
    wrapper.unmount()
  })

  it('previews an exact dataset subset and blocks an empty selection', async () => {
    apiPost.mockImplementation((path: string, body: Record<string, unknown>) => {
      if (path !== '/api/v1/data/update-plans/preview') {
        return Promise.reject(new Error(`unexpected API path: ${path}`))
      }
      const selected = body.datasets as string[]
      return Promise.resolve({
        ...updatePlan,
        dataset_windows: updatePlan.dataset_windows.filter((item) => selected.includes(item.dataset)),
      })
    })
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div />' } }, { path: '/tasks', component: { template: '<div />' } }] })
    await router.push('/')
    await router.isReady()
    const wrapper = mount(DataCenterView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] }, attachTo: document.body })
    await flushPromises()

    await wrapper.findAll('button').find((item) => item.text() === '创建更新任务')?.trigger('click')
    await flushPromises()
    const selector = wrapper.findAllComponents({ name: 'ElSelect' })
      .find((item) => item.attributes('data-testid') === 'update-dataset-select')
    selector?.vm.$emit('update:modelValue', ['daily_bar'])
    await flushPromises()
    await vi.waitFor(() => expect(apiPost).toHaveBeenCalledWith(
      '/api/v1/data/update-plans/preview', { datasets: ['daily_bar'] },
    ))
    expect(document.body.textContent).toContain('1 / 2')

    await Array.from(document.body.querySelectorAll('button'))
      .find((item) => item.textContent?.trim() === '清空')?.click()
    await flushPromises()
    expect(document.body.textContent).toContain('请至少选择一个数据集。')
    const submit = Array.from(document.body.querySelectorAll('button'))
      .find((item) => item.textContent?.includes('确认并提交计划')) as HTMLButtonElement
    expect(submit.disabled).toBe(true)
    wrapper.unmount()
  })

  it('keeps the selected subset when refreshing a stale plan', async () => {
    const previewBodies: Record<string, unknown>[] = []
    apiPost.mockImplementation((path: string, body: Record<string, unknown>) => {
      if (path === '/api/v1/data/update-plans/preview') {
        previewBodies.push(body)
        const selected = body.datasets as string[]
        return Promise.resolve({
          ...updatePlan,
          dataset_windows: updatePlan.dataset_windows.filter((item) => selected.includes(item.dataset)),
        })
      }
      if (path === '/api/v1/data/updates') {
        return Promise.reject(new DashboardApiError({
          code: 'DATA_UPDATE_PLAN_STALE',
          message: 'plan changed',
          severity: 'WARNING',
          retryable: true,
          remediation: 'refresh preview',
          request_id: 'request-1',
        }))
      }
      return Promise.reject(new Error(`unexpected API path: ${path}`))
    })
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue({} as never)
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div />' } }, { path: '/tasks', component: { template: '<div />' } }] })
    await router.push('/')
    await router.isReady()
    const wrapper = mount(DataCenterView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] }, attachTo: document.body })
    await flushPromises()

    await wrapper.findAll('button').find((item) => item.text() === '创建更新任务')?.trigger('click')
    await flushPromises()
    const selector = wrapper.findAllComponents({ name: 'ElSelect' })
      .find((item) => item.attributes('data-testid') === 'update-dataset-select')
    selector?.vm.$emit('update:modelValue', ['daily_bar'])
    await flushPromises()
    const submit = Array.from(document.body.querySelectorAll('button'))
      .find((item) => item.textContent?.includes('确认并提交计划')) as HTMLButtonElement
    submit.click()
    await flushPromises()

    await vi.waitFor(() => expect(previewBodies.filter(
      (body) => JSON.stringify(body.datasets) === JSON.stringify(['daily_bar']),
    )).toHaveLength(2))
    expect(apiPost).toHaveBeenCalledWith('/api/v1/data/updates', {
      datasets: ['daily_bar'],
      plan_hash: updatePlan.plan_hash,
    })
    wrapper.unmount()
  })
})
