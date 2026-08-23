import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { ElMessageBox } from 'element-plus'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import TasksView from './views/TasksView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiDelete = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  DashboardApiError: class extends Error {
    code = ''
    remediation = null
  },
  api: {
    get: apiGet,
    post: vi.fn(),
    delete: apiDelete,
  },
}))

const failedTask = {
  id: 'task-failed-0001',
  subject_kind: 'EXPERIMENT_RUN',
  subject_id: 'research-run-0001',
  task_type: 'EXPERIMENT_RUN',
  status: 'FAILED',
  priority: 10,
  progress: { stage: 'VALIDATE', completed: 1, total: 7 },
  created_at: '2026-08-15T00:00:00Z',
  started_at: '2026-08-15T00:00:01Z',
  updated_at: '2026-08-15T00:00:03Z',
  heartbeat_at: '2026-08-15T00:00:02Z',
  completed_at: '2026-08-15T00:00:03Z',
  worker_id: 'worker-1',
  error: { code: 'DATA_HASH_DRIFT', retryable: false },
}

const defaultPayload = {
  z_nested: { enabled: true, items: [1, 'two'] },
  api_token: 'direct-task-value',
  start: '2026-08-01',
  note: '<img src=x onerror=alert(1)>',
}

function taskDetail(payload: Record<string, unknown> = defaultPayload, taskType = 'EXPERIMENT_RUN') {
  return {
    ...failedTask,
    task_type: taskType,
    payload,
    attempts: [
      {
        id: 'attempt-2', attempt_no: 2, status: 'FAILED', worker_id: 'worker-1',
        started_at: '2026-08-15T00:00:01Z', heartbeat_at: '2026-08-15T00:00:02Z',
        completed_at: '2026-08-15T00:00:03Z', progress: { stage: 'VALIDATE' },
        error: { code: 'DATA_HASH_DRIFT', retryable: false }, has_log: true,
      },
      {
        id: 'attempt-1', attempt_no: 1, status: 'CANCELLED', worker_id: 'worker-0',
        started_at: '2026-08-14T23:00:01Z', heartbeat_at: null,
        completed_at: '2026-08-14T23:00:03Z', progress: {}, error: null, has_log: false,
      },
    ],
  }
}

function taskLog(available = true) {
  return {
    task_id: failedTask.id,
    attempt_id: 'attempt-2',
    available,
    lines: available ? ['{"event":"task.handler_failed"}'] : [],
    total_lines: available ? 721 : 0,
    truncated: available,
    diagnostic: {
      code: 'DATA_HASH_DRIFT', message: 'validated catalog is stale',
      exception_type: 'quant_research.domain.errors.QuantError', stage: 'VALIDATE', retryable: false,
      remediation: 'run validate-all before retrying', traceback: 'Traceback: catalog is stale',
    },
  }
}

async function mountRuntimeCenter(
  logAvailable = true,
  payload: Record<string, unknown> = defaultPayload,
  taskType = 'EXPERIMENT_RUN',
  initialPath = '/tasks',
) {
  apiGet.mockImplementation((path?: string) => {
    if (path?.startsWith('/api/v1/tasks?page=')) {
      return Promise.resolve({
        items: [{
          ...failedTask,
          task_type: taskType,
        }], page: 1, page_size: 25, total: 1,
        status_counts: { QUEUED: 2, RUNNING: 1, SUCCEEDED: 4, FAILED: 1, CANCEL_REQUESTED: 0, CANCELLED: 0, ORPHANED: 1 },
      })
    }
    if (path === `/api/v1/tasks/${failedTask.id}`) return Promise.resolve(taskDetail(payload, taskType))
    if (path?.includes('/attempts/attempt-2/log')) return Promise.resolve(taskLog(logAvailable))
    if (path === undefined) return Promise.resolve({ items: [], page: 1, page_size: 25, total: 0, status_counts: {} })
    return Promise.reject(new Error(`unexpected API path: ${path}`))
  })
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/tasks', component: TasksView },
      { path: '/experiments/:experimentId', name: 'experiment-detail', component: { template: '<div />' } },
    ],
  })
  await router.push(initialPath)
  await router.isReady()
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = mount(TasksView, {
    attachTo: document.body,
    global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
  })
  await flushPromises()
  return wrapper
}

describe('runtime center', () => {
  beforeEach(() => {
    apiGet.mockReset()
    apiDelete.mockReset()
  })
  afterEach(() => {
    vi.restoreAllMocks()
    document.body.innerHTML = ''
  })

  it('renders task identity and associations in separate columns', async () => {
    const wrapper = await mountRuntimeCenter()
    const headers = wrapper.findAll('.el-table__header-wrapper th')
      .map((item) => item.text().trim())

    expect(headers).toContain('任务')
    expect(headers).toContain('关联')
    expect(headers).not.toContain('任务 / 关联')
    expect(wrapper.find('.task-link').element.closest('td'))
      .not.toBe(wrapper.find('.association-cell').element.closest('td'))
    expect(wrapper.find('.association-cell').text()).toContain('EXPERIMENT_RUN · research')
    wrapper.unmount()
  })

  it('highlights failures and automatically diagnoses the latest failed attempt', async () => {
    const wrapper = await mountRuntimeCenter()
    expect(wrapper.text()).toContain('任务运行与异常诊断')
    expect(wrapper.text()).toContain('DATA_HASH_DRIFT')
    expect(wrapper.find('.runtime-row-failed').exists()).toBe(true)

    await wrapper.find('.task-link').trigger('click')
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith(
      `/api/v1/tasks/${failedTask.id}/attempts/attempt-2/log?tail_lines=500`,
    ))
    await flushPromises()

    expect(document.body.textContent).toContain('validated catalog is stale')
    expect(document.body.textContent).toContain('run validate-all before retrying')
    const traceback = document.body.querySelector('.traceback-details')
    expect(traceback).not.toBeNull()
    expect(traceback?.hasAttribute('open')).toBe(false)
    expect(document.body.textContent).toContain('当前仅显示尾部 500 行')
    expect(document.body.textContent).toContain('重试')
    expect(document.body.textContent).not.toContain('取消')
    expect(document.body.textContent).toContain('任务参数')
    expect(document.body.textContent).toContain('direct-task-value')
    expect(document.body.textContent).toContain('<img src=x onerror=alert(1)>')
    expect(document.body.querySelector('.parameter-panel img')).toBeNull()
    const parameterKeys = Array.from(document.body.querySelectorAll('.parameter-item dt'))
      .map((item) => item.textContent)
    expect(parameterKeys).toEqual(['api_token', 'note', 'start', 'z_nested'])
    const fullJson = document.body.querySelector('.parameter-json')
    expect(fullJson).not.toBeNull()
    expect(fullJson?.hasAttribute('open')).toBe(false)
    wrapper.unmount()
  })

  it('distinguishes a registered but missing log file', async () => {
    const wrapper = await mountRuntimeCenter(false)
    await wrapper.find('.task-link').trigger('click')
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith(
      `/api/v1/tasks/${failedTask.id}/attempts/attempt-2/log?tail_lines=500`,
    ))
    await flushPromises()
    const logTab = Array.from(document.body.querySelectorAll('[role="tab"]'))
      .find((item) => item.textContent?.includes('尝试与日志')) as HTMLElement
    logTab.click()
    await flushPromises()
    expect(document.body.textContent).toContain('日志已登记，但文件当前不存在或不可用')
    wrapper.unmount()
  })

  it('shows an explicit empty state when the task has no parameters', async () => {
    const wrapper = await mountRuntimeCenter(true, {})
    await wrapper.find('.task-link').trigger('click')
    await flushPromises()
    expect(document.body.textContent).toContain('该任务没有显式参数。')
    expect(document.body.querySelector('.parameter-json')).toBeNull()
    wrapper.unmount()
  })

  it('renders frozen data update windows as business parameters', async () => {
    const payload = {
      window_mode: 'AUTO_INCREMENTAL', planned_at: '2026-08-15T01:00:00Z',
      start: '2026-08-10', end: '2026-11-18', plan_hash: 'a'.repeat(64),
      dataset_windows: [
        { dataset: 'daily_bar', basis: 'INCREMENTAL', start: '2026-08-10', end: '2026-08-14', overlap_days: 4, current_watermark: '2026-08-13' },
        { dataset: 'instrument', basis: 'SNAPSHOT_REFRESH', start: '2026-08-15', end: '2026-08-15', overlap_days: 0 },
        { dataset: 'trade_calendar', basis: 'INCREMENTAL', start: '2026-07-21', end: '2026-11-18', overlap_days: 30, current_watermark: '2026-11-18' },
      ],
      skipped_datasets: [],
    }
    const wrapper = await mountRuntimeCenter(true, payload, 'DATA_UPDATE')
    await wrapper.find('.task-link').trigger('click')
    await flushPromises()
    expect(document.body.textContent).toContain('更新模式')
    expect(document.body.textContent).toContain('自动增量')
    expect(document.body.textContent).toContain('daily_bar')
    expect(document.body.textContent).toContain('增量水位')
    expect(document.body.textContent).toContain('2026-08-10 至 2026-08-14')
    expect(document.body.textContent).toContain('全量快照')
    expect(document.body.textContent).toContain('快照日期 2026-08-15')
    expect(document.body.textContent).toContain('不适用')
    expect(document.body.textContent).toContain('覆盖至 2026-11-18')
    expect(document.body.textContent).toContain('修订回看 30 天')
    expect(document.body.textContent).toContain('抓取 2026-07-21 至 2026-11-18')
    wrapper.unmount()
  })

  it('marks legacy dynamic data update payloads without inventing windows', async () => {
    const wrapper = await mountRuntimeCenter(true, { start: null, end: null }, 'DATA_UPDATE')
    await wrapper.find('.task-link').trigger('click')
    await flushPromises()
    expect(document.body.textContent).toContain('旧版动态自动窗口')
    expect(document.body.textContent).toContain('请从数据中心创建新任务')
    wrapper.unmount()
  })

  it('shows the generic research subject in task detail', async () => {
    const wrapper = await mountRuntimeCenter(true, { run_id: 'research-run-0001' })
    await wrapper.find('.task-link').trigger('click')
    await flushPromises()
    expect(document.body.textContent).toContain('EXPERIMENT_RUN · research-run-0001')
    wrapper.unmount()
  })

  it('deletes only after explicit confirmation', async () => {
    apiDelete.mockResolvedValue({ task_id: failedTask.id, status: 'DELETED' })
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue({} as never)
    const wrapper = await mountRuntimeCenter()
    const deleteButton = wrapper.findAll('button').find((item) => item.text() === '删除')
    expect(deleteButton).toBeDefined()
    await deleteButton?.trigger('click')
    await vi.waitFor(() => expect(apiDelete).toHaveBeenCalledWith(`/api/v1/tasks/${failedTask.id}`))
    wrapper.unmount()
  })

  it('initializes the status filter from an overview deep link', async () => {
    const wrapper = await mountRuntimeCenter(true, defaultPayload, 'BACKTEST', '/tasks?status=FAILED')
    await vi.waitFor(() => expect(apiGet).toHaveBeenCalledWith('/api/v1/tasks?page=1&page_size=25&status=FAILED'))
    expect(wrapper.find('.runtime-stat.active').text()).toContain('失败')
    wrapper.unmount()
  })
})
