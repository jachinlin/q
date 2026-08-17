import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { Overview } from './types'
import OverviewView from './views/OverviewView.vue'

const apiGet = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({ api: { get: apiGet } }))

const activeTask = {
  id: 'task-active-0001',
  experiment_id: 'experiment-active',
  factor_run_id: null,
  task_type: 'BACKTEST',
  status: 'RUNNING',
  priority: 0,
  progress: { stage: 'BACKTEST', completed: 3, total: 7 },
  created_at: '2026-08-15T01:00:00Z',
  started_at: '2026-08-15T01:00:02Z',
  updated_at: '2026-08-15T01:03:00Z',
  heartbeat_at: '2026-08-15T01:03:00Z',
  completed_at: null,
  worker_id: 'worker-1',
  error: null,
  result: null,
}

function overviewFixture(blocked = false): Overview {
  return {
    gate: {
      status: blocked ? 'BLOCKED' : 'READY',
      reason: blocked ? 'VALIDATION_FAILED' : 'VALIDATED',
      catalog_hash: 'a'.repeat(64),
      validated_catalog_hash: blocked ? null : 'a'.repeat(64),
      quality_run_id: blocked ? null : 'quality-1',
      updated_at: '2026-08-15T01:00:00Z',
      validated_at: blocked ? null : '2026-08-15T01:00:00Z',
    },
    freshness: {
      status: blocked ? 'MISSING' : 'CURRENT',
      counts: { CURRENT: blocked ? 5 : 8, STALE: blocked ? 2 : 0, MISSING: blocked ? 1 : 0, UNKNOWN: 0 },
      evaluated_at: '2026-08-15T01:00:00Z',
      latest_complete_session: '2026-08-14',
    },
    latest_trade_date: '2026-08-14',
    dataset_count: 8,
    gate_quality_run: null,
    latest_quality_run: blocked
      ? {
          run_id: 'quality-failed', scope: 'ALL', input_hash: 'b'.repeat(64), status: 'FAILED',
          started_at: '2026-08-15T00:00:00Z', completed_at: '2026-08-15T00:01:00Z',
          issue_count: 3, blocking_issue_count: 2,
        }
      : null,
    worker: blocked
      ? { worker_id: 'worker-1', task_id: activeTask.id, task_status: 'RUNNING', heartbeat_at: activeTask.heartbeat_at }
      : null,
    last_successful_update: null,
    tasks: {
      status_counts: {
        QUEUED: 0, RUNNING: blocked ? 1 : 0, SUCCEEDED: 8, FAILED: blocked ? 2 : 0,
        CANCEL_REQUESTED: 0, CANCELLED: 0, ORPHANED: blocked ? 1 : 0,
      },
      active: blocked ? [activeTask] : [],
    },
    experiments: {
      status_counts: { CREATED: 0, QUEUED: 0, RUNNING: 0, SUCCEEDED: 0, FAILED: 0, CANCELLED: 0 },
      recent: [],
      benchmarks: [
        { strategy_id: 'etf_rotation', experiment: null },
        { strategy_id: 'stock_multifactor', experiment: null },
      ],
    },
  }
}

async function mountOverview(payload: Overview) {
  apiGet.mockResolvedValue(payload)
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/', component: OverviewView },
      { path: '/data', component: { template: '<div />' } },
      { path: '/tasks', component: { template: '<div />' } },
      { path: '/experiments', component: { template: '<div />' } },
      { path: '/experiments/:experimentId', component: { template: '<div />' } },
      { path: '/notebook', component: { template: '<div />' } },
    ],
  })
  await router.push('/')
  await router.isReady()
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = mount(OverviewView, {
    global: { plugins: [[VueQueryPlugin, { queryClient }], router] },
  })
  await flushPromises()
  return { wrapper, queryClient }
}

describe('research workbench overview', () => {
  beforeEach(() => apiGet.mockReset())
  afterEach(() => vi.restoreAllMocks())

  it('renders a healthy readiness state and read-only entry points', async () => {
    const { wrapper, queryClient } = await mountOverview(overviewFixture())

    expect(wrapper.text()).toContain('研究环境已就绪')
    expect(wrapper.text()).toContain('当前没有阻断事项')
    expect(wrapper.text()).toContain('当前没有活动任务')
    expect(wrapper.find('a[href="/data"]').exists()).toBe(true)
    expect(wrapper.find('a[href="/experiments"]').exists()).toBe(true)
    const notebook = wrapper.get('a[href="/notebook"]')
    expect(notebook.attributes('target')).toBeUndefined()
    expect(notebook.text()).toBe('打开 Notebook')
    expect(wrapper.find('.chart').exists()).toBe(false)

    wrapper.unmount()
    queryClient.clear()
  })

  it('surfaces blocking evidence and links directly to filtered runtime views', async () => {
    const { wrapper, queryClient } = await mountOverview(overviewFixture(true))

    expect(wrapper.text()).toContain('研究环境需要处理')
    expect(wrapper.text()).toContain('2 个质量阻断项')
    expect(wrapper.text()).toContain('1 个数据集缺失')
    expect(wrapper.text()).toContain('2 个失败任务')
    expect(wrapper.text()).toContain('1 个孤儿任务')
    expect(wrapper.find('a[href="/tasks?status=FAILED"]').exists()).toBe(true)
    expect(wrapper.find('a[href="/tasks?status=ORPHANED"]').exists()).toBe(true)
    expect(wrapper.find(`a[href="/tasks?task=${activeTask.id}"]`).exists()).toBe(true)
    expect(wrapper.text()).toContain('43%')

    wrapper.unmount()
    queryClient.clear()
  })
})
