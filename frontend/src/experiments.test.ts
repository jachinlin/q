import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { ElMessageBox } from 'element-plus'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import ExperimentsView from './views/ExperimentsView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiDelete = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  api: { get: apiGet, delete: apiDelete },
}))

const experiment = {
  id: 'experiment-1',
  definition: { name: '可删除实验', description: 'test', kind: 'FACTOR_STUDY' },
  baseline_run_id: null,
  created_at: '2026-08-23T00:00:00Z',
  latest_run: { status: 'FAILED' },
  run_count: 2,
  test_uses: 0,
  has_active_runs: false,
}

async function mountList() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/experiments', component: ExperimentsView }, { path: '/experiments/:experimentId', component: { template: '<div />' } }, { path: '/experiments/new', component: { template: '<div />' } }] })
  await router.push('/experiments')
  await router.isReady()
  return mount(ExperimentsView, { global: { plugins: [[VueQueryPlugin, { queryClient }], router] } })
}

describe('experiment list deletion', () => {
  beforeEach(() => {
    apiGet.mockReset()
    apiDelete.mockReset()
  })

  it('confirms deletion of an experiment without active Runs', async () => {
    apiGet.mockResolvedValue({ items: [experiment] })
    apiDelete.mockResolvedValue({ experiment_id: experiment.id, run_count: 2, status: 'DELETED' })
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue({} as never)
    const wrapper = await mountList()
    await vi.waitFor(() => expect(wrapper.text()).toContain('可删除实验'))

    const remove = wrapper.findAll('button').find((button) => button.text() === '删除')
    if (!remove) throw new Error('missing experiment delete button')
    await remove.trigger('click')
    await flushPromises()

    expect(apiDelete).toHaveBeenCalledWith(`/api/v1/experiments/${experiment.id}`)
  })
})
