import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import NotebookView from './views/NotebookView.vue'

const apiGet = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({ api: { get: apiGet } }))

async function mountNotebook() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = mount(NotebookView, {
    global: { plugins: [[VueQueryPlugin, { queryClient }]] },
  })
  await flushPromises()
  return { wrapper, queryClient }
}

describe('embedded notebook workspace', () => {
  beforeEach(() => apiGet.mockReset())
  afterEach(() => vi.restoreAllMocks())

  it('mounts JupyterLab only after the local server is ready', async () => {
    apiGet.mockResolvedValue({ status: 'READY' })

    const { wrapper, queryClient } = await mountNotebook()
    const frame = wrapper.get('iframe[title="JupyterLab"]')

    expect(apiGet).toHaveBeenCalledWith('/api/v1/notebook/status')
    expect(frame.attributes('src')).toBe('http://127.0.0.1:8009/lab')
    expect(frame.attributes('allow')).toContain('clipboard-read')
    expect(frame.attributes('allow')).toContain('fullscreen')
    expect(frame.attributes('sandbox')).toBeUndefined()

    wrapper.unmount()
    queryClient.clear()
  })

  it('shows startup guidance and retries before mounting the frame', async () => {
    apiGet
      .mockResolvedValueOnce({ status: 'UNAVAILABLE' })
      .mockResolvedValueOnce({ status: 'READY' })

    const { wrapper, queryClient } = await mountNotebook()

    expect(wrapper.text()).toContain('Notebook 尚未启动')
    expect(wrapper.text()).toContain('uv run qlab start')
    expect(wrapper.find('iframe').exists()).toBe(false)

    await wrapper.get('button').trigger('click')
    await flushPromises()

    expect(apiGet).toHaveBeenCalledTimes(2)
    expect(wrapper.find('iframe[title="JupyterLab"]').exists()).toBe(true)

    wrapper.unmount()
    queryClient.clear()
  })
})
