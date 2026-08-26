import { QueryClient, VueQueryPlugin } from '@tanstack/vue-query'
import { flushPromises, mount } from '@vue/test-utils'
import { ElMessageBox } from 'element-plus'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import SettingsView from './views/SettingsView.vue'

const apiGet = vi.hoisted(() => vi.fn())
const apiPatch = vi.hoisted(() => vi.fn())

vi.mock('./api', () => ({
  DashboardApiError: class extends Error {},
  api: { get: apiGet, patch: apiPatch },
}))

describe('dashboard settings', () => {
  beforeEach(() => {
    apiGet.mockReset()
    apiPatch.mockReset()
    apiGet.mockResolvedValue({
      settings_path: 'C:\\Users\\tester\\qlab-data\\.env',
      data_source_token: {
        configured: true,
        source: 'DATA_ROOT_ENV',
        updated_at: '2026-08-26T12:00:00Z',
      },
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
    document.body.innerHTML = ''
  })

  it('shows only token status and clears the password field after saving', async () => {
    apiPatch.mockResolvedValue({
      settings_path: 'C:\\Users\\tester\\qlab-data\\.env',
      data_source_token: {
        configured: true,
        source: 'DATA_ROOT_ENV',
        updated_at: '2026-08-26T12:01:00Z',
      },
    })
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const wrapper = mount(SettingsView, {
      global: { plugins: [[VueQueryPlugin, { queryClient }]] },
      attachTo: document.body,
    })
    await flushPromises()

    expect(wrapper.text()).toContain('数据根 .env')
    expect(wrapper.text()).toContain('明文写入')
    expect(wrapper.text()).toContain('C:\\Users\\tester\\qlab-data\\.env')
    expect(wrapper.text()).not.toContain('existing-token')

    const input = wrapper.get('[data-testid="data-source-token"]')
    await input.setValue('new-sensitive-token')
    await wrapper.get('form').trigger('submit')
    await flushPromises()

    expect(apiPatch).toHaveBeenCalledWith('/api/v1/settings', {
      data_source_token: { operation: 'SET', value: 'new-sensitive-token' },
    })
    expect((input.element as HTMLInputElement).value).toBe('')
    expect(wrapper.text()).not.toContain('new-sensitive-token')
    wrapper.unmount()
  })

  it('clears the dashboard-managed token after explicit confirmation', async () => {
    apiPatch.mockResolvedValue({
      settings_path: 'C:\\Users\\tester\\qlab-data\\.env',
      data_source_token: {
        configured: true,
        source: 'PROCESS_ENVIRONMENT',
        updated_at: null,
      },
    })
    vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue({} as never)
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const wrapper = mount(SettingsView, {
      global: { plugins: [[VueQueryPlugin, { queryClient }]] },
      attachTo: document.body,
    })
    await flushPromises()

    const clear = wrapper.findAll('button')
      .find((item) => item.text().includes('清除 Dashboard Token'))
    await clear?.trigger('click')
    await flushPromises()

    expect(ElMessageBox.confirm).toHaveBeenCalled()
    expect(apiPatch).toHaveBeenCalledWith('/api/v1/settings', {
      data_source_token: { operation: 'CLEAR' },
    })
    expect(wrapper.text()).toContain('进程环境变量')
    wrapper.unmount()
  })
})
