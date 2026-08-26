<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { ref, watch } from 'vue'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import type { DashboardSettings } from '../types'

const client = useQueryClient()
const token = ref('')
const requestsPerMinute = ref(480)
const proxyUrl = ref('')
const maxConcurrentRequests = ref(4)

const settings = useQuery({
  queryKey: ['settings'],
  queryFn: () => api.get<DashboardSettings>('/api/v1/settings'),
})

watch(
  () => settings.data.value?.data_source_rate_limit.requests_per_minute,
  value => {
    if (value !== undefined) requestsPerMinute.value = value
  },
  { immediate: true },
)

watch(
  () => settings.data.value?.data_source_proxy.url,
  value => { proxyUrl.value = value ?? '' },
  { immediate: true },
)

watch(
  () => settings.data.value?.data_source_concurrency.max_concurrent_requests,
  value => {
    if (value !== undefined) maxConcurrentRequests.value = value
  },
  { immediate: true },
)

function showError(error: unknown) {
  ElMessage.error(error instanceof DashboardApiError
    ? (error.remediation ?? error.message)
    : String(error))
}

const saveToken = useMutation({
  mutationFn: () => api.patch<DashboardSettings>('/api/v1/settings', {
    data_source_token: { operation: 'SET', value: token.value },
  }),
  onSuccess: async (result) => {
    token.value = ''
    client.setQueryData(['settings'], result)
    ElMessage.success('数据源 Token 已保存并立即生效')
  },
  onError: showError,
})

const clearToken = useMutation({
  mutationFn: () => api.patch<DashboardSettings>('/api/v1/settings', {
    data_source_token: { operation: 'CLEAR' },
  }),
  onSuccess: async (result) => {
    token.value = ''
    client.setQueryData(['settings'], result)
    ElMessage.success('数据源 Token 已清除')
  },
  onError: showError,
})

const saveRateLimit = useMutation({
  mutationFn: () => api.patch<DashboardSettings>('/api/v1/settings', {
    data_source_rate_limit: {
      operation: 'SET',
      requests_per_minute: requestsPerMinute.value,
    },
  }),
  onSuccess: result => {
    client.setQueryData(['settings'], result)
    ElMessage.success('Tushare 请求限流已保存并立即生效')
  },
  onError: showError,
})

const clearRateLimit = useMutation({
  mutationFn: () => api.patch<DashboardSettings>('/api/v1/settings', {
    data_source_rate_limit: { operation: 'CLEAR' },
  }),
  onSuccess: result => {
    client.setQueryData(['settings'], result)
    ElMessage.success('Tushare 请求限流已清除')
  },
  onError: showError,
})

const saveProxy = useMutation({
  mutationFn: () => api.patch<DashboardSettings>('/api/v1/settings', {
    data_source_proxy: { operation: 'SET', url: proxyUrl.value },
  }),
  onSuccess: result => {
    client.setQueryData(['settings'], result)
    ElMessage.success('Tushare 代理地址已保存并立即生效')
  },
  onError: showError,
})

const clearProxy = useMutation({
  mutationFn: () => api.patch<DashboardSettings>('/api/v1/settings', {
    data_source_proxy: { operation: 'CLEAR' },
  }),
  onSuccess: result => {
    client.setQueryData(['settings'], result)
    ElMessage.success('Tushare 代理地址已清除')
  },
  onError: showError,
})

const saveConcurrency = useMutation({
  mutationFn: () => api.patch<DashboardSettings>('/api/v1/settings', {
    data_source_concurrency: {
      operation: 'SET',
      max_concurrent_requests: maxConcurrentRequests.value,
    },
  }),
  onSuccess: result => {
    client.setQueryData(['settings'], result)
    ElMessage.success('LOCALIZE 并发数已保存，将在下一个数据集请求批次生效')
  },
  onError: showError,
})

const clearConcurrency = useMutation({
  mutationFn: () => api.patch<DashboardSettings>('/api/v1/settings', {
    data_source_concurrency: { operation: 'CLEAR' },
  }),
  onSuccess: result => {
    client.setQueryData(['settings'], result)
    ElMessage.success('LOCALIZE 并发设置已清除')
  },
  onError: showError,
})

async function confirmClear() {
  await ElMessageBox.confirm(
    '将清除当前 Token 设置，是否继续？',
    '确认清除数据源 Token',
    { type: 'warning', confirmButtonText: '确认清除' },
  )
  clearToken.mutate()
}

async function confirmClearRateLimit() {
  await ElMessageBox.confirm(
    '将清除当前请求限流设置，是否继续？',
    '确认清除请求限流',
    { type: 'warning', confirmButtonText: '确认清除' },
  )
  clearRateLimit.mutate()
}

async function confirmClearProxy() {
  await ElMessageBox.confirm(
    '将清除当前代理设置，是否继续？',
    '确认清除 Tushare 代理',
    { type: 'warning', confirmButtonText: '确认清除' },
  )
  clearProxy.mutate()
}

async function confirmClearConcurrency() {
  await ElMessageBox.confirm(
    '将清除当前采集并发设置，是否继续？',
    '确认清除 LOCALIZE 并发设置',
    { type: 'warning', confirmButtonText: '确认清除' },
  )
  clearConcurrency.mutate()
}
</script>

<template>
  <div class="page-stack">
    <ErrorState
      v-if="settings.isError.value"
      :error="settings.error.value"
      message="无法读取 Dashboard 设置。"
    />
    <template v-else>
      <section class="settings-page">
        <el-alert
          class="settings-warning"
          :title="`Token 明文保存在 ${settings.data.value?.settings_path ?? '设置文件'}；已保存的值不会在页面回显。`"
          type="warning"
          :closable="false"
          show-icon
        />

        <div class="panel settings-list">
          <div class="setting-row">
            <div class="setting-label">
              <h3>访问 Token</h3>
              <p>{{ settings.data.value?.data_source_token.configured ? '已配置' : '未配置' }}</p>
            </div>
            <el-form class="inline-form" @submit.prevent="saveToken.mutate()">
              <el-input
                v-model="token"
                data-testid="data-source-token"
                type="password"
                show-password
                autocomplete="new-password"
                aria-label="新 Token"
                placeholder="输入后覆盖现有 Token"
              />
              <el-button type="primary" native-type="submit" :loading="saveToken.isPending.value" :disabled="!token">保存</el-button>
              <el-button
                data-testid="clear-data-source-token"
                type="danger"
                text
                :loading="clearToken.isPending.value"
                :disabled="settings.data.value?.data_source_token.source !== 'DATA_ROOT_ENV'"
                @click="confirmClear"
              >清除</el-button>
            </el-form>
          </div>

          <div class="setting-row">
            <div class="setting-label">
              <h3>请求限流</h3>
              <p>每分钟请求数</p>
            </div>
            <el-form class="inline-form" @submit.prevent="saveRateLimit.mutate()">
              <el-input-number
                v-model="requestsPerMinute"
                data-testid="data-source-rate-limit"
                aria-label="每分钟请求数"
                :min="1"
                :max="10000"
                controls-position="right"
              />
              <el-button type="primary" native-type="submit" :loading="saveRateLimit.isPending.value">保存</el-button>
              <el-button
                data-testid="clear-data-source-rate-limit"
                type="danger"
                text
                :loading="clearRateLimit.isPending.value"
                :disabled="settings.data.value?.data_source_rate_limit.source !== 'DATA_ROOT_ENV'"
                @click="confirmClearRateLimit"
              >清除</el-button>
            </el-form>
          </div>

          <div class="setting-row">
            <div class="setting-label">
              <h3>Tushare 代理</h3>
              <p>留空使用官方入口</p>
            </div>
            <el-form class="inline-form" @submit.prevent="saveProxy.mutate()">
              <el-input
                v-model="proxyUrl"
                data-testid="data-source-proxy"
                autocomplete="url"
                aria-label="代理 URL"
                placeholder="留空使用官方入口"
              />
              <el-button type="primary" native-type="submit" :loading="saveProxy.isPending.value" :disabled="!proxyUrl">保存</el-button>
              <el-button
                data-testid="clear-data-source-proxy"
                type="danger"
                text
                :loading="clearProxy.isPending.value"
                :disabled="settings.data.value?.data_source_proxy.source !== 'DATA_ROOT_ENV'"
                @click="confirmClearProxy"
              >清除</el-button>
            </el-form>
          </div>

          <div class="setting-row">
            <div class="setting-label">
              <h3>采集并发</h3>
              <p>1–32 路</p>
            </div>
            <el-form class="inline-form" @submit.prevent="saveConcurrency.mutate()">
              <el-input-number
                v-model="maxConcurrentRequests"
                data-testid="data-source-concurrency"
                aria-label="最大并发请求数"
                :min="1"
                :max="32"
                controls-position="right"
              />
              <el-button type="primary" native-type="submit" :loading="saveConcurrency.isPending.value">保存</el-button>
              <el-button
                data-testid="clear-data-source-concurrency"
                type="danger"
                text
                :loading="clearConcurrency.isPending.value"
                :disabled="settings.data.value?.data_source_concurrency.source !== 'DATA_ROOT_ENV'"
                @click="confirmClearConcurrency"
              >清除</el-button>
            </el-form>
          </div>
        </div>
      </section>
    </template>
  </div>
</template>

<style scoped>
.settings-page {
  width: min(100%, 980px);
}

.setting-label h3,
.setting-label p {
  margin: 0;
}

.setting-label p {
  color: var(--muted);
}

.settings-warning {
  margin-top: 0;
}

.settings-list {
  padding: 0 20px;
  margin-top: 14px;
}

.setting-row {
  display: grid;
  grid-template-columns: 180px minmax(0, 1fr);
  gap: 24px;
  align-items: center;
  padding: 17px 0;
}

.setting-row + .setting-row {
  border-top: 1px solid var(--border);
}

.setting-label h3 {
  font-size: 14px;
}

.setting-label p {
  margin-top: 4px;
  font-size: 11px;
}

.inline-form {
  display: grid;
  grid-template-columns: minmax(220px, 1fr) auto auto;
  gap: 6px;
  align-items: center;
  min-width: 0;
}

.inline-form :deep(.el-input-number) {
  width: 100%;
}

@media (max-width: 820px) {
  .setting-row {
    grid-template-columns: 1fr;
    gap: 10px;
  }
}

@media (max-width: 560px) {
  .settings-list {
    padding: 0 16px;
  }
}
</style>
