<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed, ref, watch } from 'vue'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime } from '../format'
import type { DashboardSettings } from '../types'

const client = useQueryClient()
const token = ref('')
const requestsPerMinute = ref(480)
const proxyUrl = ref('')

const settings = useQuery({
  queryKey: ['settings'],
  queryFn: () => api.get<DashboardSettings>('/api/v1/settings'),
})

const sourceLabel = computed(() => {
  const source = settings.data.value?.data_source_token.source
  if (source === 'DATA_ROOT_ENV') return '数据根 .env'
  if (source === 'PROCESS_ENVIRONMENT') return '进程环境变量'
  return '未配置'
})

const rateLimitSourceLabel = computed(() => {
  const source = settings.data.value?.data_source_rate_limit.source
  if (source === 'DATA_ROOT_ENV') return '数据根 .env'
  if (source === 'PROCESS_ENVIRONMENT') return '进程环境变量'
  return '内置默认值'
})

const proxySourceLabel = computed(() => {
  const source = settings.data.value?.data_source_proxy.source
  if (source === 'DATA_ROOT_ENV') return '数据根 .env'
  if (source === 'PROCESS_ENVIRONMENT') return '进程环境变量'
  return '未配置（Tushare 官方入口）'
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
    const fallback = result.data_source_token.source === 'PROCESS_ENVIRONMENT'
      ? '，已回退到进程环境变量'
      : ''
    ElMessage.success(`Dashboard Token 已清除${fallback}`)
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
    const fallback = result.data_source_rate_limit.source === 'PROCESS_ENVIRONMENT'
      ? '进程环境变量'
      : '内置默认值'
    ElMessage.success(`Dashboard 限流设置已清除，已回退到${fallback}`)
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
    const fallback = result.data_source_proxy.source === 'PROCESS_ENVIRONMENT'
      ? '进程环境变量'
      : 'Tushare 官方入口'
    ElMessage.success(`Dashboard 代理设置已清除，已回退到${fallback}`)
  },
  onError: showError,
})

async function confirmClear() {
  await ElMessageBox.confirm(
    '将从数据根 .env 删除 Dashboard 管理的 Token。若进程环境变量仍有 Token，它会重新生效。',
    '确认清除数据源 Token',
    { type: 'warning', confirmButtonText: '确认清除' },
  )
  clearToken.mutate()
}

async function confirmClearRateLimit() {
  await ElMessageBox.confirm(
    '将从数据根 .env 删除 Dashboard 管理的请求限流。清除后会回退到进程环境变量或内置默认值 480 次/分钟。',
    '确认清除请求限流',
    { type: 'warning', confirmButtonText: '确认清除' },
  )
  clearRateLimit.mutate()
}

async function confirmClearProxy() {
  await ElMessageBox.confirm(
    '将从数据根 .env 删除 Dashboard 管理的代理地址。清除后会回退到进程环境变量或 Tushare 官方入口。',
    '确认清除 Tushare 代理',
    { type: 'warning', confirmButtonText: '确认清除' },
  )
  clearProxy.mutate()
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
      <section class="panel settings-panel">
        <header class="panel-heading">
          <div>
            <h2>数据源</h2>
            <p>Tushare 全市场数据采集凭据。</p>
          </div>
          <StatusBadge :status="settings.data.value?.data_source_token.configured ? 'READY' : 'MISSING'" />
        </header>

        <div class="settings-status-grid">
          <div><small>当前状态</small><strong>{{ settings.data.value?.data_source_token.configured ? '已配置' : '未配置' }}</strong></div>
          <div><small>配置来源</small><strong>{{ sourceLabel }}</strong></div>
          <div><small>文件更新时间</small><strong>{{ formatTime(settings.data.value?.data_source_token.updated_at) }}</strong></div>
        </div>

        <el-alert
          title="Token 将以明文写入数据根 .env。本机可读取该文件的进程和备份工具都可能获得 Token。"
          type="warning"
          :closable="false"
          show-icon
        />

        <el-form class="token-form" label-position="top" @submit.prevent="saveToken.mutate()">
          <el-form-item label="新的数据源 Token">
            <el-input
              v-model="token"
              data-testid="data-source-token"
              type="password"
              show-password
              autocomplete="new-password"
              placeholder="输入 Token；已保存的值不会回显"
            />
          </el-form-item>
          <div class="settings-actions">
            <el-button
              type="primary"
              native-type="submit"
              :loading="saveToken.isPending.value"
              :disabled="!token"
            >保存并立即生效</el-button>
            <el-button
              type="danger"
              plain
              :loading="clearToken.isPending.value"
              :disabled="settings.data.value?.data_source_token.source !== 'DATA_ROOT_ENV'"
              @click="confirmClear"
            >清除 Dashboard Token</el-button>
          </div>
        </el-form>

        <div class="settings-path">
          <small>设置文件</small>
          <code>{{ settings.data.value?.settings_path ?? '—' }}</code>
        </div>

        <el-divider />

        <div class="rate-limit-heading">
          <h3>请求限流</h3>
          <p>同一进程内的 Tushare 请求均匀发送；不同进程分别计算额度。</p>
        </div>
        <div class="settings-status-grid">
          <div><small>每分钟请求数</small><strong>{{ settings.data.value?.data_source_rate_limit.requests_per_minute ?? 480 }}</strong></div>
          <div><small>配置来源</small><strong>{{ rateLimitSourceLabel }}</strong></div>
          <div><small>文件更新时间</small><strong>{{ formatTime(settings.data.value?.data_source_rate_limit.updated_at) }}</strong></div>
        </div>
        <el-alert
          title="默认 480 次/分钟。重试请求同样计入额度；多个 Worker 或 CLI 进程不会共享计数。"
          type="info"
          :closable="false"
          show-icon
        />
        <el-form class="token-form" label-position="top" @submit.prevent="saveRateLimit.mutate()">
          <el-form-item label="每分钟最大请求数">
            <el-input-number
              v-model="requestsPerMinute"
              data-testid="data-source-rate-limit"
              :min="1"
              :max="10000"
              controls-position="right"
              style="width:100%"
            />
          </el-form-item>
          <div class="settings-actions">
            <el-button
              type="primary"
              native-type="submit"
              :loading="saveRateLimit.isPending.value"
            >保存并立即生效</el-button>
            <el-button
              type="danger"
              plain
              :loading="clearRateLimit.isPending.value"
              :disabled="settings.data.value?.data_source_rate_limit.source !== 'DATA_ROOT_ENV'"
              @click="confirmClearRateLimit"
            >清除 Dashboard 限流设置</el-button>
          </div>
        </el-form>

        <el-divider />

        <div class="rate-limit-heading">
          <h3>Tushare 代理</h3>
          <p>代理地址会写入 Tushare Pro 对象，并在下一次请求前检测变更。</p>
        </div>
        <div class="settings-status-grid proxy-status-grid">
          <div><small>当前代理</small><strong>{{ settings.data.value?.data_source_proxy.url ?? '官方入口' }}</strong></div>
          <div><small>配置来源</small><strong>{{ proxySourceLabel }}</strong></div>
          <div><small>文件更新时间</small><strong>{{ formatTime(settings.data.value?.data_source_proxy.updated_at) }}</strong></div>
        </div>
        <el-form class="token-form" label-position="top" @submit.prevent="saveProxy.mutate()">
          <el-form-item label="代理 URL">
            <el-input
              v-model="proxyUrl"
              data-testid="data-source-proxy"
              autocomplete="url"
              placeholder="例如 https://proxy.example.com/"
            />
          </el-form-item>
          <div class="settings-actions">
            <el-button
              type="primary"
              native-type="submit"
              :loading="saveProxy.isPending.value"
              :disabled="!proxyUrl"
            >保存并立即生效</el-button>
            <el-button
              type="danger"
              plain
              :loading="clearProxy.isPending.value"
              :disabled="settings.data.value?.data_source_proxy.source !== 'DATA_ROOT_ENV'"
              @click="confirmClearProxy"
            >清除 Dashboard 代理</el-button>
          </div>
        </el-form>
      </section>
    </template>
  </div>
</template>

<style scoped>
.settings-panel {
  max-width: 820px;
}

.settings-status-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 18px;
}

.settings-status-grid div,
.settings-path {
  display: grid;
  gap: 6px;
  padding: 14px;
  border: 1px solid var(--border);
  border-radius: 10px;
  background: var(--surface-raised);
}

.settings-status-grid small,
.settings-path small {
  color: var(--muted);
}

.token-form {
  margin-top: 20px;
}

.settings-actions {
  display: flex;
  gap: 10px;
}

.settings-path {
  margin-top: 20px;
}

.settings-path code {
  overflow-wrap: anywhere;
}

.rate-limit-heading {
  margin: 22px 0 14px;
}

.rate-limit-heading h3,
.rate-limit-heading p {
  margin: 0;
}

.proxy-status-grid strong {
  overflow-wrap: anywhere;
}

.rate-limit-heading p {
  margin-top: 6px;
  color: var(--muted);
  font-size: 12px;
}

@media (max-width: 760px) {
  .settings-status-grid {
    grid-template-columns: 1fr;
  }
}
</style>
