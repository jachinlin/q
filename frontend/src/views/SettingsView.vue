<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed, ref } from 'vue'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime } from '../format'
import type { DashboardSettings } from '../types'

const client = useQueryClient()
const token = ref('')

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

async function confirmClear() {
  await ElMessageBox.confirm(
    '将从数据根 .env 删除 Dashboard 管理的 Token。若进程环境变量仍有 Token，它会重新生效。',
    '确认清除数据源 Token',
    { type: 'warning', confirmButtonText: '确认清除' },
  )
  clearToken.mutate()
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

@media (max-width: 760px) {
  .settings-status-grid {
    grid-template-columns: 1fr;
  }
}
</style>
