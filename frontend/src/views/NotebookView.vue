<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed } from 'vue'

import { api } from '../api'
import type { NotebookStatus } from '../types'

const notebookUrl = 'http://127.0.0.1:8009/lab'
const query = useQuery({
  queryKey: ['notebook-status'],
  queryFn: () => api.get<NotebookStatus>('/api/v1/notebook/status'),
  refetchInterval: 2_000,
})
const ready = computed(() => query.data.value?.status === 'READY')
</script>

<template>
  <div class="notebook-page">
    <iframe
      v-if="ready"
      class="notebook-frame"
      :src="notebookUrl"
      title="JupyterLab"
      allow="clipboard-read; clipboard-write; fullscreen"
      referrerpolicy="same-origin"
    />
    <section v-else class="notebook-state" aria-live="polite">
      <span class="notebook-state-mark" aria-hidden="true">NB</span>
      <template v-if="query.isLoading.value">
        <strong>正在连接 Notebook</strong>
        <p>等待本机 JupyterLab 完成启动。</p>
      </template>
      <template v-else-if="query.isError.value">
        <strong>无法检查 Notebook 状态</strong>
        <p>Dashboard 状态接口暂时不可用，请稍后重试。</p>
      </template>
      <template v-else>
        <strong>Notebook 尚未启动</strong>
        <p>请使用 <code>uv run qlab start</code> 同时启动 Dashboard、Worker 和 JupyterLab。</p>
      </template>
      <el-button
        v-if="!query.isLoading.value"
        type="primary"
        plain
        :loading="query.isFetching.value"
        @click="query.refetch()"
      >
        重新检查
      </el-button>
    </section>
  </div>
</template>
