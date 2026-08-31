<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed } from 'vue'
import { useRoute } from 'vue-router'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import MarkdownDocument from '../components/MarkdownDocument.vue'
import type { StrategyProfile } from '../types'

const route = useRoute()
const strategyId = computed(() => String(route.params.strategyId ?? ''))
const profile = useQuery({
  queryKey: computed(() => ['strategy-profile', strategyId.value]),
  queryFn: () => api.get<StrategyProfile>(`/api/v1/strategies/${encodeURIComponent(strategyId.value)}`),
})
</script>

<template>
  <div class="page-stack">
    <ErrorState v-if="profile.error.value" title="策略说明不可用" :error="profile.error.value instanceof DashboardApiError ? profile.error.value : new Error(String(profile.error.value))" />
    <template v-if="profile.data.value">
      <section class="panel strategy-detail-header">
        <div><span class="eyebrow">{{ profile.data.value.strategy_id }}</span><h2>{{ profile.data.value.display_name }}</h2><p>{{ profile.data.value.summary }}</p></div>
        <div class="toolbar"><RouterLink to="/strategies"><el-button>返回策略库</el-button></RouterLink><RouterLink to="/strategy-studies/new"><el-button type="primary">创建研究</el-button></RouterLink></div>
      </section>
      <section class="panel strategy-document-panel">
        <MarkdownDocument :markdown="profile.data.value.documentation_markdown" />
      </section>
    </template>
  </div>
</template>

<style scoped>
.strategy-detail-header{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;padding:24px}.strategy-detail-header h2{margin:8px 0;font-size:24px}.strategy-detail-header p{max-width:760px;margin:0;color:var(--muted);font-size:12px;line-height:1.7}.strategy-document-panel{max-width:980px;width:100%;padding:26px 34px}@media(max-width:1100px){.strategy-detail-header{flex-direction:column}.strategy-document-panel{max-width:none}}
</style>
