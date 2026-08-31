<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'

import { api, DashboardApiError } from '../api'
import ErrorState from '../components/ErrorState.vue'
import type { StrategyCatalog } from '../types'

const catalog = useQuery({
  queryKey: ['strategies'],
  queryFn: () => api.get<StrategyCatalog>('/api/v1/strategies'),
})
</script>

<template>
  <div class="page-stack">
    <section class="panel strategy-library-header">
      <div><span class="eyebrow">STRATEGY LIBRARY</span><h2>策略库</h2><p>浏览可执行策略的结构、数据依赖、组合约束与适用边界。</p></div>
      <RouterLink to="/strategy-studies/new"><el-button type="primary">新建策略研究</el-button></RouterLink>
    </section>
    <ErrorState v-if="catalog.error.value" :error="catalog.error.value instanceof DashboardApiError ? catalog.error.value : new Error(String(catalog.error.value))" />
    <section v-else class="strategy-card-grid" aria-label="策略目录">
      <RouterLink v-for="item in catalog.data.value?.strategies ?? []" :key="item.strategy_id" class="strategy-card" :to="`/strategies/${item.strategy_id}`">
        <span class="strategy-card-id">{{ item.strategy_id }}</span>
        <h2>{{ item.display_name }}</h2>
        <p>{{ item.summary }}</p>
        <strong>查看说明 <span>›</span></strong>
      </RouterLink>
    </section>
  </div>
</template>

<style scoped>
.strategy-library-header{display:flex;align-items:center;justify-content:space-between;gap:24px;padding:24px}.strategy-library-header h2{margin:8px 0 7px;font-size:22px}.strategy-library-header p{margin:0;color:var(--muted);font-size:12px}.strategy-card-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}.strategy-card{min-height:220px;display:flex;flex-direction:column;padding:22px;border:1px solid var(--border);border-radius:12px;color:var(--text);background:var(--surface);text-decoration:none;transition:transform 150ms ease,border-color 150ms ease,box-shadow 150ms ease}.strategy-card:hover{transform:translateY(-2px);border-color:rgba(37,99,235,.35);box-shadow:0 10px 24px rgba(45,66,99,.07)}.strategy-card-id{color:var(--cyan);font:10px ui-monospace,Consolas,monospace;letter-spacing:.08em}.strategy-card h2{margin:17px 0 10px;font-size:19px}.strategy-card p{margin:0;color:var(--muted);font-size:12px;line-height:1.7}.strategy-card strong{margin-top:auto;color:var(--blue);font-size:11px}.strategy-card strong span{font-size:18px}@media(max-width:1200px){.strategy-card-grid{grid-template-columns:1fr}.strategy-library-header{align-items:flex-start;flex-direction:column}}
</style>
