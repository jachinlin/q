<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed } from 'vue'

import { api } from '../api'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime } from '../format'
import type { Page, ResearchFamily } from '../types'

const families = useQuery({
  queryKey: ['research-families'],
  queryFn: () => api.get<Page<ResearchFamily>>('/api/v1/research/families?page=1&page_size=100'),
  refetchInterval: 4000,
})

const stats = computed(() => {
  const items = families.data.value?.items ?? []
  return {
    total: items.length,
    running: items.filter((item) => ['QUEUED', 'RUNNING'].includes(item.latest_execution?.status ?? '')).length,
    succeeded: items.filter((item) => item.latest_execution?.status === 'SUCCEEDED').length,
    selected: items.filter((item) => Boolean(item.latest_execution?.selected_variant_id)).length,
  }
})

function strategyLabel(value: string) {
  return ({ stock_multifactor: '股票多因子', dual_ma_trend: '双均线趋势', etf_rotation: 'ETF 轮动' } as Record<string, string>)[value] ?? value
}

function modeLabel(value: string) {
  return ({ SIGNAL_STUDY: '信号研究', PORTFOLIO_STUDY: '组合研究', BACKTEST_EXPERIMENT: '回测实验' } as Record<string, string>)[value] ?? value
}
</script>

<template>
  <div class="page-stack research-center">
    <section class="research-hero">
      <div>
        <span class="eyebrow">RESEARCH FAMILIES</span>
        <h2>从研究协议到锁定 TEST 的可复现实验</h2>
        <p>统一组织信号、组合与回测研究。每个候选只使用 VALIDATION 选型，锁定后才运行一次 TEST。</p>
      </div>
      <RouterLink to="/research/new"><el-button type="primary" size="large">创建研究族</el-button></RouterLink>
    </section>

    <section class="metrics-grid research-metrics">
      <article class="metric-card"><span class="metric-top">研究族总数<i /></span><strong class="metric-value">{{ stats.total }}</strong><p>不可变研究定义</p></article>
      <article class="metric-card tone-cyan"><span class="metric-top">活动执行<i /></span><strong class="metric-value">{{ stats.running }}</strong><p>候选展开与运行中</p></article>
      <article class="metric-card tone-green"><span class="metric-top">成功执行<i /></span><strong class="metric-value">{{ stats.succeeded }}</strong><p>已通过产物复核</p></article>
      <article class="metric-card"><span class="metric-top">已锁定候选<i /></span><strong class="metric-value">{{ stats.selected }}</strong><p>已进入独立 TEST</p></article>
    </section>

    <ErrorState v-if="families.error.value" :error="families.error.value" />
    <section v-else class="panel table-panel">
      <div class="panel-heading">
        <div><h2>研究族</h2><p>同一研究定义的重跑会创建新的 execution，不覆盖旧产物。</p></div>
      </div>
      <el-table v-loading="families.isLoading.value" :data="families.data.value?.items ?? []" empty-text="尚无研究族">
        <el-table-column label="研究" min-width="250">
          <template #default="scope">
            <RouterLink class="research-link" :to="`/research/${scope.row.id}`">
              <strong>{{ scope.row.name }}</strong><small>{{ scope.row.hypothesis }}</small>
            </RouterLink>
          </template>
        </el-table-column>
        <el-table-column label="策略" width="130"><template #default="scope">{{ strategyLabel(scope.row.strategy_id) }}</template></el-table-column>
        <el-table-column label="模式" width="120"><template #default="scope">{{ modeLabel(scope.row.research_mode) }}</template></el-table-column>
        <el-table-column label="状态" width="115"><template #default="scope"><StatusBadge :status="scope.row.latest_execution?.status ?? 'CREATED'" /></template></el-table-column>
        <el-table-column label="选中候选" width="120"><template #default="scope"><span class="hash">{{ scope.row.latest_execution?.selected_variant_id?.slice(0, 8) ?? '—' }}</span></template></el-table-column>
        <el-table-column label="标记" width="105" prop="mark" />
        <el-table-column label="创建时间" width="155"><template #default="scope">{{ formatTime(scope.row.created_at) }}</template></el-table-column>
      </el-table>
    </section>
  </div>
</template>

<style scoped>
.research-hero { min-height: 165px; display: flex; align-items: center; justify-content: space-between; gap: 30px; padding: 26px; border: 1px solid rgba(37,99,235,.22); border-radius: 14px; background: linear-gradient(120deg,#fff,#f1f6ff 60%,#eef9f7); }
.research-hero h2 { margin: 10px 0 8px; font-size: 25px; letter-spacing: -.035em; }
.research-hero p { max-width: 760px; margin: 0; color: var(--muted); font-size: 12px; line-height: 1.7; }
.research-hero a { flex: 0 0 auto; }
.research-link { display: flex; flex-direction: column; gap: 4px; color: var(--text); text-decoration: none; }
.research-link strong { font-size: 12px; }
.research-link small { overflow: hidden; color: var(--dim); text-overflow: ellipsis; white-space: nowrap; }
</style>
