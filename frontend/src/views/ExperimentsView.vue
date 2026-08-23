<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed } from 'vue'

import { api } from '../api'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime } from '../format'
import type { ExperimentOverview } from '../types'

const experiments = useQuery({
  queryKey: ['experiments'],
  queryFn: () => api.get<{ items: ExperimentOverview[] }>('/api/v1/experiments?limit=200&offset=0'),
  refetchInterval: 4000,
})
const counts = computed(() => {
  const items = experiments.data.value?.items ?? []
  return {
    total: items.length,
    strategy: items.filter((item) => item.definition.kind === 'STRATEGY_BACKTEST').length,
    factor: items.filter((item) => item.definition.kind === 'FACTOR_STUDY').length,
    baseline: items.filter((item) => item.baseline_run_id).length,
  }
})
</script>

<template>
  <div class="page-stack">
    <section class="panel experiment-hero">
      <div><span class="eyebrow">EXPERIMENT → RUN</span><h2>统一策略回测与因子研究</h2><p>实验定义研究问题；每次参数变化或重试创建独立 Run，历史产物不会被覆盖。</p></div>
      <RouterLink to="/experiments/new"><el-button type="primary" size="large">创建实验</el-button></RouterLink>
    </section>
    <section class="metrics-grid">
      <article class="metric-card"><span class="metric-top">实验总数<i /></span><strong class="metric-value">{{ counts.total }}</strong><p>不可变研究定义</p></article>
      <article class="metric-card tone-cyan"><span class="metric-top">策略回测<i /></span><strong class="metric-value">{{ counts.strategy }}</strong><p>订单驱动 T+1</p></article>
      <article class="metric-card tone-green"><span class="metric-top">因子研究<i /></span><strong class="metric-value">{{ counts.factor }}</strong><p>IC、分层与显著性</p></article>
      <article class="metric-card"><span class="metric-top">基线<i /></span><strong class="metric-value">{{ counts.baseline }}</strong><p>精确指向 Run</p></article>
    </section>
    <ErrorState v-if="experiments.error.value" :error="experiments.error.value" />
    <section v-else class="panel table-panel">
      <div class="panel-heading"><div><h2>实验</h2><p>策略回测和因子研究共享任务、指标与产物生命周期。</p></div></div>
      <el-table v-loading="experiments.isLoading.value" :data="experiments.data.value?.items ?? []" empty-text="尚无实验">
        <el-table-column label="实验" min-width="280"><template #default="scope"><RouterLink class="experiment-link" :to="`/experiments/${scope.row.id}`"><strong>{{ scope.row.definition.name }}</strong><small>{{ scope.row.definition.description || '—' }}</small></RouterLink></template></el-table-column>
        <el-table-column label="类型" width="150"><template #default="scope">{{ scope.row.definition.kind === 'STRATEGY_BACKTEST' ? '策略回测' : '因子研究' }}</template></el-table-column>
        <el-table-column label="Run / TEST" width="120"><template #default="scope">{{ scope.row.run_count }} / {{ scope.row.test_uses }}</template></el-table-column>
        <el-table-column label="基线" width="130"><template #default="scope"><span class="hash">{{ scope.row.baseline_run_id?.slice(0, 10) ?? '—' }}</span></template></el-table-column>
        <el-table-column label="状态" width="110"><template #default="scope"><StatusBadge :status="scope.row.latest_run?.status ?? 'CREATED'" /></template></el-table-column>
        <el-table-column label="创建时间" width="170"><template #default="scope">{{ formatTime(scope.row.created_at) }}</template></el-table-column>
      </el-table>
    </section>
  </div>
</template>

<style scoped>
.experiment-hero{display:flex;align-items:center;justify-content:space-between;gap:24px;padding:26px;background:linear-gradient(120deg,#fff,#f1f6ff 60%,#eef9f7)}
.experiment-hero h2{margin:9px 0 7px;font-size:25px}.experiment-hero p{margin:0;color:var(--muted)}
.experiment-link{display:flex;flex-direction:column;gap:4px;color:var(--text);text-decoration:none}.experiment-link small{color:var(--dim)}
</style>
