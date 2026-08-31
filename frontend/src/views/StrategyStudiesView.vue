<script setup lang="ts">
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { ElMessage, ElMessageBox } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/message-box/style/css'
import { computed } from 'vue'

import { api } from '../api'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime } from '../format'
import type { StrategyStudy } from '../types'

const client = useQueryClient()
const studies = useQuery({
  queryKey: ['strategy-studies'],
  queryFn: () => api.get<{ items: StrategyStudy[] }>('/api/v1/strategy-studies?limit=200&offset=0'),
  refetchInterval: 4_000,
})
const counts = computed(() => {
  const items = studies.data.value?.items ?? []
  return {
    total: items.length,
    active: items.filter((item) => ['QUEUED', 'RUNNING'].includes(item.status)).length,
    succeeded: items.filter((item) => item.status === 'SUCCEEDED').length,
    failed: items.filter((item) => ['FAILED', 'CANCELLED'].includes(item.status)).length,
  }
})
const remove = useMutation({
  mutationFn: (id: string) => api.delete<{ strategy_study_id: string; status: 'DELETED' }>(`/api/v1/strategy-studies/${id}`),
  onSuccess: async (result) => {
    ElMessage.success('策略研究已删除')
    client.removeQueries({ queryKey: ['strategy-study', result.strategy_study_id] })
    await client.invalidateQueries({ queryKey: ['strategy-studies'] })
  },
})

async function deleteStudy(study: StrategyStudy) {
  try {
    await ElMessageBox.confirm(
      `将删除策略研究“${study.definition.name}”及其产物。任务和审计历史仍会保留，此操作不可撤销。`,
      '确认删除策略研究',
      { type: 'error', confirmButtonText: '删除', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  remove.mutate(study.id)
}
</script>

<template>
  <div class="page-stack">
    <section class="panel study-hero">
      <div><span class="eyebrow">ONE SUBMISSION · ONE RESULT</span><h2>策略研究</h2><p>每次提交只执行一次；需要调整参数时，复制原定义并创建一项独立研究。</p></div>
      <div class="study-hero-actions">
        <RouterLink to="/strategies"><el-button size="large">策略库</el-button></RouterLink>
        <RouterLink to="/strategy-studies/new"><el-button type="primary" size="large">创建策略研究</el-button></RouterLink>
      </div>
    </section>
    <section class="metrics-grid">
      <article class="metric-card"><span class="metric-top">研究总数<i /></span><strong class="metric-value">{{ counts.total }}</strong><p>独立冻结配置</p></article>
      <article class="metric-card tone-cyan"><span class="metric-top">执行中<i /></span><strong class="metric-value">{{ counts.active }}</strong><p>排队或四阶段执行</p></article>
      <article class="metric-card tone-green"><span class="metric-top">已成功<i /></span><strong class="metric-value">{{ counts.succeeded }}</strong><p>Manifest 已验证</p></article>
      <article class="metric-card"><span class="metric-top">未完成<i /></span><strong class="metric-value">{{ counts.failed }}</strong><p>失败或取消</p></article>
    </section>
    <ErrorState v-if="studies.error.value" :error="studies.error.value" />
    <section v-else class="panel table-panel">
      <div class="panel-heading"><div><h2>全部研究</h2><p>VALIDATE → BACKTEST → ANALYTICS → PUBLISH</p></div></div>
      <el-table v-loading="studies.isLoading.value" :data="studies.data.value?.items ?? []" empty-text="尚无策略研究">
        <el-table-column label="研究" min-width="280"><template #default="scope"><RouterLink class="study-link" :to="`/strategy-studies/${scope.row.id}`"><strong>{{ scope.row.definition.name }}</strong><small>{{ scope.row.definition.description || '—' }}</small></RouterLink></template></el-table-column>
        <el-table-column prop="definition.strategy.strategy_id" label="策略" min-width="160" />
        <el-table-column label="区间" width="210"><template #default="scope">{{ scope.row.definition.start_date }} 至 {{ scope.row.definition.end_date }}</template></el-table-column>
        <el-table-column label="状态" width="110"><template #default="scope"><StatusBadge :status="scope.row.status" /></template></el-table-column>
        <el-table-column prop="stage" label="阶段" width="120" />
        <el-table-column label="创建时间" width="170"><template #default="scope">{{ formatTime(scope.row.created_at) }}</template></el-table-column>
        <el-table-column label="操作" width="100"><template #default="scope"><el-button text type="danger" :disabled="!['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(scope.row.status)" @click="deleteStudy(scope.row)">删除</el-button></template></el-table-column>
      </el-table>
    </section>
  </div>
</template>

<style scoped>
.study-hero{display:flex;align-items:center;justify-content:space-between;gap:24px;padding:26px;background:linear-gradient(120deg,#fff,#f1f6ff 60%,#eef9f7)}
.study-hero h2{margin:9px 0 7px;font-size:25px}.study-hero p{margin:0;color:var(--muted)}
.study-hero-actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.study-link{display:flex;flex-direction:column;gap:4px;color:var(--text);text-decoration:none}.study-link small{color:var(--dim)}
</style>
