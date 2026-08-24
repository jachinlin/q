<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query'
import { computed, ref } from 'vue'

import { api } from '../api'
import ErrorState from '../components/ErrorState.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { formatTime } from '../format'
import type { FactorDecisionMark, FactorStudyOverview, FactorStudyStatus } from '../types'

const status = ref<FactorStudyStatus | ''>('')
const decision = ref<FactorDecisionMark | ''>('')
const queryPath = computed(() => {
  const params = new URLSearchParams({ limit: '200', offset: '0' })
  if (status.value) params.set('status', status.value)
  if (decision.value) params.set('decision', decision.value)
  return `/api/v1/factor-studies?${params}`
})
const studies = useQuery({
  queryKey: computed(() => ['factor-studies', status.value, decision.value]),
  queryFn: () => api.get<{ items: FactorStudyOverview[] }>(queryPath.value),
  refetchInterval: 4000,
})
const counts = computed(() => {
  const items = studies.data.value?.items ?? []
  return {
    total: items.length,
    running: items.filter((item) => ['QUEUED', 'RUNNING'].includes(item.status)).length,
    unreviewed: items.reduce((sum, item) => sum + item.unreviewed_count, 0),
    candidate: items.reduce((sum, item) => sum + item.candidate_count, 0),
  }
})
</script>

<template>
  <div class="page-stack factor-workbench">
    <section class="panel workbench-hero">
      <div><span class="eyebrow">FACTOR EVIDENCE → HUMAN DECISION</span><h2>独立因子研究工作台</h2><p>每项研究冻结一次配置和数据身份；统计证据与 Candidate / Discarded 人工结论分开记录。</p></div>
      <RouterLink to="/factor-studies/new"><el-button type="primary" size="large">新建因子研究</el-button></RouterLink>
    </section>
    <section class="metrics-grid">
      <article class="metric-card"><span class="metric-top">研究总数<i /></span><strong class="metric-value">{{ counts.total }}</strong><p>不可变 FactorStudy</p></article>
      <article class="metric-card tone-cyan"><span class="metric-top">运行中<i /></span><strong class="metric-value">{{ counts.running }}</strong><p>排队与固定阶段执行</p></article>
      <article class="metric-card"><span class="metric-top">待评审矩阵行<i /></span><strong class="metric-value">{{ counts.unreviewed }}</strong><p>尚无人工结论</p></article>
      <article class="metric-card tone-green"><span class="metric-top">Candidate<i /></span><strong class="metric-value">{{ counts.candidate }}</strong><p>人工候选结论</p></article>
    </section>
    <ErrorState v-if="studies.error.value" :error="studies.error.value" />
    <section v-else class="panel table-panel">
      <div class="panel-heading filter-heading">
        <div><h2>研究列表</h2><p>不生成跨研究排行榜；只呈现每项研究自身的证据与评审进度。</p></div>
        <div class="toolbar" aria-label="因子研究筛选">
          <el-select v-model="status" clearable placeholder="全部状态" aria-label="状态筛选">
            <el-option v-for="item in ['QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED']" :key="item" :label="item" :value="item" />
          </el-select>
          <el-select v-model="decision" clearable placeholder="全部结论" aria-label="人工结论筛选">
            <el-option label="待评审" value="UNREVIEWED" /><el-option label="Candidate" value="CANDIDATE" /><el-option label="Discarded" value="DISCARDED" />
          </el-select>
        </div>
      </div>
      <el-table v-loading="studies.isLoading.value" :data="studies.data.value?.items ?? []" empty-text="尚无因子研究">
        <el-table-column label="研究" min-width="250"><template #default="scope"><RouterLink class="study-link" :to="`/factor-studies/${scope.row.id}`"><strong>{{ scope.row.definition.name }}</strong><small>{{ scope.row.definition.start_date }} → {{ scope.row.definition.end_date }}</small></RouterLink></template></el-table-column>
        <el-table-column label="规格" min-width="185"><template #default="scope"><span>{{ scope.row.definition.factor_ids.length }} 因子 · {{ scope.row.definition.horizons.join('/') }}D</span><small class="cell-note">{{ scope.row.definition.correction }} · Q{{ scope.row.definition.quantiles }}</small></template></el-table-column>
        <el-table-column label="执行" min-width="150"><template #default="scope"><StatusBadge :status="scope.row.status" /><small class="cell-note">{{ scope.row.stage }}</small></template></el-table-column>
        <el-table-column label="评审" min-width="195"><template #default="scope"><span class="review-count"><b class="candidate">{{ scope.row.candidate_count }}</b> / <b class="discarded">{{ scope.row.discarded_count }}</b> / {{ scope.row.unreviewed_count }}</span><small class="cell-note">候选 / 丢弃 / 待评审</small></template></el-table-column>
        <el-table-column label="数据身份" min-width="135"><template #default="scope"><span class="hash">{{ scope.row.catalog_hash.slice(0,12) }}</span></template></el-table-column>
        <el-table-column label="创建时间" width="165"><template #default="scope">{{ formatTime(scope.row.created_at) }}</template></el-table-column>
      </el-table>
      <div v-if="!studies.isLoading.value && !(studies.data.value?.items.length)" class="explicit-state"><strong>{{ status === 'RUNNING' ? '当前没有运行中的研究' : decision === 'UNREVIEWED' ? '当前没有待评审数据' : '从一份严格配置开始第一项因子研究' }}</strong><RouterLink to="/factor-studies/new">创建研究</RouterLink></div>
    </section>
  </div>
</template>

<style scoped>
.workbench-hero{display:flex;align-items:center;justify-content:space-between;gap:26px;padding:26px;background:linear-gradient(120deg,#fff,#f2f7ff 58%,#eef9f6)}
.workbench-hero h2{margin:9px 0 7px;font-size:25px}.workbench-hero p{max-width:740px;margin:0;color:var(--muted);line-height:1.7}.filter-heading{align-items:center}.toolbar :deep(.el-select){width:170px}.study-link{display:flex;flex-direction:column;gap:5px;color:var(--text);text-decoration:none}.study-link small,.cell-note{display:block;margin-top:5px;color:var(--dim);font-size:10px}.review-count{font-variant-numeric:tabular-nums}.review-count b{font-weight:700}.candidate{color:var(--success)}.discarded{color:var(--danger)}.explicit-state{display:flex;align-items:center;justify-content:center;gap:12px;padding:28px;color:var(--muted);font-size:12px}
</style>
