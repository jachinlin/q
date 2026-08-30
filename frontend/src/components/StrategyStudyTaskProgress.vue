<script setup lang="ts">
import { computed } from 'vue'

import { strategyStudyTaskProgress } from '../strategy-study-task-progress'
import type { Task } from '../types'

const props = withDefaults(defineProps<{
  task: Task
  mode?: 'compact' | 'detail'
}>(), {
  mode: 'compact',
})

const view = computed(() => strategyStudyTaskProgress(props.task))
const compactMeta = computed(() => [
  view.value.itemPosition ? `当前交易日 ${view.value.itemPosition}` : null,
  view.value.tradeDate,
  view.value.lastCompletedLabel ? `最近完成：${view.value.lastCompletedLabel}` : null,
  ...view.value.evidence,
].filter((item): item is string => Boolean(item)).join(' · '))
</script>

<template>
  <section
    class="strategy-task-progress"
    :class="`strategy-task-progress--${mode}`"
    :data-task-stage="view.stage"
    :data-task-substage="view.substage ?? ''"
    :data-task-percentage="view.percentage"
  >
    <header class="strategy-task-progress__heading">
      <div>
        <span v-if="mode === 'detail'" class="strategy-task-progress__eyebrow">策略研究进度</span>
        <strong>{{ view.stageLabel }}</strong>
        <span v-if="view.substageLabel" class="strategy-task-progress__substage">
          {{ view.substageLabel }}<template v-if="view.substageStateLabel"> · {{ view.substageStateLabel }}</template>
        </span>
      </div>
      <span class="strategy-task-progress__count">
        {{ view.waiting ? '等待进度' : `${view.completed}/${view.total}` }}
      </span>
    </header>
    <p class="strategy-task-progress__message" :title="view.message">{{ view.message }}</p>
    <div
      v-if="mode === 'compact' && compactMeta"
      class="strategy-task-progress__item"
      :title="compactMeta"
    >
      {{ compactMeta }}
    </div>
    <div v-else-if="view.itemPosition || view.tradeDate" class="strategy-task-progress__item">
      <span v-if="view.itemPosition">当前交易日 {{ view.itemPosition }}</span>
      <span v-if="view.tradeDate">{{ view.tradeDate }}</span>
    </div>
    <el-progress
      :percentage="view.percentage"
      :stroke-width="mode === 'compact' ? 5 : 8"
      :show-text="mode === 'detail'"
    />
    <div v-if="view.itemPercentage !== null && mode === 'detail'" class="strategy-task-progress__subprogress">
      <span>回测进度 {{ view.itemPercentage }}%</span>
      <el-progress :percentage="view.itemPercentage" :stroke-width="4" :show-text="false" />
    </div>
    <div
      v-if="mode === 'detail' && (view.lastCompletedLabel || view.evidence.length)"
      class="strategy-task-progress__evidence"
    >
      <span v-if="view.lastCompletedLabel">最近完成：{{ view.lastCompletedLabel }}</span>
      <span v-for="item in view.evidence" :key="item">{{ item }}</span>
    </div>
  </section>
</template>

<style scoped>
.strategy-task-progress{min-width:0}.strategy-task-progress__heading{display:flex;align-items:center;justify-content:space-between;gap:10px}.strategy-task-progress__heading>div{display:flex;min-width:0;align-items:center;gap:8px}.strategy-task-progress__heading strong{color:var(--muted);font-size:11px}.strategy-task-progress__eyebrow{color:var(--dim);font-size:9px;letter-spacing:.08em;text-transform:uppercase}.strategy-task-progress__substage{overflow:hidden;color:var(--blue);font-size:10px;text-overflow:ellipsis;white-space:nowrap}.strategy-task-progress__count{flex:none;color:var(--dim);font:10px ui-monospace,Consolas,monospace}.strategy-task-progress__message{margin:6px 0;color:var(--muted);font-size:11px;line-height:1.45}.strategy-task-progress__item,.strategy-task-progress__evidence{display:flex;flex-wrap:wrap;gap:8px;color:var(--dim);font-size:10px}.strategy-task-progress__item{margin:0 0 7px}.strategy-task-progress__item span+span::before,.strategy-task-progress__evidence span+span::before{content:"·";margin-right:8px}.strategy-task-progress__evidence{margin-top:8px}.strategy-task-progress__subprogress{display:grid;grid-template-columns:max-content 1fr;align-items:center;gap:10px;margin-top:8px;color:var(--dim);font-size:9px}.strategy-task-progress--compact{display:grid;grid-template-columns:minmax(110px,160px) minmax(120px,1fr) max-content;grid-template-areas:"stage message count" "item progress progress";align-items:center;column-gap:12px;row-gap:8px;max-width:900px}.strategy-task-progress--compact .strategy-task-progress__heading{display:contents}.strategy-task-progress--compact .strategy-task-progress__heading>div{grid-area:stage;overflow:hidden}.strategy-task-progress--compact .strategy-task-progress__heading strong{flex:none;color:var(--text)}.strategy-task-progress--compact .strategy-task-progress__count{grid-area:count;justify-self:end}.strategy-task-progress--compact .strategy-task-progress__message{grid-area:message;overflow:hidden;margin:0;text-overflow:ellipsis;white-space:nowrap}.strategy-task-progress--compact .strategy-task-progress__item{display:block;grid-area:item;overflow:hidden;margin:0;text-overflow:ellipsis;white-space:nowrap}.strategy-task-progress--compact :deep(.el-progress){grid-area:progress}.strategy-task-progress--detail{margin:14px 0;padding:16px;border:1px solid var(--border);border-radius:10px;background:var(--surface-raised)}.strategy-task-progress--detail .strategy-task-progress__heading>div{align-items:baseline}.strategy-task-progress--detail .strategy-task-progress__heading strong{font-size:13px}.strategy-task-progress--detail .strategy-task-progress__message{margin:9px 0 7px;font-size:12px}
</style>
