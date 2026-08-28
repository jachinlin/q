<script setup lang="ts">
import { computed } from 'vue'

import { factorStudyTaskProgress } from '../factor-study-task-progress'
import type { Task } from '../types'

const props = withDefaults(defineProps<{
  task: Task
  mode?: 'compact' | 'detail'
}>(), {
  mode: 'compact',
})

const view = computed(() => factorStudyTaskProgress(props.task))
</script>

<template>
  <section
    class="factor-task-progress"
    :class="`factor-task-progress--${mode}`"
    :data-task-stage="view.stage"
    :data-task-substage="view.substage ?? ''"
    :data-task-percentage="view.percentage"
  >
    <header class="factor-task-progress__heading">
      <div>
        <span v-if="mode === 'detail'" class="factor-task-progress__eyebrow">因子研究进度</span>
        <strong>{{ view.stageLabel }}</strong>
        <span v-if="view.substageLabel" class="factor-task-progress__substage">
          {{ view.substageLabel }}<template v-if="view.substageStateLabel"> · {{ view.substageStateLabel }}</template>
        </span>
      </div>
      <span class="factor-task-progress__count">
        {{ view.waiting ? '等待进度' : `${view.completed}/${view.total}` }}
      </span>
    </header>
    <p class="factor-task-progress__message" :title="view.message">{{ view.message }}</p>
    <div v-if="view.itemPosition || view.signalDate" class="factor-task-progress__item">
      <span v-if="view.itemPosition">当前批次 {{ view.itemPosition }}</span>
      <span v-if="view.signalDate">{{ view.signalDate }}</span>
    </div>
    <el-progress
      :percentage="view.percentage"
      :stroke-width="mode === 'compact' ? 5 : 8"
      :show-text="mode === 'detail'"
    />
    <div v-if="view.itemPercentage !== null && mode === 'detail'" class="factor-task-progress__subprogress">
      <span>子步骤 {{ view.itemPercentage }}%</span>
      <el-progress :percentage="view.itemPercentage" :stroke-width="4" :show-text="false" />
    </div>
    <div v-if="view.lastCompletedLabel || view.evidence.length" class="factor-task-progress__evidence">
      <span v-if="view.lastCompletedLabel">最近完成：{{ view.lastCompletedLabel }}</span>
      <span v-for="item in view.evidence" :key="item">{{ item }}</span>
    </div>
  </section>
</template>

<style scoped>
.factor-task-progress{min-width:0}.factor-task-progress__heading{display:flex;align-items:center;justify-content:space-between;gap:10px}.factor-task-progress__heading>div{display:flex;min-width:0;align-items:center;gap:8px}.factor-task-progress__heading strong{color:var(--muted);font-size:11px}.factor-task-progress__eyebrow{color:var(--dim);font-size:9px;letter-spacing:.08em;text-transform:uppercase}.factor-task-progress__substage{overflow:hidden;color:var(--blue);font-size:10px;text-overflow:ellipsis;white-space:nowrap}.factor-task-progress__count{flex:none;color:var(--dim);font:10px ui-monospace,Consolas,monospace}.factor-task-progress__message{margin:6px 0;color:var(--muted);font-size:11px;line-height:1.45}.factor-task-progress__item,.factor-task-progress__evidence{display:flex;flex-wrap:wrap;gap:8px;color:var(--dim);font-size:10px}.factor-task-progress__item{margin:0 0 7px}.factor-task-progress__item span+span::before,.factor-task-progress__evidence span+span::before{content:"·";margin-right:8px}.factor-task-progress__evidence{margin-top:8px}.factor-task-progress__subprogress{display:grid;grid-template-columns:max-content 1fr;align-items:center;gap:10px;margin-top:8px;color:var(--dim);font-size:9px}.factor-task-progress--compact{display:grid;grid-template-columns:minmax(190px,260px) minmax(140px,1fr) max-content;grid-template-areas:"stage message count" "item progress progress" "evidence evidence evidence";align-items:center;column-gap:12px;row-gap:8px;max-width:980px}.factor-task-progress--compact .factor-task-progress__heading{display:contents}.factor-task-progress--compact .factor-task-progress__heading>div{grid-area:stage}.factor-task-progress--compact .factor-task-progress__heading strong{color:var(--text)}.factor-task-progress--compact .factor-task-progress__count{grid-area:count;justify-self:end}.factor-task-progress--compact .factor-task-progress__message{grid-area:message;overflow:hidden;margin:0;text-overflow:ellipsis;white-space:nowrap}.factor-task-progress--compact .factor-task-progress__item{grid-area:item;overflow:hidden;margin:0;white-space:nowrap}.factor-task-progress--compact :deep(.el-progress){grid-area:progress}.factor-task-progress--compact .factor-task-progress__evidence{grid-area:evidence;margin:0}.factor-task-progress--detail{margin:14px 0;padding:16px;border:1px solid var(--border);border-radius:10px;background:var(--surface-raised)}.factor-task-progress--detail .factor-task-progress__heading>div{align-items:baseline}.factor-task-progress--detail .factor-task-progress__heading strong{font-size:13px}.factor-task-progress--detail .factor-task-progress__message{margin:9px 0 7px;font-size:12px}
</style>
