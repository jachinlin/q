<script setup lang="ts">
import { computed } from 'vue'

import { dataTaskProgress } from '../data-task-progress'
import type { Task } from '../types'

const props = withDefaults(defineProps<{
  task: Task
  mode?: 'compact' | 'detail'
}>(), {
  mode: 'compact',
})

const view = computed(() => dataTaskProgress(props.task))
</script>

<template>
  <section
    class="data-task-progress"
    :class="`data-task-progress--${mode}`"
    :data-task-stage="view.stage"
    :data-task-percentage="view.percentage"
  >
    <header class="data-task-progress__heading">
      <div>
        <span v-if="mode === 'detail'" class="data-task-progress__eyebrow">当前活动进度</span>
        <strong>{{ view.stage }}</strong>
      </div>
      <span class="data-task-progress__count">
        {{ view.waiting ? '等待进度' : `${view.completed}/${view.total}` }}
      </span>
    </header>
    <p class="data-task-progress__message" :title="view.message">{{ view.message }}</p>
    <div v-if="view.dataset || view.datasetPosition" class="data-task-progress__dataset">
      <span v-if="view.dataset">{{ view.dataset }}</span>
      <span v-if="view.datasetPosition">数据集 {{ view.datasetPosition }}</span>
    </div>
    <el-progress
      :percentage="view.percentage"
      :stroke-width="mode === 'compact' ? 5 : 8"
      :show-text="mode === 'detail'"
    />
  </section>
</template>

<style scoped>
.data-task-progress{min-width:0}.data-task-progress__heading{display:flex;align-items:center;justify-content:space-between;gap:10px}.data-task-progress__heading>div{display:flex;min-width:0;align-items:center;gap:8px}.data-task-progress__heading strong{color:var(--muted);font-size:11px}.data-task-progress__eyebrow{color:var(--dim);font-size:9px;letter-spacing:.08em;text-transform:uppercase}.data-task-progress__count{flex:none;color:var(--dim);font:10px ui-monospace,Consolas,monospace}.data-task-progress__message{margin:6px 0;color:var(--muted);font-size:11px;line-height:1.45}.data-task-progress__dataset{display:flex;gap:8px;margin:0 0 7px;color:var(--dim);font-size:10px}.data-task-progress__dataset span+span::before{content:"·";margin-right:8px}.data-task-progress--compact{display:grid;grid-template-columns:minmax(110px,160px) minmax(120px,1fr) max-content;grid-template-areas:"stage message count" "dataset progress progress";align-items:center;column-gap:12px;row-gap:8px;max-width:900px}.data-task-progress--compact .data-task-progress__heading{display:contents}.data-task-progress--compact .data-task-progress__heading>div{grid-area:stage}.data-task-progress--compact .data-task-progress__heading strong{color:var(--text);font-size:11px}.data-task-progress--compact .data-task-progress__count{grid-area:count;justify-self:end}.data-task-progress--compact .data-task-progress__message{grid-area:message;overflow:hidden;margin:0;text-overflow:ellipsis;white-space:nowrap}.data-task-progress--compact .data-task-progress__dataset{grid-area:dataset;overflow:hidden;margin:0;text-overflow:ellipsis;white-space:nowrap}.data-task-progress--compact :deep(.el-progress){grid-area:progress}.data-task-progress--detail{margin:14px 0;padding:16px;border:1px solid var(--border);border-radius:10px;background:var(--surface-raised)}.data-task-progress--detail .data-task-progress__heading>div{align-items:baseline}.data-task-progress--detail .data-task-progress__heading strong{font-size:13px}.data-task-progress--detail .data-task-progress__message{margin:9px 0 7px;font-size:12px}.data-task-progress--detail .data-task-progress__dataset{margin-bottom:10px}
</style>
