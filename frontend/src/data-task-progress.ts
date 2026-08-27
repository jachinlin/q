import type { Task } from './types'

const DATA_TASK_TYPES = new Set([
  'DATA_BOOTSTRAP',
  'DATA_UPDATE',
  'DATA_VALIDATION',
])

export type DataTaskProgressView = {
  stage: string
  message: string
  dataset: string | null
  datasetPosition: string | null
  completed: number
  total: number
  percentage: number
  waiting: boolean
}

export function isDataTask(task: Pick<Task, 'task_type'>): boolean {
  return DATA_TASK_TYPES.has(task.task_type)
}

export function dataTaskProgress(task: Pick<Task, 'status' | 'progress'>): DataTaskProgressView {
  const progress = task.progress
  const context = isRecord(progress?.context) ? progress.context : {}
  const total = nonNegativeInteger(progress?.total)
  const completed = Math.min(nonNegativeInteger(progress?.completed), total)
  const percentage = task.status === 'SUCCEEDED'
    ? 100
    : total > 0
      ? Math.min(100, Math.max(0, Math.round(completed / total * 100)))
      : 0
  const datasetIndex = positiveInteger(context.dataset_index)
  const datasetTotal = positiveInteger(context.dataset_total)

  return {
    stage: nonEmptyText(progress?.stage) ?? (task.status === 'QUEUED' ? 'QUEUED' : '—'),
    message: nonEmptyText(progress?.message) ?? (task.status === 'QUEUED'
      ? '等待 Worker 接收任务'
      : '等待进度更新'),
    dataset: nonEmptyText(context.dataset),
    datasetPosition: datasetIndex !== null && datasetTotal !== null
      ? `${Math.min(datasetIndex, datasetTotal)}/${datasetTotal}`
      : null,
    completed,
    total,
    percentage,
    waiting: total <= 0,
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function nonEmptyText(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null
}

function nonNegativeInteger(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) && Number.isInteger(value) && value >= 0
    ? value
    : 0
}

function positiveInteger(value: unknown): number | null {
  const normalized = nonNegativeInteger(value)
  return normalized > 0 ? normalized : null
}
