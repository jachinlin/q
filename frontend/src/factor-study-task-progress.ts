import type { Task } from './types'

const STAGE_LABELS: Record<string, string> = {
  QUEUED: '等待执行',
  VALIDATE: '校验研究',
  PREPARE_INPUTS: '准备输入',
  ANALYZE_FACTORS: '分析因子',
  PUBLISH: '发布产物',
}

const SUBSTAGE_LABELS: Record<string, string> = {
  BUILD_UNIVERSE: '构建 PIT 股票池',
  COMPUTE_FACTORS: '计算研究因子',
  BUILD_SIGNALS: '构建研究信号',
  LOAD_LABEL_INPUTS: '加载标签输入',
  BUILD_FORWARD_RETURNS: '构建远期收益',
  ANALYZE_STATISTICS: '计算研究统计',
  BUILD_METRICS: '整理研究指标',
  PUBLISH_ARTIFACTS: '写入并复核产物',
  REGISTER_OUTPUTS: '登记研究输出',
}

const STATE_LABELS: Record<string, string> = {
  STARTED: '进行中',
  PROGRESS: '进行中',
  COMPLETED: '已完成',
}

const EVIDENCE_LABELS: Array<[string, string]> = [
  ['session_count', '交易日'],
  ['eligible_row_count', '股票池成员'],
  ['instrument_count', '证券'],
  ['requested_factor_count', '请求因子'],
  ['execution_factor_count', '执行因子'],
  ['factor_row_count', '因子行'],
  ['signal_row_count', '信号行'],
  ['signal_variant_count', '信号版本'],
  ['bar_row_count', '行情行'],
  ['executable_state_row_count', '可执行状态行'],
  ['label_table_count', '标签表'],
  ['label_row_count', '标签行'],
  ['metric_count', '指标'],
  ['artifact_count', '产物'],
  ['artifact_row_count', '产物行'],
  ['artifact_byte_count', '产物字节'],
]

export type FactorStudyTaskProgressView = {
  stage: string
  stageLabel: string
  message: string
  substage: string | null
  substageLabel: string | null
  substageStateLabel: string | null
  lastCompletedLabel: string | null
  evidence: string[]
  completed: number
  total: number
  percentage: number
  waiting: boolean
  itemPosition: string | null
  itemPercentage: number | null
  signalDate: string | null
}

export function isFactorStudyTask(task: Pick<Task, 'task_type'>): boolean {
  return task.task_type === 'FACTOR_STUDY'
}

export function factorStudyTaskProgress(
  task: Pick<Task, 'status' | 'progress'>,
): FactorStudyTaskProgressView {
  const progress = task.progress
  const context = isRecord(progress?.context) ? progress.context : {}
  const total = nonNegativeInteger(progress?.total)
  const completed = Math.min(nonNegativeInteger(progress?.completed), total)
  const percentage = task.status === 'SUCCEEDED'
    ? 100
    : total > 0
      ? Math.min(100, Math.round(completed / total * 100))
      : 0
  const stage = nonEmptyText(progress?.stage) ?? (task.status === 'QUEUED' ? 'QUEUED' : '—')
  const substage = nonEmptyText(context.substage)
  const lastCompleted = nonEmptyText(context.last_completed_substage)
  const itemCompleted = positiveInteger(context.item_completed)
  const itemTotal = positiveInteger(context.item_total)
  const boundedItemCompleted = itemCompleted !== null && itemTotal !== null
    ? Math.min(itemCompleted, itemTotal)
    : null
  const evidenceSource = isRecord(context.last_completed_evidence)
    ? context.last_completed_evidence
    : context.substage_state === 'COMPLETED'
      ? context
      : {}

  return {
    stage,
    stageLabel: STAGE_LABELS[stage] ?? stage,
    message: nonEmptyText(progress?.message) ?? (task.status === 'QUEUED'
      ? '等待 Worker 接收任务'
      : '等待进度更新'),
    substage,
    substageLabel: substage === null ? null : (SUBSTAGE_LABELS[substage] ?? substage),
    substageStateLabel: stateLabel(context.substage_state),
    lastCompletedLabel: lastCompleted === null
      ? null
      : (SUBSTAGE_LABELS[lastCompleted] ?? lastCompleted),
    evidence: evidence(EVIDENCE_LABELS, evidenceSource),
    completed,
    total,
    percentage,
    waiting: total <= 0,
    itemPosition: boundedItemCompleted !== null && itemTotal !== null
      ? `${boundedItemCompleted}/${itemTotal}`
      : null,
    itemPercentage: boundedItemCompleted !== null && itemTotal !== null
      ? Math.round(boundedItemCompleted / itemTotal * 100)
      : null,
    signalDate: nonEmptyText(context.signal_date),
  }
}

function stateLabel(value: unknown): string | null {
  const state = nonEmptyText(value)
  return state === null ? null : (STATE_LABELS[state] ?? state)
}

function evidence(
  fields: Array<[string, string]>,
  values: Record<string, unknown>,
): string[] {
  const result = fields.flatMap(([field, label]) => {
    const value = nonNegativeIntegerOrNull(values[field])
    return value === null ? [] : [`${label} ${value.toLocaleString('zh-CN')}`]
  })
  const tableCounts = values.table_row_counts
  if (isRecord(tableCounts)) {
    const rows = Object.values(tableCounts)
      .map(nonNegativeIntegerOrNull)
      .filter((value): value is number => value !== null)
    result.push(`统计表 ${rows.length}`, `统计行 ${rows.reduce((sum, value) => sum + value, 0).toLocaleString('zh-CN')}`)
  }
  return result.slice(0, 6)
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function nonEmptyText(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null
}

function nonNegativeInteger(value: unknown): number {
  return nonNegativeIntegerOrNull(value) ?? 0
}

function nonNegativeIntegerOrNull(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && Number.isInteger(value) && value >= 0
    ? value
    : null
}

function positiveInteger(value: unknown): number | null {
  const normalized = nonNegativeIntegerOrNull(value)
  return normalized !== null && normalized > 0 ? normalized : null
}
