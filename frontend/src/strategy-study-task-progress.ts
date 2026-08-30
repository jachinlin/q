import type { Task } from './types'

const STAGE_LABELS: Record<string, string> = {
  QUEUED: '等待执行',
  VALIDATE: '校验研究',
  BACKTEST: '执行回测',
  ANALYTICS: '分析结果',
  PUBLISH: '发布产物',
}

const SUBSTAGE_LABELS: Record<string, string> = {
  BUILD_STRATEGY: '构造冻结策略',
  RUN_BACKTEST: '执行策略回测',
  CALCULATE_ANALYTICS: '计算绩效与归因',
  PUBLISH_ARTIFACTS: '写入并复核产物',
  REGISTER_OUTPUTS: '登记研究输出',
}

const STATE_LABELS: Record<string, string> = {
  STARTED: '进行中',
  PROGRESS: '进行中',
  COMPLETED: '已完成',
}

const EVIDENCE_LABELS: Array<[string, string]> = [
  ['market_instrument_count', '证券'],
  ['stock_instrument_count', '股票'],
  ['fund_instrument_count', 'ETF'],
  ['sessions_completed', '交易日'],
  ['metric_count', '指标'],
  ['artifact_count', '产物'],
  ['artifact_row_count', '产物行'],
  ['artifact_byte_count', '产物字节'],
  ['disclosure_count', '披露'],
]

export type StrategyStudyTaskProgressView = {
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
  tradeDate: string | null
}

export function isStrategyStudyTask(task: Pick<Task, 'task_type'>): boolean {
  return task.task_type === 'STRATEGY_STUDY'
}

export function strategyStudyTaskProgress(
  task: Pick<Task, 'status' | 'progress'>,
): StrategyStudyTaskProgressView {
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
    tradeDate: nonEmptyText(context.trade_date),
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
    result.push(
      `数据表 ${rows.length}`,
      `数据行 ${rows.reduce((sum, value) => sum + value, 0).toLocaleString('zh-CN')}`,
    )
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
