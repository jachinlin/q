import { describe, expect, it } from 'vitest'

import { isStrategyStudyTask, strategyStudyTaskProgress } from './strategy-study-task-progress'
import type { Task } from './types'

function task(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task-1',
    subject_kind: 'STRATEGY_STUDY',
    subject_id: 'study-1',
    task_type: 'STRATEGY_STUDY',
    status: 'RUNNING',
    priority: 0,
    progress: {
      stage: 'BACKTEST',
      completed: 1,
      total: 4,
      message: '正在执行策略回测（850/1699）',
      context: {
        substage: 'RUN_BACKTEST',
        substage_state: 'PROGRESS',
        item_completed: 850,
        item_total: 1699,
        trade_date: '2021-06-22',
        last_completed_substage: 'BUILD_STRATEGY',
        last_completed_evidence: {
          market_instrument_count: 3,
          stock_instrument_count: 0,
          fund_instrument_count: 3,
        },
      },
    },
    created_at: '2026-08-30T00:00:00Z',
    started_at: '2026-08-30T00:00:01Z',
    updated_at: '2026-08-30T00:00:02Z',
    heartbeat_at: '2026-08-30T00:00:02Z',
    completed_at: null,
    worker_id: 'worker-1',
    error: null,
    result: null,
    ...overrides,
  }
}

describe('strategy study task progress', () => {
  it('keeps four-stage progress while exposing sampled backtest progress and evidence', () => {
    expect(strategyStudyTaskProgress(task())).toEqual({
      stage: 'BACKTEST',
      stageLabel: '执行回测',
      message: '正在执行策略回测（850/1699）',
      substage: 'RUN_BACKTEST',
      substageLabel: '执行策略回测',
      substageStateLabel: '进行中',
      lastCompletedLabel: '构造冻结策略',
      evidence: ['证券 3', '股票 0', 'ETF 3'],
      completed: 1,
      total: 4,
      percentage: 25,
      waiting: false,
      itemPosition: '850/1699',
      itemPercentage: 50,
      tradeDate: '2021-06-22',
    })
  })

  it('handles queued, successful and malformed progress without inventing counters', () => {
    const queued = strategyStudyTaskProgress(task({
      status: 'QUEUED',
      progress: { stage: 'QUEUED', completed: 0, total: 0, message: '', context: {} },
    }))
    expect(queued).toMatchObject({ percentage: 0, waiting: true, message: '等待 Worker 接收任务' })

    const succeeded = strategyStudyTaskProgress(task({
      status: 'SUCCEEDED',
      progress: { stage: 'PUBLISH', completed: 4, total: 4, message: '完成', context: {} },
    }))
    expect(succeeded.percentage).toBe(100)

    const malformed = strategyStudyTaskProgress(task({
      progress: {
        stage: 'BACKTEST', completed: Number.NaN, total: -1, message: '',
        context: { item_completed: '5', item_total: 10, substage: [] },
      },
    }))
    expect(malformed).toMatchObject({ completed: 0, total: 0, waiting: true, itemPosition: null, substage: null })
    expect(isStrategyStudyTask(task())).toBe(true)
    expect(isStrategyStudyTask(task({ task_type: 'FACTOR_STUDY' }))).toBe(false)
  })
})
