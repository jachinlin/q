import { describe, expect, it } from 'vitest'

import { factorStudyTaskProgress, isFactorStudyTask } from './factor-study-task-progress'
import type { Task } from './types'

function task(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task-1',
    subject_kind: 'FACTOR_STUDY',
    subject_id: 'study-1',
    task_type: 'FACTOR_STUDY',
    status: 'RUNNING',
    priority: 0,
    progress: {
      stage: 'ANALYZE_FACTORS',
      completed: 2,
      total: 4,
      message: '正在准备 PIT 股票池（500/1000）',
      context: {
        substage: 'BUILD_UNIVERSE',
        substage_state: 'PROGRESS',
        item_completed: 500,
        item_total: 1000,
        signal_date: '2022-01-05',
        last_completed_substage: 'COMPUTE_FACTORS',
        last_completed_evidence: {
          requested_factor_count: 2,
          execution_factor_count: 3,
          factor_row_count: 12345,
        },
      },
    },
    created_at: '2026-08-28T00:00:00Z',
    started_at: '2026-08-28T00:00:01Z',
    updated_at: '2026-08-28T00:00:02Z',
    heartbeat_at: '2026-08-28T00:00:02Z',
    completed_at: null,
    worker_id: 'worker-1',
    error: null,
    result: null,
    ...overrides,
  }
}

describe('factor study task progress', () => {
  it('keeps four-stage progress while exposing sampled item progress and evidence', () => {
    expect(factorStudyTaskProgress(task())).toEqual({
      stage: 'ANALYZE_FACTORS',
      stageLabel: '分析因子',
      message: '正在准备 PIT 股票池（500/1000）',
      substage: 'BUILD_UNIVERSE',
      substageLabel: '构建 PIT 股票池',
      substageStateLabel: '进行中',
      lastCompletedLabel: '计算研究因子',
      evidence: ['请求因子 2', '执行因子 3', '因子行 12,345'],
      completed: 2,
      total: 4,
      percentage: 50,
      waiting: false,
      itemPosition: '500/1000',
      itemPercentage: 50,
      signalDate: '2022-01-05',
    })
  })

  it('handles queued, successful and malformed progress without inventing counters', () => {
    const queued = factorStudyTaskProgress(task({
      status: 'QUEUED',
      progress: { stage: 'QUEUED', completed: 0, total: 0, message: '', context: {} },
    }))
    expect(queued).toMatchObject({ percentage: 0, waiting: true, message: '等待 Worker 接收任务' })

    const succeeded = factorStudyTaskProgress(task({
      status: 'SUCCEEDED',
      progress: { stage: 'PUBLISH', completed: 4, total: 4, message: '完成', context: {} },
    }))
    expect(succeeded.percentage).toBe(100)

    const malformed = factorStudyTaskProgress(task({
      progress: {
        stage: 'ANALYZE_FACTORS', completed: Number.NaN, total: -1, message: '',
        context: { item_completed: '5', item_total: 10, substage: [] },
      },
    }))
    expect(malformed).toMatchObject({ completed: 0, total: 0, waiting: true, itemPosition: null, substage: null })
    expect(isFactorStudyTask(task())).toBe(true)
    expect(isFactorStudyTask(task({ task_type: 'STRATEGY_STUDY' }))).toBe(false)
  })
})
