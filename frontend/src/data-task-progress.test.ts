import { describe, expect, it } from 'vitest'

import { dataTaskProgress, isDataTask } from './data-task-progress'
import type { Task } from './types'

function task(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task-1',
    subject_kind: null,
    subject_id: null,
    task_type: 'DATA_UPDATE',
    status: 'RUNNING',
    priority: 0,
    progress: {
      stage: 'CURATE', completed: 3, total: 8, message: '正在清洗 stock_daily_bar Raw 3/8',
      context: { dataset: 'stock_daily_bar', dataset_index: 6, dataset_total: 20 },
    },
    created_at: '2026-08-27T00:00:00Z',
    started_at: '2026-08-27T00:00:01Z',
    updated_at: '2026-08-27T00:00:02Z',
    heartbeat_at: '2026-08-27T00:00:02Z',
    completed_at: null,
    worker_id: 'worker-1',
    error: null,
    result: null,
    ...overrides,
  }
}

describe('DATA task progress', () => {
  it('normalizes the current activity without inventing a whole-pipeline percentage', () => {
    expect(dataTaskProgress(task())).toEqual({
      stage: 'CURATE',
      message: '正在清洗 stock_daily_bar Raw 3/8',
      dataset: 'stock_daily_bar',
      datasetPosition: '6/20',
      completed: 3,
      total: 8,
      percentage: 38,
      waiting: false,
    })
  })

  it('shows queued tasks as waiting and successful tasks as complete', () => {
    const queued = dataTaskProgress(task({
      status: 'QUEUED',
      progress: { stage: 'queued', completed: 0, total: 0, message: '', context: {} },
    }))
    expect(queued.percentage).toBe(0)
    expect(queued.waiting).toBe(true)
    expect(queued.message).toBe('等待 Worker 接收任务')

    const succeeded = dataTaskProgress(task({
      status: 'SUCCEEDED',
      progress: { stage: 'COMPLETE', completed: 0, total: 0, message: '完成', context: {} },
    }))
    expect(succeeded.percentage).toBe(100)
  })

  it('fails closed for malformed counters and only recognizes supported DATA tasks', () => {
    const malformed = task({
      status: 'FAILED',
      progress: {
        stage: 'VALIDATE', completed: Number.NaN, total: -1, message: '校验失败',
        context: { dataset_index: '2', dataset_total: 20 },
      },
    })
    expect(dataTaskProgress(malformed)).toMatchObject({
      completed: 0, total: 0, percentage: 0, waiting: true, datasetPosition: null,
    })
    expect(isDataTask(task({ task_type: 'DATA_VALIDATION' }))).toBe(true)
    expect(isDataTask(task({ task_type: 'STRATEGY_STUDY' }))).toBe(false)
  })
})
