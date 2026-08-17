import dayjs from 'dayjs'

export const formatDate = (value?: string | null) =>
  value ? dayjs(value).format('YYYY-MM-DD') : '—'

export const formatTime = (value?: string | null) =>
  value ? dayjs(value).format('MM-DD HH:mm:ss') : '—'

export const formatDuration = (
  startedAt?: string | null,
  completedAt?: string | null,
  now = Date.now(),
) => {
  if (!startedAt) return '—'
  const start = dayjs(startedAt)
  const end = completedAt ? dayjs(completedAt) : dayjs(now)
  if (!start.isValid() || !end.isValid()) return '—'
  let seconds = Math.max(0, end.diff(start, 'second'))
  const days = Math.floor(seconds / 86_400)
  seconds %= 86_400
  const hours = Math.floor(seconds / 3_600)
  seconds %= 3_600
  const minutes = Math.floor(seconds / 60)
  seconds %= 60
  if (days > 0) return `${days}天 ${hours}小时`
  if (hours > 0) return `${hours}小时 ${minutes}分`
  if (minutes > 0) return `${minutes}分 ${seconds}秒`
  return `${seconds}秒`
}

export const formatNumber = (value?: number | null) =>
  value == null || !Number.isFinite(value) ? '—' : new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 2 }).format(value)

export const formatPercent = (value?: number | null) =>
  value == null || !Number.isFinite(value) ? '—' : `${value >= 0 ? '+' : ''}${(value * 100).toFixed(2)}%`

export const shortHash = (value?: string | null) => (value ? `${value.slice(0, 8)}…` : '—')

export const statusType = (status: string) => {
  if (['SUCCEEDED', 'PASSED', 'READY'].includes(status)) return 'success'
  if (['FAILED', 'FATAL', 'SEVERE', 'BLOCKED', 'ORPHANED'].includes(status)) return 'danger'
  if (['RUNNING', 'QUEUED', 'CANCEL_REQUESTED'].includes(status)) return 'primary'
  if (['WARNING', 'CANCELLED'].includes(status)) return 'warning'
  return 'info'
}
