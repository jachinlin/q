import type { ApiError } from './types'

const REQUEST_TIMEOUT = 15_000

export class DashboardApiError extends Error {
  code: string
  remediation: string | null
  requestId: string | null
  status: number

  constructor(payload: ApiError['error'], status: number) {
    super(payload.message)
    this.name = 'DashboardApiError'
    this.code = payload.code
    this.remediation = payload.remediation
    this.requestId = payload.request_id ?? null
    this.status = status
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController()
  const timeout = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT)
  const method = init?.method ?? 'GET'
  const headers = new Headers(init?.headers)
  if (method !== 'GET') {
    headers.set('Content-Type', 'application/json')
    headers.set('X-Request-ID', crypto.randomUUID())
  }
  try {
    const response = await fetch(path, { ...init, headers, signal: controller.signal })
    const payload = (await response.json()) as T | ApiError
    if (!response.ok) {
      const detail = (payload as Partial<ApiError>).error
      if (detail && typeof detail.code === 'string' && typeof detail.message === 'string') {
        throw new DashboardApiError(detail, response.status)
      }
      throw new DashboardApiError({
        code: `HTTP_${response.status}`,
        message: `请求失败（HTTP ${response.status}）`,
        severity: 'SEVERE',
        retryable: response.status >= 500,
        remediation: '请刷新页面后重试；若问题持续，请查看 Dashboard 服务日志。',
        request_id: response.headers.get('X-Request-ID') ?? '',
      }, response.status)
    }
    return payload as T
  } finally {
    window.clearTimeout(timeout)
  }
}

export const api = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body: unknown = {}) =>
    request<T>(path, { method: 'POST', body: JSON.stringify(body) }),
  delete: <T>(path: string) =>
    request<T>(path, { method: 'DELETE', body: '{}' }),
  patch: <T>(path: string, body: unknown) =>
    request<T>(path, { method: 'PATCH', body: JSON.stringify(body) }),
}
