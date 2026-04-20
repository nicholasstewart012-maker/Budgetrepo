const stripTrailingSlash = (value) => String(value || '').replace(/\/+$/, '')

export function getApiBase() {
  const envBase = stripTrailingSlash(import.meta.env.VITE_API_BASE_URL || '')
  if (envBase) return envBase

  if (typeof window === 'undefined') return ''

  const { origin, hostname, port } = window.location
  if (origin === 'null') return ''

  // Dev server uses Vite proxy, so relative requests should stay same-origin.
  if (port === '5173') return ''

  // If the UI is hosted by the backend, same-origin requests also work.
  if (hostname === 'localhost' || hostname === '127.0.0.1') {
    return port === '8000' ? '' : 'http://127.0.0.1:8000'
  }

  return origin
}
