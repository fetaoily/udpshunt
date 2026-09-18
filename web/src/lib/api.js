// status fetch + rate derivation shared by all components.
export async function fetchStatus() {
  const res = await fetch('/status')
  if (!res.ok) throw new Error(`status ${res.status}`)
  return res.json()
}

// clients fetch: top rows of the per-client-IP table, server-side sorted.
export async function fetchClients(sort, order, limit) {
  const q = new URLSearchParams({ sort, order, limit: String(limit) })
  const res = await fetch('/clients?' + q)
  if (!res.ok) throw new Error(`clients ${res.status}`)
  return res.json()
}

// perSecond: rate between the last two cumulative samples.
export function perSecond(history, key) {
  if (!history || history.length < 2) return 0
  const a = history[history.length - 2]
  const b = history[history.length - 1]
  const dt = (b.t - a.t) / 1000
  if (dt <= 0) return 0
  return (b[key] - a[key]) / dt
}

// rateSeries: per-second deltas over the whole ring (for charts).
export function rateSeries(history, key) {
  if (!history) return []
  const out = []
  for (let i = 1; i < history.length; i++) {
    const dt = (history[i].t - history[i - 1].t) / 1000
    if (dt <= 0) continue
    out.push((history[i][key] - history[i - 1][key]) / dt)
  }
  return out
}

export function humanBytes(v) {
  if (v < 1024) return v.toFixed(0) + ' B'
  const units = ['KiB', 'MiB', 'GiB', 'TiB']
  let i = -1
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++ }
  return v.toFixed(1) + ' ' + units[i + 1]
}
