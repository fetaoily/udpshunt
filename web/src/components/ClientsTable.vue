<template>
  <div class="card">
    <h3>clients <span class="meta">tracked {{ cs?.tracked ?? 0 }}</span>
      <span v-if="cs?.evicted" class="meta">evicted {{ cs.evicted }}</span></h3>
    <p v-if="cs && !cs.enabled" class="meta">client stats are disabled (client_stats.enabled: false)</p>
    <table v-else>
      <thead>
        <tr>
          <th v-for="c in cols" :key="c.key" :class="{ active: sort === c.key }" @click="setSort(c.key)">
            {{ c.label }}<span v-if="sort === c.key">{{ asc ? ' ▲' : ' ▼' }}</span>
          </th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="r in cs?.rows ?? []" :key="r.ip">
          <td class="ip">{{ r.ip }}</td>
          <td class="num">{{ r.requests }}</td>
          <td class="num">{{ r.pps_in.toFixed(1) }}</td>
          <td class="num">{{ r.responses }}</td>
          <td class="num">{{ humanBytes(r.bps_in) }}/s</td>
          <td class="num">{{ humanBytes(r.bps_out) }}/s</td>
          <td class="num">{{ humanBytes(r.bytes_in) }}</td>
          <td class="num">{{ humanBytes(r.bytes_out) }}</td>
          <td class="num">{{ ago(r.last_seen) }}</td>
        </tr>
      </tbody>
    </table>
    <p v-if="cs && cs.enabled" class="meta">top {{ (cs.rows ?? []).length }} of {{ cs.tracked }} IPs · click a header to sort</p>
  </div>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { fetchClients, humanBytes } from '../lib/api.js'

// Column order matches the TUI clients view; key is the /clients sort param.
const cols = [
  { label: 'ip', key: 'ip' },
  { label: 'req', key: 'requests' },
  { label: 'req/s', key: 'pps_in' },
  { label: 'resp', key: 'responses' },
  { label: 'up/s', key: 'bps_in' },
  { label: 'down/s', key: 'bps_out' },
  { label: 'up', key: 'bytes_in' },
  { label: 'down', key: 'bytes_out' },
  { label: 'last seen', key: 'last_seen' },
]

const cs = ref(null)
const sort = ref('requests')
const asc = ref(false)
let timer = null

async function poll() {
  try {
    cs.value = await fetchClients(sort.value, asc.value ? 'asc' : 'desc', 200)
  } catch {
    // keep the last snapshot; the error line on <main> covers API loss
  }
}

function setSort(key) {
  if (sort.value === key) {
    asc.value = !asc.value
  } else {
    sort.value = key
    asc.value = key !== 'requests' && key !== 'last_seen'
  }
  poll()
}

function ago(nanos) {
  if (!nanos) return '-'
  const s = Math.max(0, Math.round((Date.now() - nanos / 1e6) / 1000))
  if (s < 60) return s + 's'
  if (s < 3600) return Math.floor(s / 60) + 'm' + (s % 60) + 's'
  return Math.floor(s / 3600) + 'h' + Math.floor((s % 3600) / 60) + 'm'
}

onMounted(() => {
  poll()
  timer = setInterval(poll, 2000)
})
onBeforeUnmount(() => clearInterval(timer))
</script>

<style scoped>
.card { border: 1px solid #333; border-radius: 6px; padding: 12px 16px; margin-bottom: 16px; }
h3 { margin: 0 0 8px; font-size: 14px; }
.meta { color: #888; font-weight: normal; font-size: 12px; margin-left: 8px; }
p.meta { margin: 4px 0 0; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid #222; }
th { cursor: pointer; color: #888; user-select: none; white-space: nowrap; }
th.active { color: #ddd; }
td.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
td.ip { font-family: ui-monospace, monospace; }
tr:hover td { background: #1a1a1a; }
</style>
