<template>
  <div class="panel">
    <h2>Overview</h2>
    <div class="stats">
      <div class="stat"><span class="label">uptime</span><span class="value">{{ status.uptime }}</span></div>
      <div class="stat"><span class="label">sessions</span><span class="value">{{ status.sessions?.active ?? 0 }}</span></div>
      <div class="stat"><span class="label">backends up</span><span class="value">{{ healthyCount }}/{{ backendCount }}</span></div>
      <div class="stat"><span class="label">in</span><span class="value">{{ inBps }} ({{ inPps }} pps)</span></div>
      <div class="stat"><span class="label">out</span><span class="value">{{ outBps }} ({{ outPps }} pps)</span></div>
    </div>
    <div ref="chart" class="chart"></div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import * as echarts from 'echarts'
import { humanBytes, perSecond, rateSeries } from '../lib/api.js'

const props = defineProps({ status: Object })
const chart = ref(null)
let instance = null

const healthyCount = computed(() => sumBackends(props.status).healthy)
const backendCount = computed(() => sumBackends(props.status).total)
const inPps = computed(() => sumRate(props.status, 'in'))
const outPps = computed(() => sumRate(props.status, 'out'))
const inBps = computed(() => humanBytes(sumRate(props.status, 'bytes_in')))
const outBps = computed(() => humanBytes(sumRate(props.status, 'bytes_out')))

function sumBackends(st) {
  let healthy = 0, total = 0
  for (const l of st?.listeners ?? []) for (const b of l.backends ?? []) { total++; if (b.healthy) healthy++ }
  return { healthy, total }
}
function sumRate(st, key) {
  let v = 0
  for (const l of st?.listeners ?? []) v += perSecond(l.history, key)
  return v
}

function render() {
  if (!chart.value || !props.status) return
  if (!instance) instance = echarts.init(chart.value)
  const listeners = props.status.listeners ?? []
  const series = listeners.map(l => ({
    name: l.name,
    type: 'line',
    showSymbol: false,
    data: rateSeries(l.history, 'in')
  }))
  const n = listeners[0]?.history?.length ?? 0
  instance.setOption({
    backgroundColor: 'transparent',
    title: { text: 'packets in / s', left: 8, top: 4, textStyle: { fontSize: 12 } },
    tooltip: { trigger: 'axis' },
    legend: { bottom: 0 },
    grid: { left: 48, right: 16, top: 32, bottom: 32 },
    xAxis: { type: 'category', data: Array.from({ length: Math.max(n - 1, 0) }, (_, i) => i) },
    yAxis: { type: 'value' },
    series
  }, true)
}

watch(() => props.status, render)
onBeforeUnmount(() => instance?.dispose())
</script>

<style scoped>
.panel { border: 1px solid #333; border-radius: 6px; padding: 12px 16px; margin-bottom: 16px; }
h2 { margin: 0 0 8px; font-size: 15px; }
.stats { display: flex; gap: 24px; flex-wrap: wrap; margin-bottom: 12px; }
.stat { display: flex; flex-direction: column; }
.label { color: #888; font-size: 11px; }
.value { font-size: 16px; }
.chart { width: 100%; height: 220px; }
</style>
