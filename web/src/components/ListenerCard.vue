<template>
  <div class="card">
    <h3>{{ listener.name }} <span class="bind">{{ listener.bind }}</span> <span class="balance">{{ listener.balance }}</span></h3>
    <div class="chart" :ref="chartRef"></div>
    <table>
      <thead><tr><th>backend</th><th>health</th><th>sessions</th></tr></thead>
      <tbody>
        <tr v-for="b in listener.backends ?? []" :key="b.addr">
          <td>{{ b.addr }}</td>
          <td :class="b.healthy ? 'up' : 'down'">{{ b.healthy ? 'UP' : 'DOWN' }}</td>
          <td>{{ b.sessions }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts'
import { rateSeries } from '../lib/api.js'

const props = defineProps({ listener: Object })
const chartRef = ref(null)
let instance = null

function mount() {
  if (chartRef.value && !instance) {
    instance = echarts.init(chartRef.value)
  }
}
const onResize = () => instance?.resize()
onMounted(() => {
  mount()
  if (props.listener) render()
  window.addEventListener('resize', onResize)
})

function render() {
  mount()
  if (!instance) return
  const h = props.listener?.history ?? []
  instance.setOption({
    backgroundColor: 'transparent',
    grid: { left: 48, right: 8, top: 8, bottom: 20 },
    xAxis: { type: 'category', show: false, data: Array.from({ length: Math.max(h.length - 1, 0) }, (_, i) => i) },
    yAxis: { type: 'value' },
    series: [{ type: 'line', showSymbol: false, data: rateSeries(h, 'in') }]
  }, true)
}
watch(() => props.listener, render)
onBeforeUnmount(() => {
  window.removeEventListener('resize', onResize)
  instance?.dispose()
})
</script>

<style scoped>
.card { border: 1px solid #333; border-radius: 6px; padding: 12px 16px; margin-bottom: 16px; }
h3 { margin: 0 0 8px; font-size: 14px; }
.bind, .balance { color: #888; font-weight: normal; font-size: 12px; margin-left: 8px; }
.chart { width: 100%; height: 120px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid #222; }
.up { color: #4ade80; }
.down { color: #f87171; }
</style>
