<template>
  <main>
    <h1>udpshunt</h1>
    <p v-if="error" class="error">admin API unreachable: {{ error }}</p>
    <template v-if="status">
      <OverviewPanel :status="status" />
      <ListenerCard v-for="l in status.listeners ?? []" :key="l.name" :listener="l" />
      <ClientsTable />
      <EventStream :events="status.events ?? []" />
    </template>
  </main>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { fetchStatus } from './lib/api.js'
import OverviewPanel from './components/OverviewPanel.vue'
import ListenerCard from './components/ListenerCard.vue'
import ClientsTable from './components/ClientsTable.vue'
import EventStream from './components/EventStream.vue'

const status = ref(null)
const error = ref('')
let timer = null

async function poll() {
  try {
    status.value = await fetchStatus()
    error.value = ''
  } catch (e) {
    error.value = String(e.message || e)
  }
}

onMounted(() => {
  poll()
  timer = setInterval(poll, 2000)
})
onBeforeUnmount(() => clearInterval(timer))
</script>

<style>
body { margin: 0; background: #111; color: #ddd; font: 14px/1.5 system-ui, sans-serif; }
main { max-width: 960px; margin: 0 auto; padding: 24px 16px; }
h1 { font-size: 18px; }
.error { color: #f87171; }
</style>
