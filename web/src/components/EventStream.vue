<template>
  <div class="panel">
    <h2>Events</h2>
    <table>
      <tbody>
        <tr v-for="e in events" :key="e.time + e.kind">
          <td class="time">{{ time(e.time) }}</td>
          <td class="kind">{{ e.kind }}</td>
          <td>{{ e.detail }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
defineProps({ events: Array })
function time(iso) {
  return new Date(iso).toLocaleTimeString()
}
</script>

<style scoped>
.panel { border: 1px solid #333; border-radius: 6px; padding: 12px 16px; }
h2 { margin: 0 0 8px; font-size: 15px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
td { padding: 3px 8px; border-bottom: 1px solid #222; }
.time { color: #888; white-space: nowrap; }
.kind { font-weight: 600; white-space: nowrap; }
</style>
