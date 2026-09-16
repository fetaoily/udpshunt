import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  base: '/ui/',
  plugins: [vue()],
  server: {
    proxy: {
      // Dev convenience: serve the API from a running udpshunt.
      '/status': 'http://127.0.0.1:9155',
      '/metrics': 'http://127.0.0.1:9155'
    }
  }
})
