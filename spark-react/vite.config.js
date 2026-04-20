import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/ui/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      '/query': 'http://127.0.0.1:8000',
      '/history': 'http://127.0.0.1:8000',
      '/status': 'http://127.0.0.1:8000',
      '/ingest': 'http://127.0.0.1:8000',
      '/highlight': 'http://127.0.0.1:8000',
      '/view-source': 'http://127.0.0.1:8000',
      '/document': 'http://127.0.0.1:8000',
      '/feedback': 'http://127.0.0.1:8000',
      '/admin': 'http://127.0.0.1:8000',
    }
  }
})
