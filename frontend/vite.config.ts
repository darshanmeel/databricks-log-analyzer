import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// API target for the dev proxy; override with DBX_API_URL=http://127.0.0.1:9000 npm run dev
const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env ?? {};
const apiTarget = env.DBX_API_URL || 'http://127.0.0.1:8765';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': { target: apiTarget, changeOrigin: true },
    },
  },
  build: { outDir: 'dist', emptyOutDir: true, chunkSizeWarningLimit: 1200 },
});
