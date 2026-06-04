import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    port: 5173,
    proxy: {
      // In dev, forward the JSON API + health endpoints to the running broker's
      // metrics port. In production the SPA is served same-origin by the Go binary.
      '/api': {
        target: 'http://127.0.0.1:7101',
        changeOrigin: true
      },
      '/healthz': {
        target: 'http://127.0.0.1:7101',
        changeOrigin: true
      },
      '/metrics': {
        target: 'http://127.0.0.1:7101',
        changeOrigin: true
      }
    }
  }
});
