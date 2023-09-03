import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig } from 'vite'

export default defineConfig({
  build: {
    sourcemap: true,
    minify: false,
  },
  outDir: '../dist',
  plugins: [sveltekit()],
  resolve: {
    alias: {
      '@components': '/src/components',
      '@stores': '/src/stores',
      '@routes': '/src/routes',
    },
  },
})
