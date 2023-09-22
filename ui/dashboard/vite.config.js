import { sentrySvelteKit } from "@sentry/sveltekit";
import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig } from 'vite'

export default defineConfig({
  build: {
    sourcemap: true,
    minify: false,
  },
  outDir: '../dist',
  plugins: [sentrySvelteKit({
    sourceMapsUploadOptions: {
      org: "masagi-limited-cd21c228f",
      project: "javascript-sveltekit"
    }
  }), sveltekit()],
  resolve: {
    alias: {
      '@components': '/src/components',
      '@stores': '/src/stores',
      '@routes': '/src/routes',
    },
  },
})
