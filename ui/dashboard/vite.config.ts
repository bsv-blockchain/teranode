import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig, type UserConfigExport } from 'vite'

export default defineConfig({
  build: {
    sourcemap: true,
    minify: false,
  },
  outDir: '../dist',
  plugins: [sveltekit()],
  test: {
    include: ['src/**/*.{test,spec}.{js,ts}'],
  },
} as UserConfigExport)
