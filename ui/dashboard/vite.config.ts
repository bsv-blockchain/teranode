import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig, type UserConfigExport } from 'vite'
import { visualizer } from 'rollup-plugin-visualizer'

export default defineConfig({
  build: {
    sourcemap: true,
    minify: true,
  },
  outDir: '../dist',
  plugins: [
    sveltekit(),
    visualizer({
      emitFile: true,
      filename: 'stats.html',
    }),
  ],
  test: {
    include: ['src/**/*.{test,spec}.{js,ts}'],
  },
} as UserConfigExport)
