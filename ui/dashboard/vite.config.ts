import { sentrySvelteKit } from '@sentry/sveltekit'
import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig } from 'vitest/config'
import type { UserConfigExport } from 'vitest/config'

export default defineConfig({
  build: {
    sourcemap: true,
    minify: false,
  },
  outDir: '../dist',
  plugins: [
    sentrySvelteKit({
      sourceMapsUploadOptions: {
        org: 'masagi-limited-cd21c228f',
        project: 'javascript-sveltekit',
      },
    }),
    sveltekit(),
  ],
  // resolve: {
  //   alias: {
  //     '@components': '/src/lib/components',
  //     '@stores': '/src/stores',
  //     '@routes': '/src/routes',
  //     '@utils': '/src/utils',
  //   },
  // },
  test: {
    include: ['src/**/*.{test,spec}.{js,ts}'],
  },
} as UserConfigExport)
