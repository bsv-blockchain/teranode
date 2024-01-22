import { sentrySvelteKit } from '@sentry/sveltekit'
import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig, type UserConfigExport } from 'vite'

// See:
// - https://github.com/getsentry/sentry-javascript/tree/master/packages/sveltekit
// - https://github.com/getsentry/sentry-javascript/tree/master/packages/sveltekit#uploading-source-maps
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
