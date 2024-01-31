import { sveltekit } from '@sveltejs/kit/vite'
import { defineConfig, type UserConfigExport } from 'vite'
// import { visualizer } from 'rollup-plugin-visualizer'
import svg from '@poppanator/sveltekit-svg'

export default defineConfig({
  build: {
    sourcemap: true,
    minify: true,
    rollupOptions: {
      output: {
        manualChunks: (id) => {
          if (id.includes('node_modules')) {
            if (id.includes('zrender')) {
              return 'vendor_zrender'
            } else if (id.includes('echarts')) {
              return 'vendor_echarts'
            }
            // return 'vendor' // all other package goes here
          }
          if (id.includes('.svg')) {
            if (
              [
                'icon-search',
                'icon-home',
                'icon-binoculars',
                'icon-p2p',
                'icon-network',
                'icon-chevron',
              ].includes(id)
            ) {
              return 'icons_nav'
            } else if (
              ['icon-cube', 'icon-arrow-transfer', 'icon-scale', 'icon-status'].includes(id)
            ) {
              return 'icons_nav'
            } else if (
              ['check-circle', 'exclamation-circle', 'exclamation', 'information-circle'].includes(
                id,
              )
            ) {
              return 'icons_toast'
            }
            return 'icons_rest'
          }
        },
      },
    },
  },
  outDir: '../dist',
  plugins: [
    sveltekit(),
    // visualizer({
    //   emitFile: true,
    //   filename: 'stats.html',
    // }),
    svg({
      includePaths: ['./src/internal/assets/icons/', './src/lib/assets/icons/'],
      svgoOptions: {
        multipass: true,
        plugins: [
          'removeDimensions',
          {
            name: 'convertColors',
            params: {
              currentColor: true,
            },
          },
        ],
      },
    }),
  ],
  test: {
    include: ['src/**/*.{test,spec}.{js,ts}'],
  },
} as UserConfigExport)
