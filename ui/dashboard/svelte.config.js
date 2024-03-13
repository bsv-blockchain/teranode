import adapter from '@sveltejs/adapter-static'
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte'
import * as path from 'path'

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),
  kit: {
    adapter: adapter({
      fallback: 'index.html',
    }),
    alias: {
      $internal: path.resolve('./src/internal'),
    },
  },
  vite: {
    build: {
      sourcemap: true,
      terserOptions: {
        keep_fnames: true,
      },
    },
  },
}

export default config
