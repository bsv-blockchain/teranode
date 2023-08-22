import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	outDir: '../dist',
  plugins: [sveltekit()],
	resolve: {
		alias: {
			"@components": "./src/components",
		}
	},
});
