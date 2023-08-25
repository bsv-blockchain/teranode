import adapter from '@sveltejs/adapter-static';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	kit: {
		adapter: adapter()
	},
	vite: {
		build: {
			sourcemap: true,
			terserOptions: {
				keep_fnames: true
			}
		}

	}
};

export default config;
