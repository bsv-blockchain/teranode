# Dashboard

```txt
SAO - Here are the steps I took to create this project as a fully static site:

npm create svelte@4 dashboard
cd dashboard
npm install
npm install -D @sveltejs/adapter-static

Change the following in svelte.config.js:
import adapter from '@sveltejs/adapter-static';

Change the following in package.json:
"scripts": {
    "build": "svelte-kit build",
    "dev": "svelte-kit dev",
    "preview": "svelte-kit preview",
    "start": "svelte-kit start"
  },

Useful site: https://www.philkruft.dev/blog/how-to-build-a-static-sveltekit-site/

npm run build
npx serve build
```

## create-svelte

Everything you need to build a Svelte project, powered by [`create-svelte`](https://github.com/sveltejs/kit/tree/master/packages/create-svelte).

## Creating a project

If you're seeing this, you've probably already done this step. Congrats!

```bash
# create a new project in the current directory
npm create svelte@latest

# create a new project in my-app
npm create svelte@latest my-app
```

## Developing

Once you've created a project and installed dependencies with `npm install` (or `pnpm install` or `yarn`), start a development server:

```bash
npm run dev

# or start the server and open the app in a new browser tab
npm run dev -- --open
```

## Building

To create a production version of your app:

```bash
npm run build
```

You can preview the production build with `npm run preview`.

> To deploy your app, you may need to install an [adapter](https://kit.svelte.dev/docs/adapters) for your target environment.
