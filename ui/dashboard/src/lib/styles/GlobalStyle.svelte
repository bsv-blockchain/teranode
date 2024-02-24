<script lang="ts">
  import { theme as themeStore } from '$lib/stores/media'
  import deepmerge from 'deepmerge'

  import { defaults, light } from './themes'
  import { setCSSVariables } from './utils/css'

  // web fonts
  import './css/inter.css'

  export let theme
  export let themeNs
  export let customThemeProps: any = {}

  const themes = {
    light: light,
  }

  let themeProps = {}

  $: {
    themeProps = {}

    if (theme !== null) {
      if (themes[theme]) {
        switch (theme) {
          case 'light':
            themeProps = light
            break
        }
      }
      themeProps = deepmerge(themeProps, customThemeProps)

      $themeStore = theme
    } else {
      themeProps = light
    }

    setCSSVariables(deepmerge(defaults, themeProps), themeNs)
  }
</script>

<slot />

<style>
  :global(:root) {
    --box-sizing: var(--app-box-sizing);
    --font-family: var(--app-font-family);
    --font-family-mono: var(--app-mono-font-family);
    background: var(--app-bg-color);
    color: var(--app-color);
  }

  :global(body) {
    box-sizing: var(--box-sizing);
    font-family: var(--font-family);
    margin: 0;
    padding: 0;
  }

  :global(a),
  :global(a:hover),
  :global(a:focus),
  :global(a:visited),
  :global(a:active) {
    text-decoration: none;
    color: var(--link-default-enabled-color);
  }
  :global(a:hover) {
    text-decoration: underline;
  }
  :global(sup) {
    vertical-align: top;
    position: relative;
    top: -0.5em;
  }

  :root {
    --toastContainerTop: auto;
    --toastContainerRight: auto;
    --toastContainerBottom: 30px;
    --toastContainerLeft: 30px;
    --toastBorderRadius: var(--toast-border-radius);
    --toastMsgPadding: 0;
    --toastWidth: var(--toast-width);
    --toastBarHeight: 3px;
    --toastBarBackground: var(--toast-bar-bg-color, rgba(0, 0, 0, 0.1));
  }
  :global(._toastBtn) {
    position: absolute;
    z-index: 100;
    color: #282933;
    top: 19px;
    right: 9px;
  }
  :global(._toastItem) {
    background: var(--app-bg-color);
    color: var(--app-color);
  }
</style>
