<script lang="ts">
  import { theme as themeStore } from '$lib/stores/media'
  import deepmerge from 'deepmerge'

  import { defaults } from './themes'
  import { dark as darkTheme } from '$internal/styles/themes/dark'
  import { light as lightTheme } from '$internal/styles/themes/light'
  import { setCSSVariables } from './utils/css'

  // web fonts
  import './css/inter.css'

  export let theme
  export let themeNs
  export let customThemeProps: any = {}

  const themes = {
    dark: darkTheme,
    light: lightTheme,
  }

  let themeProps = {}

  $: {
    themeProps = themes[theme] ?? lightTheme

    if (theme !== null) {
      themeProps = deepmerge(themeProps, customThemeProps)
      $themeStore = theme
    }

    setCSSVariables(deepmerge(defaults, themeProps), themeNs)

    if (typeof document !== 'undefined' && (theme === 'light' || theme === 'dark')) {
      document.documentElement.setAttribute('data-theme', theme)
    }
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
    /* Override svelte-toast library defaults (white text, dark bg) so the
       toast uses the app's colour scheme instead. */
    --toastColor: var(--app-color);
    --toastBackground: var(--comp-bg-color, var(--app-bg-color));

    /* Message box border colors */
    --msgbox-default-border-color: #3e4451;
    --msgbox-block-border-color: #4caf50;
    --msgbox-mining_on-border-color: #2196f3;
    --msgbox-miningon-border-color: #2196f3;
    --msgbox-subtree-border-color: #ff9800;
    --msgbox-ping-border-color: #9c27b0;
    --msgbox-node_status-border-color: #00bcd4;
    --msgbox-getminingcandidate-border-color: #795548;
    --msgbox-tx-border-color: #ff5722;
    --msgbox-transaction-border-color: #ff5722;
    --msgbox-inv-border-color: #e91e63;
    --msgbox-getdata-border-color: #673ab7;
    --msgbox-getheaders-border-color: #3f51b5;
    --msgbox-headers-border-color: #009688;
    --msgbox-version-border-color: #607d8b;
    --msgbox-verack-border-color: #8bc34a;
    --msgbox-addr-border-color: #ffc107;
    --msgbox-getblocks-border-color: #cddc39;

    /* Message box background and text colors */
    --msgbox-bg-color: rgba(255, 255, 255, 0.04);
    --msgbox-label-color: rgba(255, 255, 255, 0.66);
    --msgbox-value-color: rgba(255, 255, 255, 0.88);
  }
  :global(._toastBtn) {
    position: absolute;
    z-index: 100;
    color: #282933;
    top: 19px;
    right: 9px;
  }
  :global(._toastItem) {
    /* Use component background (white in light, dark in dark) so the toast
       stands out from the page background (#F0F2F5 in light mode). */
    background: var(--comp-bg-color, var(--app-bg-color));
    color: var(--app-color);
    box-shadow: 0 2px 12px rgba(0, 0, 0, 0.12);
  }
</style>
