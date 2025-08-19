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
    background: var(--app-bg-color);
    color: var(--app-color);
  }
</style>
