<script lang="ts">
  import { onMount } from 'svelte'
  import { page } from '$app/stores'
  import { SvelteToast } from '@zerodevx/svelte-toast'
  import { createTippy } from '$lib/actions/tooltip'
  import { pageLinks, spinCount, contentLeft } from '$internal/stores/nav'
  import { query } from '$lib/actions'
  import {
    mediaSize,
    MediaSize,
    theme,
    themeNs,
    injectedLogos,
    i18n as i18nStore,
    tippy,
  } from '$lib/stores/media'
  import { dark as darkTheme } from '$internal/styles/themes/dark'
  import { sm, md, lg, xl } from '$lib/styles/breakpoints'
  import GlobalStyle from '$lib/styles/GlobalStyle.svelte'
  import Spinner from '$lib/components/spinner/index.svelte'
  import i18n from '$internal/i18n'
  import { logos } from '$internal/assets/logos'
  import { init as initLib } from '$lib'

  import { connectToP2PServer } from '$internal/stores/p2pStore'

  onMount(async () => {
    connectToP2PServer()
  })

  // web fonts
  import '$internal/assets/css/JetBrainsMono.css'
  import '$internal/assets/css/Satoshi.css'

  // tippy
  import 'tippy.js/dist/tippy.css'
  import 'tippy.js/animations/perspective-subtle.css'

  $tippy = createTippy({
    animation: 'perspective-subtle',
    arrow: false,
  })

  $: {
    $i18nStore = {
      t: $i18n.t,
      baseKey: '',
    }
  }

  // inject assets
  initLib({
    useLibIcons: false,
    iconNameOverrides: {
      'chevron-right': 'icon-chevron-right-line',
      'chevron-down': 'icon-chevron-down-line',
      'chevron-up': 'icon-chevron-up-line',
    },
  })
  $injectedLogos = logos

  $theme = 'dark'
  // $theme = 'light'

  let customThemeProps = {}

  $: {
    switch ($theme) {
      case 'dark':
        customThemeProps = darkTheme
        break
      case 'light':
        customThemeProps = {}
        break
      default:
    }
  }

  $pageLinks = {
    type: 'page-links',
    variant: 'normal',
    items: [
      {
        icon: 'icon-home-line',
        iconSelected: 'icon-home-solid',
        path: '/',
        label: $i18n.t('page.home.menu-label'),
      },
      {
        icon: 'icon-binoculars-line',
        iconSelected: 'icon-binoculars-solid',
        path: '/viewer',
        label: $i18n.t('page.viewer.menu-label'),
      },
      {
        icon: 'icon-p2p-line',
        iconSelected: 'icon-p2p-solid',
        path: '/p2p',
        label: $i18n.t('page.p2p.menu-label'),
      },
      {
        icon: 'icon-network-line',
        iconSelected: 'icon-network-solid',
        path: '/network',
        label: $i18n.t('page.network.menu-label'),
      },
      // {
      //   icon: 'icon-bell-line',
      //   iconSelected: 'icon-bell-solid',
      //   path: '/notifications',
      //   label: $i18n.t('page.notifications.menu-label'),
      // },
    ],
  }

  $: {
    const pathname = $page.url.pathname

    let items: any[] = []

    if ($pageLinks) {
      items = $pageLinks.items.map((route) => ({
        ...route,
        selected:
          (pathname === '/' && route.path == '/') || pathname.indexOf(`${route.path}/`) === 0,
      }))
      $pageLinks.items = items
    }
  }

  $: queryXl = query(xl)
  $: queryLg = query(lg)
  $: queryMd = query(md)
  $: querySm = query(sm)

  $: {
    if ($queryXl) {
      $mediaSize = MediaSize.xl
    } else if ($queryLg) {
      $mediaSize = MediaSize.lg
    } else if ($queryMd) {
      $mediaSize = MediaSize.md
    } else if ($querySm) {
      $mediaSize = MediaSize.sm
    } else {
      $mediaSize = MediaSize.xs
    }
  }

  const toastOptions = {
    duration: 3000, // duration of progress bar tween to the `next` value
    pausable: true, // pause progress bar tween on mouse hover
    dismissable: true, // allow dismiss with close button
    reversed: false, // insert new toast to bottom of stack
    intro: { y: 192 },
  }
</script>

<GlobalStyle theme={$theme} themeNs={$themeNs} {customThemeProps}>
  <slot />
</GlobalStyle>

{#if $spinCount > 0}
  <Spinner offsetX={$contentLeft} coverColor="var(--app-cover-bg-color)" />
{/if}

<SvelteToast options={toastOptions} />
