<svelte:options runes={true} />

<script lang="ts">
  import { onMount, untrack } from 'svelte'
  import type { Snippet } from 'svelte'
  import { get } from 'svelte/store'
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
  import { sm, md, lg, xl } from '$lib/styles/breakpoints'
  import GlobalStyle from '$lib/styles/GlobalStyle.svelte'
  import Spinner from '$lib/components/spinner/index.svelte'
  import i18n from '$internal/i18n'
  import { logos } from '$internal/assets/logos'
  import { init as initLib } from '$lib'

  import { connectToP2PServer } from '$internal/stores/p2pStore'

  let { children }: { children?: Snippet } = $props()

  onMount(() => {
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
    interactive: true,
    interactiveBorder: 20,
    interactiveDebounce: 100,
    appendTo: () => document.body,
  })

  $effect(() => {
    $i18nStore = {
      t: $i18n.t,
      baseKey: '',
    }
  })

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
        path: '/peers',
        label: $i18n.t('page.peers.menu-label'),
      },
      {
        icon: 'icon-network-line',
        iconSelected: 'icon-network-solid',
        path: '/network',
        label: $i18n.t('page.network.menu-label'),
      },
      // {
      //   icon: 'icon-arrow-transfer-line',
      //   iconSelected: 'icon-arrow-transfer-line',
      //   path: '/ancestors',
      //   label: $i18n.t('page.ancestors.menu-label'),
      // },
      {
        icon: 'icon-admin-line',
        iconSelected: 'icon-admin-solid',
        path: '/admin',
        label: $i18n.t('page.admin.menu-label'),
      },
      {
        icon: 'icon-settings-line',
        iconSelected: 'icon-settings-solid',
        path: '/settings',
        label: $i18n.t('page.settings.menu-label'),
      },
      // {
      //   icon: 'icon-bell-line',
      //   iconSelected: 'icon-bell-solid',
      //   path: '/forks',
      //   label: $i18n.t('page.forks.menu-label'),
      // },
    ],
  }

  $effect(() => {
    // Only depend on pathname; reading/writing $pageLinks here would self-trigger the effect.
    const pathname = $page.url.pathname

    untrack(() => {
      const links = get(pageLinks)
      if (links) {
        const items = links.items.map((route) => ({
          ...route,
          selected:
            (pathname === '/' && route.path == '/') ||
            pathname === route.path ||
            pathname.indexOf(`${route.path}/`) === 0,
        }))
        $pageLinks = { ...links, items }
      }
    })
  })

  const queryXl = query(xl)
  const queryLg = query(lg)
  const queryMd = query(md)
  const querySm = query(sm)

  $effect(() => {
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
  })

  const toastOptions = {
    duration: 3000, // duration of progress bar tween to the `next` value
    pausable: true, // pause progress bar tween on mouse hover
    dismissable: true, // allow dismiss with close button
    reversed: false, // insert new toast to bottom of stack
    intro: { y: 192 },
  }
</script>

<GlobalStyle theme={$theme} themeNs={$themeNs}>
  {@render children?.()}
</GlobalStyle>

{#if $spinCount > 0}
  <Spinner
    offsetX={$mediaSize <= MediaSize.sm ? 0 : $contentLeft}
    coverColor="var(--app-cover-bg-color)"
  />
{/if}

<SvelteToast options={toastOptions} />
