<svelte:options runes={true} />

<script lang="ts">
  import type { Snippet } from 'svelte'
  import { goto } from '$app/navigation'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { pageLinks, contentLeft } from '../../../../stores/nav'
  import MobileNavbar from '$lib/components/navigation/mobile-navbar/index.svelte'
  import Drawer from '$lib/components/navigation/drawer/index.svelte'
  import Logo from '$lib/components/logo/index.svelte'
  import Menu from '$lib/components/navigation/menu/index.svelte'
  import Toolbar from '$internal/components/toolbar/index.svelte'
  import Footer from '$internal/components/footer/index.svelte'
  // import Banner from '$internal/components/banner/index.svelte'
  import AnimMenuIcon from '$internal/components/anim-menu-icon/index.svelte'
  import ContentMenu from '../../content/menu/index.svelte'
  import i18n from '$internal/i18n'

  let {
    testId = null,
    showGlobalToolbar = true,
    showTools = true,
    children,
  }: {
    testId?: string | undefined | null
    showGlobalToolbar?: boolean
    showTools?: boolean
    children?: Snippet
  } = $props()

  const t = $derived($i18n.t)

  function onLogo() {
    goto('/')
  }

  function onMenuItem(detail) {
    const { item, type } = detail

    if (type === 'page-links') {
      goto(item.path)
    } else {
      window.open(item.path, '_blank')
    }

    if (showMobileNavbar) {
      showMenu = false
    }
  }

  let showMenu = $state(true)
  let showMobileNavbar = $state(false)
  $effect(() => {
    let newShowMobileNavbar = $mediaSize <= MediaSize.sm
    if (showMobileNavbar !== newShowMobileNavbar) {
      showMenu = false
    }
    showMobileNavbar = newShowMobileNavbar
  })

  function onToggleMenu() {
    showMenu = !showMenu
  }

  function onDrawerMetrics(detail) {
    if (!showMobileNavbar) {
      if (detail.position === 'left') {
        $contentLeft = detail.width
      }
    }
  }

  const expanded = $derived(showMobileNavbar || $contentLeft > 60)

  const showDrawer = $derived((showMobileNavbar && showMenu) || !showMobileNavbar)

  function onDrawerClose() {
    showMenu = false
  }

  const menuKey = $derived(JSON.stringify($pageLinks.items))
</script>

{#if showMobileNavbar}
  <MobileNavbar offsetTop={'var(--banner-height, 0px)'}>
    <div class="navbar-content">
      <button class="logo-container" onclick={onLogo} type="button">
        <Logo name="teranode" height={28} />
        <Logo name="teranode-text" height={14} />
      </button>
      <button class="icon" onclick={(e) => onToggleMenu()} type="button">
        <AnimMenuIcon open={showDrawer} />
      </button>
    </div>
  </MobileNavbar>
{/if}

<!-- <Banner text={t('global.warning')} /> -->

<div
  class="content-container"
  data-test-id={testId}
  style:--offset-top={showMobileNavbar
    ? `calc(var(--header-height) + var(--banner-height, 0px))`
    : 'var(--banner-height, 0px)'}
  style:--offset-left={showMobileNavbar ? '0px' : `${$contentLeft}px`}
>
  <ContentMenu>
    {#if showGlobalToolbar}
      <Toolbar style="padding-bottom: 13px;" {showTools} />
    {/if}
    {@render children?.()}
  </ContentMenu>

  <Footer />
</div>

{#if showDrawer}
  <Drawer
    snapBelowHeader={showMobileNavbar}
    enableCollapse={!showMobileNavbar}
    minWidth={60}
    maxWidth={212}
    offsetTop={'var(--banner-height, 0px)'}
    collapsed={!expanded}
    showCover={showMobileNavbar}
    showHeader={!showMobileNavbar}
    coverColor="var(--app-cover-bg-color)"
    onmetrics={onDrawerMetrics}
    onclose={onDrawerClose}
    onheaderSelect={onLogo}
  >
    {#snippet header()}
      <div class="logo-container">
        <Logo name="teranode" height={28} />
        {#if expanded}
          <Logo name="teranode-text" height={14} />
        {/if}
      </div>
    {/snippet}
    {#key menuKey}
      <Menu
        collapsed={!expanded}
        idField="path"
        data={$pageLinks.items}
        onselect={(e) => onMenuItem({ item: e.item, type: 'page-links' })}
      />
    {/key}
  </Drawer>
{/if}

<style>
  .navbar-content {
    display: flex;
    align-items: center;
    justify-content: space-between;

    width: 100%;
    padding: 0 16px;
  }
  .navbar-content .icon {
    cursor: pointer;
    background: rgba(0, 0, 0, 0);
  }

  .content-container {
    position: absolute;
    top: var(--offset-top);
    left: var(--offset-left);
    bottom: 0;
    width: calc(100% - var(--offset-left));
    overflow-x: hidden;
    overflow-y: auto;
    transition: top var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
    background: var(--app-bg-color);
  }

  .logo-container {
    display: flex;
    align-items: center;
    gap: 14px;
    cursor: pointer;
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    color: inherit;
  }

  .icon {
    background: none;
    border: none;
    padding: 0;
    cursor: pointer;
  }
</style>
