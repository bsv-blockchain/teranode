<script lang="ts">
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

  export let testId: string | undefined | null = null
  export let showGlobalToolbar = true
  export let showTools = true
  export let showWarning = true

  import i18n from '$internal/i18n'

  $: t = $i18n.t

  function onLogo() {
    goto('/')
  }

  function onMenuItem(e) {
    const { item, type } = e.detail

    if (type === 'page-links') {
      goto(item.path)
    } else {
      window.open(item.path, '_blank')
    }

    if (showMobileNavbar) {
      showMenu = false
    }
  }

  let showMenu = true
  let showMobileNavbar = false
  $: {
    let newShowMobileNavbar = $mediaSize <= MediaSize.sm
    if (showMobileNavbar !== newShowMobileNavbar) {
      showMenu = false
    }
    showMobileNavbar = newShowMobileNavbar
  }

  function onToggleMenu() {
    showMenu = !showMenu
  }

  function onDrawerMetrics(e) {
    const detail = e.detail

    if (!showMobileNavbar) {
      if (detail.position === 'left') {
        $contentLeft = detail.width
      }
    }
  }

  $: expanded = showMobileNavbar || $contentLeft > 60

  $: showDrawer = (showMobileNavbar && showMenu) || !showMobileNavbar

  function onDrawerClose(e) {
    showMenu = false
  }

  $: menuKey = JSON.stringify($pageLinks.items)
</script>

{#if showMobileNavbar}
  <MobileNavbar offsetTop={'var(--banner-height, 0px)'}>
    <div class="navbar-content">
      <div class="logo-container" on:click={onLogo}>
        <Logo name="teranode" height={28} />
        <Logo name="teranode-text" height={14} />
      </div>
      <div class="icon" on:click={(e) => onToggleMenu()}>
        <AnimMenuIcon open={showDrawer} />
      </div>
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
      <Toolbar style="padding-bottom: 13px;" {showTools} {showWarning} />
    {/if}
    <slot />
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
    on:metrics={onDrawerMetrics}
    on:close={onDrawerClose}
    on:header-select={onLogo}
  >
    <div slot="header" class="logo-container">
      <Logo name="teranode" height={28} />
      {#if expanded}
        <Logo name="teranode-text" height={14} />
      {/if}
    </div>
    {#key menuKey}
      <Menu
        collapsed={!expanded}
        idField="path"
        data={$pageLinks.items}
        on:select={(e) => onMenuItem({ detail: { item: e.detail.item, type: 'page-links' } })}
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
  }

  .logo-container {
    display: flex;
    align-items: center;
    gap: 14px;
    cursor: pointer;
  }
</style>
