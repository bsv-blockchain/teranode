<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { clickOutside } from '../../actions'
  import { mediaSize, MediaSize } from '../../stores/media'
  import { getBreakpointForMediaSize } from '$lib/styles/breakpoints'
  import { SidebarSectionRenderer } from '$lib/components'
  import type { SidebarSection } from '../sidebar/types'

  import Icon from '../icon/index.svelte'
  import Logo from '../logo/index.svelte'

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let open = false
  export let showLinks = true
  export let dataKey = 'path'
  export let hasProducts = false
  export let sections: SidebarSection[] = []
  export let logoSvg

  let pageLinksSection: SidebarSection
  let productLinksSection: SidebarSection

  $: {
    let result = sections.filter((section) => section.type === 'page-links')
    if (result && result.length > 0) {
      pageLinksSection = result[0]
    }

    result = sections.filter((section) => section.type === 'product-links')
    if (result && result.length > 0) {
      productLinksSection = result[0]
    }
  }

  let showMobile = false
  let showMenu = true

  $: {
    showMobile = false
    showMenu = true

    if ($mediaSize <= MediaSize.sm) {
      showMobile = true
      showMenu = false
    }
  }

  $: hasMenu = pageLinksSection && pageLinksSection.items.length > 0
  $: showHeaderMenu = showLinks && hasMenu && showMenu
  $: showIcon = (showMobile && hasMenu) || hasProducts

  function onLink(item, type) {
    dispatch('link', { item, type })
  }

  function onToggle() {
    open = !open
    dispatch('toggle-menu', { open })
  }

  $: iconSelected = open

  let iconRef

  function onToggleContextMenu() {
    onToggle()
  }

  function onCloseContextMenu(e) {
    if (!iconRef.contains(e.explicitOriginalTarget)) {
      onToggleContextMenu()
    }
  }

  $: tabVarStr = `--header-tab-default`

  let cssVars: string[] = []
  $: {
    let tabStates = ['enabled', 'hover']
    cssVars = [
      ...tabStates.reduce(
        (acc, state) => [...acc, `--tab-${state}-bg-color:var(${tabVarStr}-${state}-bg-color)`],
        [] as string[],
      ),
      `--height:var(--header-height)`,
      `--padding:var(--header-size-${getBreakpointForMediaSize($mediaSize)}-padding)`,
      `--bg-color:var(--header-bg-color)`,
      `--logo-color:var(--header-logo-color)`,
      `--logo-width:var(--header-logo-width)`,
      `--logo-height:var(--header-logo-height)`,
      `--logo-margin-right:var(--header-logo-margin-right)`,
      `--menu-icon-size:var(--header-menu-icon-size)`,
      `--menu-icon-wrapper-margin-right:var(--header-menu-icon-wrapper-margin-right)`,
      `--menu-icon-wrapper-size:var(--header-menu-icon-wrapper-size)`,
      `--menu-icon-wrapper-padding:var(--header-menu-icon-wrapper-padding)`,
      `--tab-padding:var(--header-tab-padding)`,
      `--tab-border-radius:var(--header-tab-border-radius)`,
      `--tab-color:var(--header-tab-color)`,
      `--tab-font-size:var(--header-tab-font-size)`,
      `--tab-font-weight:var(--header-tab-font-weight)`,
      `--tab-line-height:var(--header-tab-line-height)`,
      `--tab-letter-spacing:var(--header-tab-letter-spacing)`,
      `--tab-gap:var(--header-tab-gap)`,
    ]
  }
</script>

<div
  data-test-id={testId}
  class={`tui-header${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
>
  <div class="content">
    {#if showIcon}
      <!-- svelte-ignore a11y-click-events-have-key-events -->
      <div
        bind:this={iconRef}
        class="icon"
        class:selected={!showMobile && iconSelected}
        on:click={(e) => (showMobile ? onToggle() : onToggleContextMenu())}
      >
        <Icon
          name={open ? 'close' : hasProducts ? 'product-menu' : 'menu'}
          style={`--width:var(--header-menu-icon-size);--height:var(--header-menu-icon-size)`}
        />
      </div>
    {/if}
    <div class="logo">
      <Logo
        {logoSvg}
        style={`--width:var(--header-logo-width);--height:var(--header-logo-height)`}
      />
    </div>
    {#if showHeaderMenu}
      <div class="links">
        {#each pageLinksSection.items as link, i (link[dataKey])}
          <!-- svelte-ignore a11y-click-events-have-key-events -->
          <div class="tab" on:click={(e) => onLink(link, pageLinksSection.type)}>
            {link.label}
          </div>
        {/each}
      </div>
    {/if}
    <div class="spacer" />
  </div>
  {#if !showMobile && iconSelected}
    <div class="context-menu" use:clickOutside on:outclick={onCloseContextMenu}>
      <div class="title">Switch to</div>
      <div class="hr" />
      <SidebarSectionRenderer section={productLinksSection} renderTitle={false} {onLink} />
    </div>
  {/if}
</div>

<style>
  .tui-header {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);
    display: flex;
    align-items: center;

    position: absolute;
    top: 0;
    right: 0;
    left: 0;

    height: var(--height);
    max-height: var(--height);
    width: 100%;
    padding: var(--padding);
    transition: padding 0.2s linear;

    background-color: var(--bg-color);
  }

  .context-menu {
    position: absolute;
    top: var(--header-height);
    left: 0;
    width: 280px;

    display: flex;
    flex-direction: column;

    background: #ffffff;
    box-shadow: 0px 0px 16px rgba(0, 0, 0, 0.08);
    border-radius: 0px 0px 4px 4px;

    z-index: 200;
  }
  .context-menu .title {
    padding: 24px 32px 18px 32px;
    /* Heading 5 */

    font-family: 'Inter';
    font-style: normal;
    font-weight: 600;
    font-size: 16px;
    line-height: 24px;
    /* identical to box height, or 150% */

    letter-spacing: 0.01em;

    /* Gray/800 */

    color: #282933;
  }
  .context-menu .hr {
    width: 100%;
    height: 0;
    border: 1px solid #e0dfe2;
    margin-bottom: -6px;
  }

  .icon {
    box-sizing: var(--box-sizing);

    width: var(--menu-icon-wrapper-size);
    height: var(--menu-icon-wrapper-size);
    padding: var(--menu-icon-wrapper-padding);

    border-radius: var(--tab-border-radius);
    margin-left: 0;
    margin-right: var(--menu-icon-wrapper-margin-right);

    color: var(--logo-color);
    cursor: pointer;
  }
  .icon:hover,
  .icon.selected {
    margin-left: 0;
    background-color: var(--tab-hover-bg-color);
    cursor: pointer;
  }

  .content {
    box-sizing: var(--box-sizing);

    position: relative;
    display: flex;
    align-items: center;

    width: 100%;
  }

  .logo {
    flex-shrink: 0;
    box-sizing: var(--box-sizing);

    align-self: center;
    width: var(--logo-width);
    height: var(--logo-height);

    margin-left: 0;
    margin-right: var(--logo-margin-right);
    color: var(--logo-color);
  }

  .tab {
    box-sizing: var(--box-sizing);
    padding: var(--tab-padding);

    font-size: var(--tab-font-size);
    font-weight: var(--tab-font-weight);
    line-height: var(--tab-line-height);
    letter-spacing: var(--tab-letter-spacing);
    white-space: nowrap;

    color: var(--tab-color);
    background-color: var(--tab-enabled-bg-color);
    border-radius: var(--tab-border-radius);
  }
  .tab:hover {
    background-color: var(--tab-hover-bg-color);
    cursor: pointer;
  }

  .links {
    display: flex;
    /* flex: 0; */
    gap: var(--tab-gap);
    /* background-color: red; */
  }
  .spacer {
    flex: 1;
  }
  .actions {
    align-self: flex-end;
    display: flex;
    align-items: center;
    gap: 10px;
  }
</style>
