<svelte:options runes={true} />

<script lang="ts">
  import type { Snippet } from 'svelte'
  import { fade } from 'svelte/transition'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import Icon from '../../icon/index.svelte'

  let {
    testId = null,
    position = 'left', // left | right
    minWidth = 60,
    maxWidth = 212,
    offsetTop = '',
    snapBelowHeader = true,
    coverColor = 'rgba(40, 41, 51, 0.7)',
    showCover = false,
    showHeader = false,
    showFooter = false,
    enableCollapse = true,
    collapsed = true,
    onmetrics,
    onclose,
    onheaderSelect,
    header,
    footer,
    children,
  }: {
    testId?: string | undefined | null
    position?: string
    minWidth?: number
    maxWidth?: number
    offsetTop?: string
    snapBelowHeader?: boolean
    coverColor?: string
    showCover?: boolean
    showHeader?: boolean
    showFooter?: boolean
    enableCollapse?: boolean
    collapsed?: boolean
    onmetrics?: (detail: { position: string; width: number }) => void
    onclose?: () => void
    onheaderSelect?: () => void
    header?: Snippet
    footer?: Snippet
    children?: Snippet
  } = $props()

  let collapse = $state(collapsed)
  $effect(() => {
    collapse = collapsed
  })

  function onCollapseClick() {
    collapse = !collapse
  }

  const calcW = $derived(collapse ? minWidth : maxWidth)
  $effect(() => {
    const timeoutId = setTimeout(() => {
      onmetrics?.({ position, width: calcW })
    }, 0)
    return () => clearTimeout(timeoutId)
  })

  const hasHeader = $derived(showHeader && !!header)
  const hasFooter = $derived(showFooter && !!footer)

  function onClose() {
    onclose?.()
  }

  function onHeader() {
    onheaderSelect?.()
  }

  const cssVars = $derived([
    `--width:${$mediaSize <= MediaSize.xs ? '100%' : `${calcW}px`}`,
    `--top:${snapBelowHeader ? `calc(var(--header-height)${offsetTop ? ` + ${offsetTop}` : '0'} )` : `${offsetTop}`}`,
    `--content-top:${hasHeader ? 'var(--header-height)' : '0'}`,
    `--content-bottom:${hasFooter ? 'var(--drawer-footer-height)' : '0'}`,
    `--cover-color:${coverColor}`,
  ])
</script>

{#if showCover}
  <div
    class="cover"
    role="button"
    tabindex="-1"
    in:fade
    onmousedown={(e) => {
      e.preventDefault()
      onClose()
    }}
    onkeydown={(e) => e.key === 'Escape' && onClose()}
    style={`${cssVars.join(';')}`}
  ></div>
{/if}

<div
  class={`tui-drawer${collapse ? ' collapsed' : ''}`}
  data-test-id={testId}
  style={`${cssVars.join(';')}`}
>
  {#if enableCollapse}
    <button
      class="collapse-icon"
      onclick={onCollapseClick}
      type="button"
      aria-label="Toggle drawer"
    >
      <Icon name="chevron-right" class="icon" size={15} />
    </button>
  {/if}

  <div class="container">
    {#key collapsed}
      {#if hasHeader}
        <button class="header" onclick={onHeader} type="button">
          {@render header?.()}
        </button>
      {/if}
    {/key}

    <div class="content">
      {@render children?.()}
    </div>

    {#if hasFooter}
      <button class="footer" onclick={onHeader} type="button">
        {@render footer?.()}
      </button>
    {/if}
  </div>
</div>

<style>
  .tui-drawer {
    display: flex;
    position: fixed;
    top: var(--top);
    left: 0;
    bottom: 0;
    width: var(--width);
    overflow: hidden;
    color: var(--comp-color);
    background: var(--comp-bg-color);
    z-index: 4;
    transition: width var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
    overflow: visible;
  }

  .container {
    width: 100%;
    height: 100%;
    overflow: hidden;
  }

  .tui-drawer .header {
    width: 100%;
    height: var(--header-height);

    padding: 0 16px;
    background: none;
    border: none;
    display: block;
    text-align: left;
    font: inherit;
    color: inherit;
    cursor: pointer;

    display: flex;
    align-items: center;
    /* background: green; */
  }
  .tui-drawer .header:hover {
    cursor: pointer;
  }

  .tui-drawer .content {
    position: absolute;
    width: var(--width);
    transition: width var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
    top: var(--content-top);
    bottom: var(--content-bottom);
    overflow-y: auto;
    overflow-x: hidden;
    /* background: red; */
  }

  .cover {
    width: 100%;
    height: 100%;
    position: fixed;
    left: 0;
    top: var(--top);
    background-color: var(--cover-color);
    z-index: 3;
  }

  .tui-drawer .collapse-icon {
    display: flex;
    align-items: center;
    justify-content: center;
    position: absolute;
    right: -12px;
    top: 18px;
    width: 24px;
    height: 24px;
    border-radius: 12px;
    background-color: transparent;
    color: var(--comp-color);

    z-index: 3;
    box-shadow: 0px 0px 4px transparent;
  }

  .tui-drawer .collapse-icon {
    transform: rotate(180deg);
    transition:
      transform var(--easing-duration, 0.2s) var(--easing-function, ease-in-out),
      background-color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out),
      box-shadow var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
    border: none;
    padding: 0;
    background: transparent;
    cursor: pointer;
  }
  .tui-drawer.collapsed .collapse-icon {
    transform: rotate(0deg);
  }
  .tui-drawer .collapse-icon:hover {
    cursor: pointer;
    background-color: var(--app-subtle-bg-color);
    box-shadow: 0px 0px 4px var(--app-overlay-color);
  }

  .tui-drawer .footer {
    width: 100%;
    background: none;
    border: none;
    display: block;
    text-align: left;
    font: inherit;
    color: inherit;
    cursor: pointer;
    padding: 0;
  }
</style>
