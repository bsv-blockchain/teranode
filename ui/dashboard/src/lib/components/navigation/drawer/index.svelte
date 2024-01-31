<script lang="ts">
  import { onDestroy } from 'svelte'
  import { createEventDispatcher } from 'svelte'
  import { fade } from 'svelte/transition'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import Icon from '../../icon/index.svelte'

  export let testId: string | undefined | null = null
  export let position = 'left' // left | right
  export let minWidth = 60
  export let maxWidth = 212
  export let snapBelowHeader = true
  export let coverColor = 'rgba(40, 41, 51, 0.7)'
  export let showCover = false
  export let showHeader = false
  export let showFooter = false

  export let enableCollapse = true
  export let collapsed = true

  const dispatch = createEventDispatcher()

  let collapse = collapsed
  $: {
    collapse = collapsed
  }
  function onCollapseClick() {
    collapse = !collapse
  }

  let timeoutId: any = null
  let calcW = collapse ? minWidth : maxWidth
  $: {
    calcW = collapse ? minWidth : maxWidth

    timeoutId = setTimeout(() => {
      dispatch('metrics', { position, width: calcW })
    }, 0)
  }

  $: hasHeader = showHeader && $$slots.header
  $: hasFooter = showFooter && $$slots.footer

  function onClose() {
    dispatch('close', {})
  }

  function onHeader() {
    dispatch('header-select', {})
  }

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--width:${$mediaSize <= MediaSize.xs ? '100%' : `${calcW}px`}`,
      `--top:${snapBelowHeader ? 'var(--header-height)' : '0'}`,
      `--content-top:${hasHeader ? 'var(--header-height)' : '0'}`,
      `--content-bottom:${hasFooter ? 'var(--drawer-footer-height)' : '0'}`,
      `--cover-color:${coverColor}`,
    ]
  }

  onDestroy(() => {
    if (timeoutId) {
      clearTimeout(timeoutId)
    }
  })
</script>

{#if showCover}
  <div class="cover" in:fade on:mousedown|preventDefault={onClose} style={`${cssVars.join(';')}`} />
{/if}

<div
  class={`tui-drawer${collapse ? ' collapsed' : ''}`}
  data-test-id={testId}
  style={`${cssVars.join(';')}`}
>
  {#if enableCollapse}
    <div class="collapse-icon" on:click={onCollapseClick}>
      <Icon name="chevron-right" class="icon" size={15} />
    </div>
  {/if}

  <div class="container">
    {#key collapsed}
      {#if hasHeader}
        <div class="header" on:click={onHeader}>
          <slot name="header" />
        </div>
      {/if}
    {/key}

    <div class="content">
      <slot />
    </div>

    {#if hasFooter}
      <div class="footer" on:click={onHeader}>
        <slot name="footer" />
      </div>
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
    background-color: rgba(255, 255, 255, 0);
    color: var(--comp-color);

    z-index: 3;
    box-shadow: 0px 0px 4px rgba(0, 0, 0, 0);
  }

  .tui-drawer .collapse-icon {
    transform: rotate(180deg);
    transition:
      transform var(--easing-duration, 0.2s) var(--easing-function, ease-in-out),
      background-color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out),
      box-shadow var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
  }
  .tui-drawer.collapsed .collapse-icon {
    transform: rotate(0deg);
  }
  .tui-drawer .collapse-icon:hover {
    cursor: pointer;
    background-color: rgba(255, 255, 255, 0.05);
    box-shadow: 0px 0px 4px rgba(0, 0, 0, 0.08);
  }
</style>
