<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { fade, fly } from 'svelte/transition'
  import { SidebarSectionRenderer } from '$lib/components'
  import type { SidebarSection } from './types'

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null
  export let sections: SidebarSection[] = []
  export let menuWidth = 392

  function onLink(item, type) {
    dispatch('link', { item, type })
  }

  function onClose() {
    dispatch('close')
  }
</script>

<div
  class="tui-sidebar"
  data-test-id={testId}
  style:--top-local="var(--header-height)"
  style:--menu-width-local={menuWidth + 'px'}
>
  <div class="cover" in:fade on:mousedown|preventDefault={onClose} />
  <div class="menu" in:fly={{ x: -menuWidth, duration: 200 }}>
    <div class="content">
      {#each sections as section, i (section.type)}
        <SidebarSectionRenderer {section} {onLink} />
        {#if i < sections.length - 1}
          <div class="hr" />
        {/if}
      {/each}
    </div>
  </div>
</div>

<style>
  .tui-sidebar {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);
  }

  .cover {
    width: 100%;
    height: 100%;
    position: fixed;
    left: 0;
    top: var(--top-local);
    background-color: rgba(40, 41, 51, 0.7);
  }

  .menu {
    position: absolute;
    width: var(--menu-width-local);
    top: var(--top-local);
    left: 0;
    bottom: 0;

    /* padding: 32px 22px; */

    background: white;
  }

  .content {
    overflow-x: hidden;
    overflow-y: auto;
    height: 100%;

    display: flex;
    flex-direction: column;
  }

  .hr {
    width: 100%;
    height: 0;
    border: 1px solid #e0dfe2;
  }
</style>
