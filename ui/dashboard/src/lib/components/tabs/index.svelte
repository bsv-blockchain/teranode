<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { ComponentSize } from '$lib/styles/types'
  import type { InputSizeType } from '$lib/styles/types/input'

  import { Tab } from '$lib/components'

  export let name: string
  export let items: { label: string; value: any; icon?: string; iconAfter?: string }[] = []
  export let value: any
  export let size: InputSizeType = ComponentSize.large

  let type = 'select'

  const dispatch = createEventDispatcher()

  function onClick(itemIndex: number) {
    value = items[itemIndex].value
    dispatch('change', { name, type, value })
  }

  function onKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    switch (keyCode) {
      case 'Enter':
        e.preventDefault()
        if (document.activeElement && document.activeElement.classList.contains('tui-tab')) {
          ;(document.activeElement as any).click()
        }
        return false
    }
  }
</script>

<div class="tui-tabs" on:keydown={onKeyDown}>
  {#each items as item, i (item.value)}
    <Tab
      icon={item.icon}
      iconAfter={item.iconAfter}
      selected={value == item.value}
      {size}
      on:click={(_) => onClick(i)}
    >
      {item.label}
    </Tab>
  {/each}
</div>

<style>
  .tui-tabs {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    display: flex;
    gap: 4px;
  }
</style>
