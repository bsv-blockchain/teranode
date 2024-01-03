<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import MenuItem from '../menu-item/index.svelte'

  const dispatch = createEventDispatcher()

  export let data: any[] = []
  export let collapsed = false
  export let idField = 'id'

  function onMenuItem(item: any) {
    dispatch('select', { item })
  }
</script>

<div class="tui-menu">
  {#each data as item (item[idField])}
    <MenuItem
      icon={item.icon}
      iconSelected={item.iconSelected}
      label={item.label}
      selected={item.selected}
      {collapsed}
      on:click={(e) => onMenuItem(item)}
    />
  {/each}
</div>

<style>
  .tui-menu {
    display: flex;
    flex-direction: column;
    gap: 10px;
    padding: 10px;

    color: var(--comp-color);
  }
</style>
