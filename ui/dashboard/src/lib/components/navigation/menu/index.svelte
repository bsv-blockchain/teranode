<svelte:options runes={true} />

<script lang="ts">
  import MenuItem from '../menu-item/index.svelte'

  let {
    data = [],
    collapsed = false,
    idField = 'id',
    onselect,
  }: {
    data?: any[]
    collapsed?: boolean
    idField?: string
    onselect?: (detail: { item: any }) => void
  } = $props()

  function onMenuItem(item: any) {
    onselect?.({ item })
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
      onclick={() => onMenuItem(item)}
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
