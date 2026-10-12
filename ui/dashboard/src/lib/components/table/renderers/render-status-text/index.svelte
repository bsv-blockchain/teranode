<svelte:options runes={true} />

<script lang="ts">
  let { value = '', statusColor = '#54adff' }: { value?: string; statusColor?: string } = $props()
</script>

{#if value}
  <div class="row" style:--status-color-local={statusColor}>
    <div class="status"></div>
    <div class="value">{value}</div>
  </div>
{:else}
  ''
{/if}

<style>
  .row {
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .status {
    width: 6px;
    height: 6px;
    border-radius: 3px;
    background-color: var(--status-color-local);
  }
</style>
