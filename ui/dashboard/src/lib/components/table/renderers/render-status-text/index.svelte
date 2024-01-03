<script lang="ts">
  export let value = ''
  export let statusColor = '#54adff'
</script>

{#if value}
  <div class="row" style:--status-color-local={statusColor}>
    <div class="status" />
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
