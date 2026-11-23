<svelte:options runes={true} />

<script lang="ts">
  let {
    value = '',
    tooltip = '',
    isSyncPeer = false,
    isBanned = false,
    className = '',
  }: {
    value?: string
    tooltip?: string
    isSyncPeer?: boolean
    isBanned?: boolean
    className?: string
  } = $props()
</script>

<span class="legacy-address {className}" title={tooltip || value}>
  <span class="addr">{value}</span>
  {#if isSyncPeer}
    <span class="badge sync">SYNC</span>
  {/if}
  {#if isBanned}
    <span class="badge banned">BANNED</span>
  {/if}
</span>

<style>
  .legacy-address {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
  }

  .addr {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .badge {
    display: inline-block;
    flex: none;
    padding: 3px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.06em;
    vertical-align: middle;
  }

  /* Solid fill, not a tint: the sync peer is the one row an operator looks for,
     so the badge has to carry across a full-width table. */
  .badge.sync {
    background: #2e9e4f;
    color: #ffffff;
    box-shadow: 0 0 0 1px rgba(46, 158, 79, 0.45);
  }

  .badge.banned {
    background: #d93025;
    color: #ffffff;
    box-shadow: 0 0 0 1px rgba(217, 48, 37, 0.45);
  }
</style>
