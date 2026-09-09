<svelte:options runes={true} />

<script lang="ts">
  import Card from '$internal/components/card/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import Table from '$lib/components/table/index.svelte'
  import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'
  import RenderSpanWithTooltip from '$lib/components/table/renderers/render-span-with-tooltip/index.svelte'
  import LegacyAddressCell from './LegacyAddressCell.svelte'

  interface LegacyDetail {
    inbound: boolean
    protocol_version: number
    service_flags: number
    ping_micros: number
    time_offset_secs: number
    starting_height: number
    is_sync_peer: boolean
    time_connected: number
  }

  interface LegacyPeer {
    id: string
    client_name?: string
    height?: number
    network_address?: string
    is_connected?: boolean
    is_banned?: boolean
    bytes_sent?: number
    bytes_received?: number
    legacy?: LegacyDetail
  }

  let { peers = [] as LegacyPeer[] } = $props()

  const connectedCount = $derived(peers.filter((p: LegacyPeer) => p.is_connected).length)

  // The table sorts on the raw row values, so flatten the nested legacy detail
  // up to the row and leave every formatting decision to the cell renderers.
  const rows = $derived(
    peers.map((peer: LegacyPeer) => ({
      id: peer.id,
      address: peer.network_address || String(peer.id || '').replace(/^legacy:/, ''),
      client_name: peer.client_name || '',
      // A payload with transport 'legacy' but no nested block has an unknown
      // direction; do not report it as outbound.
      dir: peer.legacy ? (peer.legacy.inbound ? 'in' : 'out') : '-',
      protocol_version: peer.legacy?.protocol_version ?? 0,
      service_flags: peer.legacy?.service_flags ?? 0,
      ping_micros: peer.legacy?.ping_micros ?? 0,
      time_offset_secs: peer.legacy?.time_offset_secs ?? 0,
      starting_height: peer.legacy?.starting_height ?? 0,
      height: peer.height ?? 0,
      bytes_sent: peer.bytes_sent ?? 0,
      bytes_received: peer.bytes_received ?? 0,
      time_connected: peer.legacy?.time_connected ?? 0,
      is_connected: peer.is_connected ?? false,
      is_banned: peer.is_banned ?? false,
      is_sync_peer: peer.legacy?.is_sync_peer ?? false,
    })),
  )

  const colDefs = [
    { id: 'address', name: 'Address', type: 'string', props: { width: '15%' } },
    { id: 'client_name', name: 'User Agent', type: 'string', props: { width: '11%' } },
    { id: 'dir', name: 'Dir', type: 'string', props: { width: '4%' } },
    { id: 'protocol_version', name: 'Version', type: 'number', props: { width: '6%' } },
    { id: 'service_flags', name: 'Services', type: 'number', props: { width: '6%' } },
    { id: 'ping_micros', name: 'Ping', type: 'number', props: { width: '7%' } },
    { id: 'time_offset_secs', name: 'Offset', type: 'number', props: { width: '6%' } },
    { id: 'starting_height', name: 'Start Height', type: 'number', props: { width: '8%' } },
    { id: 'height', name: 'Height', type: 'number', props: { width: '8%' } },
    { id: 'bytes_sent', name: 'Sent', type: 'number', props: { width: '7%' } },
    { id: 'bytes_received', name: 'Received', type: 'number', props: { width: '7%' } },
    { id: 'time_connected', name: 'Connected', type: 'string', props: { width: '15%' } },
  ]

  function formatBytes(value: number): string {
    if (!value) return '0 B'

    const units = ['B', 'KB', 'MB', 'GB', 'TB']
    let scaled = value
    let unit = 0

    while (scaled >= 1024 && unit < units.length - 1) {
      scaled /= 1024
      unit += 1
    }

    return `${scaled.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`
  }

  function formatPing(micros: number): string {
    if (!micros) return '-'

    return `${(micros / 1000).toFixed(1)} ms`
  }

  function formatOffset(seconds: number): string {
    if (!seconds) return '0s'

    return `${seconds > 0 ? '+' : ''}${seconds}s`
  }

  function formatServiceFlags(flags: number): string {
    if (!flags) return '-'

    return `0x${flags.toString(16)}`
  }

  function formatConnected(unixSeconds: number): string {
    if (!unixSeconds || unixSeconds <= 0) return '-'

    return new Date(unixSeconds * 1000).toLocaleString()
  }

  // The shared table renders rows itself and offers no per-row class hook, so a
  // disconnected peer is dimmed cell by cell instead.
  function cellClass(item, extra = '') {
    const classes = extra ? [extra] : []
    if (!item.is_connected) {
      classes.push('dimmed')
    }

    return classes.join(' ')
  }

  function numCell(item, value: string) {
    return {
      component: RenderSpan,
      props: { value, className: cellClass(item, 'num') },
      value: '',
    }
  }

  const renderCells = {
    address: (idField, item) => ({
      component: LegacyAddressCell,
      props: {
        value: item.address,
        tooltip: item.id,
        isSyncPeer: item.is_sync_peer,
        isBanned: item.is_banned,
        className: cellClass(item),
      },
      value: '',
    }),
    client_name: (idField, item) => ({
      component: RenderSpanWithTooltip,
      props: {
        value: item.client_name || '-',
        tooltip: item.client_name || '',
        className: cellClass(item),
      },
      value: '',
    }),
    dir: (idField, item) => ({
      component: RenderSpan,
      props: { value: item.dir, className: cellClass(item) },
      value: '',
    }),
    protocol_version: (idField, item) =>
      numCell(item, item.protocol_version ? String(item.protocol_version) : '-'),
    service_flags: (idField, item) => numCell(item, formatServiceFlags(item.service_flags)),
    ping_micros: (idField, item) => numCell(item, formatPing(item.ping_micros)),
    time_offset_secs: (idField, item) => numCell(item, formatOffset(item.time_offset_secs)),
    starting_height: (idField, item) => numCell(item, item.starting_height.toLocaleString()),
    height: (idField, item) => numCell(item, item.height.toLocaleString()),
    bytes_sent: (idField, item) => numCell(item, formatBytes(item.bytes_sent)),
    bytes_received: (idField, item) => numCell(item, formatBytes(item.bytes_received)),
    time_connected: (idField, item) => ({
      component: RenderSpan,
      props: { value: formatConnected(item.time_connected), className: cellClass(item) },
      value: '',
    }),
  }
</script>

<Card contentPadding="0" showFooter={false}>
  {#snippet title()}
    <div class="title">
      <Typo variant="title" size="h4" value="Legacy Peers (Bitcoin P2P)" />
    </div>
  {/snippet}
  {#snippet headerTools()}
    <div class="stats">
      <span class="stat-item">
        <span class="stat-label">Connected:</span>
        <span class="stat-value">{connectedCount}</span>
      </span>
      <span class="stat-item">
        <span class="stat-label">Known:</span>
        <span class="stat-value">{peers.length}</span>
      </span>
    </div>
  {/snippet}

  {#if rows.length === 0}
    <div class="no-data">
      <p>No legacy peers</p>
      <p class="sub">No Bitcoin P2P (legacy) peers are registered.</p>
    </div>
  {:else}
    <div class="legacy-table">
      <Table
        name="legacy-peers"
        variant="dynamic"
        idField="id"
        {colDefs}
        data={rows}
        sortEnabled={true}
        filtersEnabled={false}
        paginationEnabled={false}
        pager={false}
        {renderCells}
        getRenderProps={null}
        getRowIconActions={null}
      />
    </div>
  {/if}
</Card>

<style>
  .title {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .stats {
    display: flex;
    gap: 20px;
  }

  .stat-item {
    display: flex;
    align-items: center;
    gap: 6px;
  }

  .stat-label {
    color: var(--comp-label-color);
    font-size: 13px;
  }

  .stat-value {
    color: #1878ff;
    font-size: 14px;
    font-weight: 600;
  }

  .no-data {
    padding: 40px 24px;
    text-align: center;
    color: var(--comp-label-color);
  }

  .no-data .sub {
    font-size: 0.85rem;
    opacity: 0.7;
  }

  .legacy-table :global(.dimmed) {
    opacity: 0.45;
  }

  /* Every legacy column is a short scalar, so a wrapped cell reads as a broken
     row rather than as more information. */
  .legacy-table :global(th),
  .legacy-table :global(td) {
    white-space: nowrap;
  }
</style>
