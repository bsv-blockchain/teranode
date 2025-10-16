<script lang="ts">
  import { onMount, onDestroy } from 'svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import Table from '$lib/components/table/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import Icon from '$lib/components/icon/index.svelte'
  import { Button } from '$lib/components'
  import i18n from '$internal/i18n'
  import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'
  import RenderSpanWithTooltip from '$lib/components/table/renderers/render-span-with-tooltip/index.svelte'

  $: t = $i18n.t

  const pageKey = 'page.peers'

  interface PeerData {
    id: string
    height: number
    block_hash: string
    data_hub_url: string
    is_healthy: boolean
    health_duration_ms: number
    last_health_check: number
    ban_score: number
    is_banned: boolean
    is_connected: boolean
    connected_at: number
    bytes_received: number
    last_block_time: number
    last_message_time: number
    url_responsive: boolean
    last_url_check: number
  }

  let data: PeerData[] = []
  let isLoading = false
  let error: string | null = null
  let refreshInterval: number | null = null

  // Fetch peer data from the API
  async function fetchPeers() {
    isLoading = true
    error = null

    try {
      const response = await fetch('/api/p2p/peers')

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const result = await response.json()

      if (result.error) {
        throw new Error(result.error)
      }

      data = result.peers || []
    } catch (err) {
      console.error('Failed to fetch peers:', err)
      error = err instanceof Error ? err.message : 'Unknown error'
      data = []
    } finally {
      isLoading = false
    }
  }

  // Format bytes to human-readable format
  function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i]
  }

  // Format timestamp to relative time
  function formatRelativeTime(timestamp: number): string {
    if (timestamp === 0) return 'Never'

    const now = Math.floor(Date.now() / 1000)
    const diff = now - timestamp

    if (diff < 60) return `${diff}s ago`
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
    return `${Math.floor(diff / 86400)}d ago`
  }

  // Format duration in milliseconds
  function formatDuration(ms: number): string {
    if (ms === 0) return '0ms'
    if (ms < 1000) return `${ms}ms`
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
    if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`
    return `${(ms / 3600000).toFixed(1)}h`
  }

  // Column definitions for the table
  function getColDefs() {
    return [
      {
        id: 'id',
        name: 'Peer ID',
        type: 'string',
        props: {
          width: '15%',
        },
      },
      {
        id: 'is_connected',
        name: 'Status',
        type: 'string',
        props: {
          width: '10%',
        },
      },
      {
        id: 'height',
        name: 'Height',
        type: 'number',
        props: {
          width: '10%',
        },
      },
      {
        id: 'block_hash',
        name: 'Block Hash',
        type: 'string',
        props: {
          width: '15%',
        },
      },
      {
        id: 'data_hub_url',
        name: 'DataHub URL',
        type: 'string',
        props: {
          width: '15%',
        },
      },
      {
        id: 'ban_score',
        name: 'Ban Score',
        type: 'number',
        props: {
          width: '10%',
        },
      },
      {
        id: 'bytes_received',
        name: 'Data Received',
        type: 'number',
        props: {
          width: '12%',
        },
      },
      {
        id: 'last_message_time',
        name: 'Last Message',
        type: 'number',
        props: {
          width: '13%',
        },
      },
    ]
  }

  $: colDefs = getColDefs()

  // Custom render functions
  const renderCells = {
    id: (idField, item, colId) => {
      const value = item[colId] || ''
      const shortId = value.length > 16 ? `${value.slice(0, 8)}...${value.slice(-8)}` : value
      return {
        component: RenderSpanWithTooltip,
        props: {
          value: shortId,
          tooltip: value,
          className: 'peer-id',
        },
        value: '',
      }
    },
    is_connected: (idField, item, colId) => {
      const isConnected = item.is_connected
      const isHealthy = item.is_healthy
      const isBanned = item.is_banned
      const urlResponsive = item.url_responsive

      let status = ''
      let className = ''

      if (isBanned) {
        status = 'Banned'
        className = 'status-banned'
      } else if (!isConnected) {
        status = 'Disconnected'
        className = 'status-disconnected'
      } else if (!isHealthy) {
        status = 'Unhealthy'
        className = 'status-unhealthy'
      } else if (!urlResponsive) {
        status = 'URL Down'
        className = 'status-url-down'
      } else {
        status = 'Healthy'
        className = 'status-healthy'
      }

      return {
        component: RenderSpan,
        props: {
          value: status,
          className: className,
        },
        value: '',
      }
    },
    height: (idField, item, colId) => {
      const value = item[colId]
      return {
        component: RenderSpan,
        props: {
          value: value ? value.toLocaleString() : '0',
          className: 'num',
        },
        value: '',
      }
    },
    block_hash: (idField, item, colId) => {
      const value = item[colId] || ''
      const shortHash = value.length > 16 ? `${value.slice(0, 8)}...${value.slice(-8)}` : value
      return {
        component: RenderSpanWithTooltip,
        props: {
          value: shortHash || '-',
          tooltip: value,
          className: 'hash',
        },
        value: '',
      }
    },
    data_hub_url: (idField, item, colId) => {
      const value = item[colId] || ''
      return {
        component: RenderSpan,
        props: {
          value: value || '-',
          className: 'url',
        },
        value: '',
      }
    },
    ban_score: (idField, item, colId) => {
      const score = item[colId] || 0
      const isBanned = item.is_banned
      const className = isBanned ? 'ban-score-banned num' : score > 50 ? 'ban-score-warning num' : 'num'

      return {
        component: RenderSpan,
        props: {
          value: score.toString(),
          className: className,
        },
        value: '',
      }
    },
    bytes_received: (idField, item, colId) => {
      const bytes = item[colId] || 0
      return {
        component: RenderSpan,
        props: {
          value: formatBytes(bytes),
          className: 'num',
        },
        value: '',
      }
    },
    last_message_time: (idField, item, colId) => {
      const timestamp = item[colId] || 0
      return {
        component: RenderSpan,
        props: {
          value: formatRelativeTime(timestamp),
          className: 'time',
        },
        value: '',
      }
    },
  }

  // Auto-refresh every 10 seconds
  onMount(() => {
    fetchPeers()
    refreshInterval = window.setInterval(fetchPeers, 10000)
  })

  onDestroy(() => {
    if (refreshInterval) {
      clearInterval(refreshInterval)
    }
  })
</script>

<PageWithMenu>
  <Card contentPadding="0">
    <div class="title" slot="title">
      <Typo variant="title" size="h4" value={t(`${pageKey}.title`, { defaultValue: 'Peer Registry' })} />
    </div>
    <svelte:fragment slot="header-tools">
      <div class="stats">
        <span class="stat-item">
          <span class="stat-label">Total:</span>
          <span class="stat-value">{data.length}</span>
        </span>
        <span class="stat-item">
          <span class="stat-label">Connected:</span>
          <span class="stat-value"
            >{data.filter((p) => p.is_connected && !p.is_banned).length}</span
          >
        </span>
        <span class="stat-item">
          <span class="stat-label">Healthy:</span>
          <span class="stat-value"
            >{data.filter((p) => p.is_healthy && p.is_connected && !p.is_banned).length}</span
          >
        </span>
      </div>
      {#if data.length > 0}
        <Button size="small" on:click={fetchPeers} disabled={isLoading}>
          {isLoading ? 'Refreshing...' : 'Refresh'}
        </Button>
      {/if}
      <div class="live">
        <div class="live-icon connected">
          <Icon name="icon-status-light-glow-solid" size={14} />
        </div>
        <div class="live-label">{t(`page.network.live`)}</div>
      </div>
    </svelte:fragment>
    {#if error}
      <div class="no-data">
        <Icon name="icon-status-light-glow-solid" size={48} color="#ff6b6b" />
        <p>Failed to load peer data</p>
        <p class="sub">{error}</p>
        <Button size="small" on:click={fetchPeers} disabled={isLoading}>
          Retry
        </Button>
      </div>
    {:else if isLoading && data.length === 0}
      <div class="no-data">
        <Icon name="icon-status-light-glow-solid" size={48} color="rgba(255, 255, 255, 0.2)" />
        <p>Loading peer data...</p>
      </div>
    {:else if data.length === 0}
      <div class="no-data">
        <Icon name="icon-status-light-glow-solid" size={48} color="rgba(255, 255, 255, 0.2)" />
        <p>No peers available</p>
        <p class="sub">Waiting for peer connections...</p>
      </div>
    {:else}
      <Table
        name="peers"
        variant="dynamic"
        idField="id"
        {colDefs}
        {data}
        pagination={{
          page: 1,
          pageSize: 25,
        }}
        i18n={{ t, baseKey: 'comp.pager' }}
        pager={true}
        expandUp={true}
        {renderCells}
        getRowIconActions={null}
        on:action={() => {}}
      />
    {/if}
  </Card>
</PageWithMenu>

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
    color: rgba(255, 255, 255, 0.66);
    font-size: 13px;
  }

  .stat-value {
    color: #1878ff;
    font-size: 14px;
    font-weight: 600;
  }

  .live {
    display: flex;
    align-items: center;
    gap: 4px;

    color: rgba(255, 255, 255, 0.66);

    font-family: Satoshi;
    font-size: 13px;
    font-style: normal;
    font-weight: 700;
    line-height: 18px;
    letter-spacing: 0.26px;

    text-transform: uppercase;
  }

  .live-icon {
    color: #ce1722;
  }

  .live-icon.connected {
    color: #15b241;
  }

  .live-label {
    color: rgba(255, 255, 255, 0.66);
  }

  .no-data {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 60px 20px;
    gap: 12px;
  }

  .no-data p {
    margin: 0;
    color: rgba(255, 255, 255, 0.66);
    font-size: 16px;
  }

  .no-data p.sub {
    color: rgba(255, 255, 255, 0.44);
    font-size: 14px;
  }

  /* Custom styles for table cells */
  :global(.peer-id) {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: rgba(255, 255, 255, 0.88);
  }

  :global(.hash) {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: rgba(255, 255, 255, 0.66);
  }

  :global(.url) {
    font-size: 12px;
    color: rgba(255, 255, 255, 0.66);
    word-break: break-all;
  }

  :global(.num) {
    text-align: right !important;
    display: block !important;
    width: 100% !important;
    font-variant-numeric: tabular-nums;
  }

  :global(.time) {
    font-size: 12px;
    color: rgba(255, 255, 255, 0.66);
  }

  /* Status indicators */
  :global(.status-healthy) {
    color: #15b241 !important;
    font-weight: 600;
  }

  :global(.status-unhealthy) {
    color: #ffa500 !important;
    font-weight: 600;
  }

  :global(.status-disconnected) {
    color: #999 !important;
  }

  :global(.status-banned) {
    color: #ff6b6b !important;
    font-weight: 600;
  }

  :global(.status-url-down) {
    color: #ff9800 !important;
    font-weight: 600;
  }

  /* Ban score colors */
  :global(.ban-score-warning) {
    color: #ffa500 !important;
    font-weight: 600;
  }

  :global(.ban-score-banned) {
    color: #ff6b6b !important;
    font-weight: 600;
  }

  /* Right-align numeric column headers */
  :global(th:nth-child(3)),
  /* Height */
  :global(th:nth-child(6)),
  /* Ban Score */
  :global(th:nth-child(7)),
  /* Data Received */
  :global(th:nth-child(8))
    /* Last Message */ {
    text-align: right !important;
  }

  :global(th:nth-child(3) .table-cell-row),
  :global(th:nth-child(6) .table-cell-row),
  :global(th:nth-child(7) .table-cell-row),
  :global(th:nth-child(8) .table-cell-row) {
    justify-content: flex-end !important;
  }
</style>
