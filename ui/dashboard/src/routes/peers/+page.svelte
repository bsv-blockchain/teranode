<script lang="ts">
  import { onMount, onDestroy } from 'svelte'
  import { page } from '$app/stores'
  import { goto } from '$app/navigation'
  import { browser } from '$app/environment'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import Table from '$lib/components/table/index.svelte'
  import Pager from '$internal/components/pager/index.svelte'
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
    // Catchup metrics
    catchup_attempts: number
    catchup_successes: number
    catchup_failures: number
    catchup_last_attempt: number
    catchup_last_success: number
    catchup_last_failure: number
    catchup_reputation_score: number
    catchup_malicious_count: number
    catchup_avg_response_ms: number
  }

  interface PreviousAttemptData {
    peer_id: string
    peer_url: string
    target_block_hash: string
    target_block_height: number
    error_message: string
    error_type: string
    attempt_time: number
    duration_ms: number
    blocks_validated: number
  }

  interface CatchupStatusData {
    is_catching_up: boolean
    peer_id: string
    peer_url: string
    target_block_hash: string
    target_block_height: number
    current_height: number
    total_blocks: number
    blocks_fetched: number
    blocks_validated: number
    start_time: number
    duration_ms: number
    fork_depth: number
    common_ancestor_hash: string
    common_ancestor_height: number
    previous_attempt?: PreviousAttemptData
  }

  let allData: PeerData[] = []  // Full dataset
  let data: PeerData[] = []      // Paginated data for display
  let catchupStatus: CatchupStatusData | null = null
  let isLoading = false
  let error: string | null = null
  let refreshInterval: number | null = null
  let catchupRefreshInterval: number | null = null

  // Persistent pagination state
  let currentPage = 1
  let currentPageSize = 25

  // Persistent sort state
  let sortColumn = ''
  let sortOrder = ''

  // Local storage keys for persistence
  const PEERS_PAGE_SIZE_KEY = 'teranode-peers-pagesize'
  const PEERS_SORT_KEY = 'teranode-peers-sort'

  // Load pageSize from localStorage
  function loadPageSizeFromStorage(): number {
    if (browser) {
      try {
        const stored = localStorage.getItem(PEERS_PAGE_SIZE_KEY)
        if (stored) {
          const parsed = parseInt(stored, 10)
          if (parsed > 0 && parsed <= 100) {
            return parsed
          }
        }
      } catch (error) {
        console.warn('Failed to load pageSize from localStorage:', error)
      }
    }
    return 25 // Default value
  }

  // Load sort preferences from localStorage
  function loadSortFromStorage(): { sortColumn: string; sortOrder: string } | null {
    if (browser) {
      try {
        const stored = localStorage.getItem(PEERS_SORT_KEY)
        if (stored) {
          return JSON.parse(stored)
        }
      } catch (error) {
        console.warn('Failed to load sort from localStorage:', error)
      }
    }
    return null
  }

  // Save pageSize to localStorage
  function savePageSizeToStorage(size: number) {
    if (browser) {
      try {
        localStorage.setItem(PEERS_PAGE_SIZE_KEY, size.toString())
      } catch (error) {
        console.warn('Failed to save pageSize to localStorage:', error)
      }
    }
  }

  // Save sort to localStorage
  function saveSortToStorage(sortColumn: string, sortOrder: string) {
    if (browser) {
      try {
        localStorage.setItem(PEERS_SORT_KEY, JSON.stringify({ sortColumn, sortOrder }))
      } catch (error) {
        console.warn('Failed to save sort to localStorage:', error)
      }
    }
  }

  // Load saved preferences on mount
  if (browser) {
    const savedSort = loadSortFromStorage()
    if (savedSort) {
      sortColumn = savedSort.sortColumn
      sortOrder = savedSort.sortOrder
    }
  }

  // Get pagination from URL and localStorage
  $: {
    // First, try to get pageSize from URL (highest priority)
    const urlPageSize = $page.url.searchParams.get('pageSize')
    if (urlPageSize) {
      const parsed = parseInt(urlPageSize, 10)
      if (parsed > 0 && parsed <= 100) {
        currentPageSize = parsed
      }
    } else {
      // If no URL parameter, use localStorage default
      currentPageSize = loadPageSizeFromStorage()
    }

    // Get page from URL
    const urlPage = $page.url.searchParams.get('page')
    if (urlPage) {
      const parsed = parseInt(urlPage, 10)
      if (parsed > 0) {
        currentPage = parsed
      }
    } else {
      currentPage = 1 // Always reset to page 1 if not in URL
    }

    // Update paginated data when URL changes
    updatePaginatedData()
  }

  // Update URL when pagination changes
  function updateURL(newPage: number, newPageSize: number) {
    const url = new URL($page.url)
    url.searchParams.set('pageSize', newPageSize.toString())
    // Only set page in URL if it's not page 1 (keep URLs clean)
    if (newPage > 1) {
      url.searchParams.set('page', newPage.toString())
    } else {
      url.searchParams.delete('page')
    }
    goto(url.toString(), { replaceState: true })
  }

  function updatePaginatedData() {
    const startIndex = (currentPage - 1) * currentPageSize
    const endIndex = startIndex + currentPageSize
    data = allData.slice(startIndex, endIndex)
  }

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

      // Filter out peers whose last message was over 1 minute ago
      const now = Math.floor(Date.now() / 1000)
      const oneMinuteAgo = now - 60
      allData = (result.peers || []).filter(peer => peer.last_message_time > oneMinuteAgo)
      updatePaginatedData()
    } catch (err) {
      console.error('Failed to fetch peers:', err)
      error = err instanceof Error ? err.message : 'Unknown error'
      allData = []
      data = []
    } finally {
      isLoading = false
    }
  }

  // Fetch catchup status from the API
  async function fetchCatchupStatus() {
    try {
      const response = await fetch('/api/catchup/status')

      if (!response.ok) {
        console.error(`Catchup status fetch failed: HTTP ${response.status}`)
        catchupStatus = null
        return
      }

      const result = await response.json()

      if (result.error) {
        console.error(`Catchup status error: ${result.error}`)
        catchupStatus = null
        return
      }

      // Store the catchup status if catching up, otherwise clear it
      if (result.is_catching_up) {
        catchupStatus = result
      } else {
        catchupStatus = null
      }
    } catch (err) {
      console.error('Failed to fetch catchup status:', err)
      catchupStatus = null
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

  // Format duration in milliseconds
  function formatDuration(ms: number): string {
    if (ms === 0) return '0ms'
    if (ms < 1000) return `${ms}ms`
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
    if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`
    return `${(ms / 3600000).toFixed(1)}h`
  }

  // Handle pagination changes
  function onPageChange(e) {
    const data = e.detail
    const newPage = data.value.page
    const newPageSize = data.value.pageSize

    // Save pageSize to localStorage if it changed
    if (newPageSize !== currentPageSize) {
      savePageSizeToStorage(newPageSize)
    }

    currentPage = newPage
    currentPageSize = newPageSize
    updatePaginatedData()
    updateURL(newPage, newPageSize)
  }

  // Handle sort changes
  function onSort(e) {
    const { colId, value } = e.detail
    sortColumn = colId
    sortOrder = value

    // Save sort to localStorage, or remove if clearing
    if (colId && value) {
      saveSortToStorage(colId, value)
    } else {
      // Clear sort from localStorage
      if (browser) {
        try {
          localStorage.removeItem(PEERS_SORT_KEY)
        } catch (error) {
          console.warn('Failed to remove sort from localStorage:', error)
        }
      }
    }
  }

  // Clear sort
  function clearSort() {
    onSort({ detail: { colId: '', value: '' } })
  }

  // Handle table actions
  function handleAction(event) {
    // Handle any table actions if needed
    console.log('Table action:', event.detail)
  }

  $: hasSorting = sortColumn && sortOrder

  let totalPages = 0
  const onTotal = (e) => {
    totalPages = e.detail.total
  }

  $: showPagerNav = totalPages > 1
  $: showPagerSize = showPagerNav || (totalPages === 1 && allData.length > 5)
  $: showTableFooter = showPagerSize

  $: i18nLocal = { t, baseKey: 'comp.pager' }

  // Format error type to human-readable string
  function formatErrorType(errorType: string): string {
    const errorTypeMap: Record<string, string> = {
      'validation_failure': 'Validation Failed',
      'network_error': 'Network Error',
      'secret_mining': 'Secret Mining Detected',
      'coinbase_maturity_violation': 'Coinbase Maturity Violation',
      'checkpoint_verification_failed': 'Checkpoint Verification Failed',
      'connection_error': 'Connection Error',
      'unknown_error': 'Unknown Error',
    }
    return errorTypeMap[errorType] || errorType
  }

  // Column definitions for the table
  function getColDefs() {
    return [
      {
        id: 'id',
        name: 'Peer ID',
        type: 'string',
        props: {
          width: '12%',
        },
      },
      {
        id: 'is_connected',
        name: 'Status',
        type: 'string',
        props: {
          width: '8%',
        },
      },
      {
        id: 'height',
        name: 'Height',
        type: 'number',
        props: {
          width: '8%',
        },
      },
      {
        id: 'catchup_reputation_score',
        name: 'Reputation',
        type: 'number',
        props: {
          width: '9%',
        },
      },
      {
        id: 'catchup_success_rate',
        name: 'Success Rate',
        type: 'number',
        props: {
          width: '10%',
        },
      },
      {
        id: 'catchup_attempts',
        name: 'Attempts',
        type: 'number',
        props: {
          width: '8%',
        },
      },
      {
        id: 'catchup_avg_response_ms',
        name: 'Avg Response',
        type: 'number',
        props: {
          width: '10%',
        },
      },
      {
        id: 'bytes_received',
        name: 'Bytes Received',
        type: 'number',
        props: {
          width: '12%',
        },
      },
      {
        id: 'data_hub_url',
        name: 'DataHub URL',
        type: 'string',
        props: {
          width: '23%',
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
      } else if (isHealthy && isConnected) {
        // For connected and healthy peers, show as Healthy regardless of URL status
        status = 'Healthy'
        className = 'status-healthy'
      } else if (!urlResponsive) {
        status = 'URL Down'
        className = 'status-url-down'
      } else {
        status = 'Connected'
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
    catchup_reputation_score: (idField, item, colId) => {
      const score = item[colId] || 0
      let className = 'num reputation'
      let displayValue = score.toFixed(1)

      // Color based on score
      if (score >= 80) {
        className += ' reputation-excellent'
      } else if (score >= 60) {
        className += ' reputation-good'
      } else if (score >= 40) {
        className += ' reputation-fair'
      } else if (score > 0) {
        className += ' reputation-poor'
      } else {
        displayValue = '-'
      }

      return {
        component: RenderSpan,
        props: {
          value: displayValue,
          className: className,
        },
        value: '',
      }
    },
    catchup_success_rate: (idField, item, colId) => {
      const successes = item.catchup_successes || 0
      const failures = item.catchup_failures || 0
      const totalAttempts = successes + failures

      if (totalAttempts === 0) {
        return {
          component: RenderSpan,
          props: {
            value: '-',
            className: 'num',
          },
          value: '',
        }
      }

      const rate = (successes / totalAttempts) * 100
      let className = 'num'

      if (rate >= 90) {
        className += ' success-rate-excellent'
      } else if (rate >= 75) {
        className += ' success-rate-good'
      } else if (rate >= 50) {
        className += ' success-rate-fair'
      } else {
        className += ' success-rate-poor'
      }

      return {
        component: RenderSpan,
        props: {
          value: `${rate.toFixed(1)}%`,
          className: className,
        },
        value: '',
      }
    },
    catchup_attempts: (idField, item, colId) => {
      const attempts = item.catchup_attempts || 0
      const successes = item.catchup_successes || 0
      const failures = item.catchup_failures || 0

      const tooltipText = `Successes: ${successes}\nFailures: ${failures}\nTotal: ${attempts}`

      return {
        component: RenderSpanWithTooltip,
        props: {
          value: attempts.toString(),
          tooltip: tooltipText,
          className: 'num',
        },
        value: '',
      }
    },
    catchup_avg_response_ms: (idField, item, colId) => {
      const ms = item[colId] || 0

      if (ms === 0) {
        return {
          component: RenderSpan,
          props: {
            value: '-',
            className: 'num',
          },
          value: '',
        }
      }

      return {
        component: RenderSpan,
        props: {
          value: formatDuration(ms),
          className: 'num',
        },
        value: '',
      }
    },
  }

  // Auto-refresh every 10 seconds for peers, every 3 seconds for catchup status
  onMount(() => {
    fetchPeers()
    fetchCatchupStatus()
    refreshInterval = window.setInterval(fetchPeers, 10000)
    // Check catchup status more frequently to provide real-time updates
    catchupRefreshInterval = window.setInterval(fetchCatchupStatus, 3000)
  })

  onDestroy(() => {
    if (refreshInterval) {
      clearInterval(refreshInterval)
    }
    if (catchupRefreshInterval) {
      clearInterval(catchupRefreshInterval)
    }
  })
</script>

<PageWithMenu>
  {#if catchupStatus && catchupStatus.is_catching_up}
    <div class="catchup-status-wrapper">
      <Card contentPadding="16px">
        <div class="catchup-header">
        <div class="catchup-title">
          <div class="spinner"></div>
          <Typo variant="title" size="h5" value="Block Catchup in Progress" />
        </div>
        <div class="catchup-duration">
          {formatDuration(catchupStatus.duration_ms)}
        </div>
      </div>
      <div class="catchup-details">
        <div class="catchup-detail-item">
          <span class="catchup-label">Syncing From</span>
          <span class="catchup-value peer-id-value">{catchupStatus.peer_id || 'Unknown'}</span>
        </div>
        <div class="catchup-detail-item">
          <span class="catchup-label">Peer URL</span>
          <span class="catchup-value url-value">{catchupStatus.peer_url || '-'}</span>
        </div>
        <div class="catchup-detail-item">
          <span class="catchup-label">Target Block</span>
          <span class="catchup-value">
            <span class="hash-value">{catchupStatus.target_block_hash?.slice(0, 8)}...{catchupStatus.target_block_hash?.slice(-8)}</span>
            <span class="height-badge">#{catchupStatus.target_block_height?.toLocaleString()}</span>
          </span>
        </div>
        <div class="catchup-detail-item">
          <span class="catchup-label">Starting Height</span>
          <span class="catchup-value">#{catchupStatus.current_height?.toLocaleString()}</span>
        </div>
        <div class="catchup-detail-item">
          <span class="catchup-label">Progress</span>
          <span class="catchup-value progress-value">
            {catchupStatus.blocks_validated || 0} / {catchupStatus.total_blocks || 0} blocks
            {#if catchupStatus.total_blocks > 0}
              <span class="progress-percentage">
                ({((catchupStatus.blocks_validated / catchupStatus.total_blocks) * 100).toFixed(1)}%)
              </span>
            {/if}
          </span>
        </div>
        {#if catchupStatus.fork_depth > 0}
          <div class="catchup-detail-item">
            <span class="catchup-label">Fork Depth</span>
            <span class="catchup-value fork-depth">{catchupStatus.fork_depth} blocks</span>
          </div>
        {/if}
        {#if catchupStatus.common_ancestor_hash}
          <div class="catchup-detail-item">
            <span class="catchup-label">Common Ancestor</span>
            <span class="catchup-value">
              <span class="hash-value">{catchupStatus.common_ancestor_hash?.slice(0, 8)}...{catchupStatus.common_ancestor_hash?.slice(-8)}</span>
              {#if catchupStatus.common_ancestor_height}
                <span class="height-badge-small">#{catchupStatus.common_ancestor_height?.toLocaleString()}</span>
              {/if}
            </span>
          </div>
        {/if}
      </div>
      <div class="progress-bar">
        <div
          class="progress-bar-fill"
          style="width: {catchupStatus.total_blocks > 0 ? (catchupStatus.blocks_validated / catchupStatus.total_blocks) * 100 : 0}%"
        ></div>
      </div>
      {#if catchupStatus.previous_attempt}
        <div class="previous-attempt">
          <div class="previous-attempt-header">
            <Icon name="icon-status-light-glow-solid" size={14} color="#ffa500" />
            <span class="previous-attempt-title">Previous Attempt Failed</span>
          </div>
          <div class="previous-attempt-grid">
            <div class="previous-attempt-item">
              <span class="attempt-label">Peer:</span>
              <span class="attempt-value peer-id-value">{catchupStatus.previous_attempt.peer_id}</span>
            </div>
            <div class="previous-attempt-item">
              <span class="attempt-label">Error Type:</span>
              <span class="attempt-value error-value">
                {formatErrorType(catchupStatus.previous_attempt.error_type)}
              </span>
            </div>
            <div class="previous-attempt-item">
              <span class="attempt-label">Duration:</span>
              <span class="attempt-value">{formatDuration(catchupStatus.previous_attempt.duration_ms)}</span>
            </div>
            {#if catchupStatus.previous_attempt.blocks_validated > 0}
              <div class="previous-attempt-item">
                <span class="attempt-label">Blocks Validated:</span>
                <span class="attempt-value">{catchupStatus.previous_attempt.blocks_validated.toLocaleString()}</span>
              </div>
            {/if}
          </div>
          <div class="error-message-container">
            <div class="error-message-label">Error Message:</div>
            <div class="error-message-box" title={catchupStatus.previous_attempt.error_message}>
              {catchupStatus.previous_attempt.error_message}
            </div>
          </div>
        </div>
      {/if}
      </Card>
    </div>
  {/if}

  <Card contentPadding="0" showFooter={showTableFooter}>
    <div class="title" slot="title">
      <Typo variant="title" size="h4" value={t(`${pageKey}.title`, { defaultValue: 'Peer Registry' })} />
    </div>
    <svelte:fragment slot="header-tools">
      <div class="stats">
        <span class="stat-item">
          <span class="stat-label">Total:</span>
          <span class="stat-value">{allData.length}</span>
        </span>
        <span class="stat-item">
          <span class="stat-label">Connected:</span>
          <span class="stat-value"
            >{allData.filter((p) => p.is_connected && !p.is_banned).length}</span
          >
        </span>
        <span class="stat-item">
          <span class="stat-label">Healthy:</span>
          <span class="stat-value"
            >{allData.filter((p) => p.is_healthy && p.is_connected && !p.is_banned).length}</span
          >
        </span>
      </div>
      <Pager
        i18n={i18nLocal}
        expandUp={true}
        totalItems={allData?.length}
        showPageSize={false}
        showQuickNav={false}
        showNav={showPagerNav}
        value={{
          page: currentPage,
          pageSize: currentPageSize,
        }}
        hasBoundaryRight={true}
        on:change={onPageChange}
        on:total={onTotal}
      />
      {#if allData.length > 0}
        <Button size="small" on:click={fetchPeers} disabled={isLoading}>
          {isLoading ? 'Refreshing...' : 'Refresh'}
        </Button>
      {/if}
      {#if hasSorting}
        <button class="clear-sort-btn" on:click={clearSort} title="Clear sorting">
          <Icon name="icon-close-line" size={16} />
        </button>
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
        sort={{
          sortColumn,
          sortOrder,
        }}
        sortEnabled={true}
        pagination={{
          page: 1,
          pageSize: -1,
        }}
        paginationEnabled={false}
        i18n={{ t, baseKey: 'comp.pager' }}
        pager={false}
        expandUp={true}
        {renderCells}
        getRenderProps={null}
        getRowIconActions={null}
        on:sort={onSort}
        on:action={handleAction}
      />
    {/if}
    <div slot="footer">
      <Pager
        i18n={i18nLocal}
        expandUp={true}
        totalItems={allData?.length}
        showPageSize={showPagerSize}
        showQuickNav={showPagerNav}
        showNav={showPagerNav}
        value={{
          page: currentPage,
          pageSize: currentPageSize,
        }}
        hasBoundaryRight={true}
        on:change={onPageChange}
      />
    </div>
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
  :global(th:nth-child(4)),
  /* Reputation */
  :global(th:nth-child(5)),
  /* Success Rate */
  :global(th:nth-child(6)),
  /* Attempts */
  :global(th:nth-child(7)),
  /* Avg Response */
  :global(th:nth-child(9)),
  /* Last Message */
  :global(th:nth-child(10))
    /* Ban Score */ {
    text-align: right !important;
  }

  :global(th:nth-child(3) .table-cell-row),
  :global(th:nth-child(4) .table-cell-row),
  :global(th:nth-child(5) .table-cell-row),
  :global(th:nth-child(6) .table-cell-row),
  :global(th:nth-child(7) .table-cell-row),
  :global(th:nth-child(9) .table-cell-row),
  :global(th:nth-child(10) .table-cell-row) {
    justify-content: flex-end !important;
  }

  /* Catchup reputation score colors */
  :global(.reputation-excellent) {
    color: #15b241 !important;
    font-weight: 600;
  }

  :global(.reputation-good) {
    color: #52c41a !important;
    font-weight: 600;
  }

  :global(.reputation-fair) {
    color: #ffa500 !important;
    font-weight: 600;
  }

  :global(.reputation-poor) {
    color: #ff6b6b !important;
    font-weight: 600;
  }

  /* Success rate colors */
  :global(.success-rate-excellent) {
    color: #15b241 !important;
    font-weight: 600;
  }

  :global(.success-rate-good) {
    color: #52c41a !important;
    font-weight: 600;
  }

  :global(.success-rate-fair) {
    color: #ffa500 !important;
    font-weight: 600;
  }

  :global(.success-rate-poor) {
    color: #ff6b6b !important;
    font-weight: 600;
  }

  /* Catchup status card styles */
  .catchup-status-wrapper {
    margin-bottom: 20px;
  }

  .catchup-status-wrapper :global(.card) {
    background: linear-gradient(135deg, rgba(255, 152, 0, 0.1) 0%, rgba(255, 193, 7, 0.05) 100%) !important;
    border-left: 4px solid #ff9800 !important;
  }

  .catchup-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
    padding-bottom: 12px;
    border-bottom: 1px solid rgba(255, 152, 0, 0.2);
  }

  .catchup-title {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .spinner {
    width: 18px;
    height: 18px;
    border: 3px solid rgba(255, 152, 0, 0.3);
    border-top-color: #ff9800;
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }

  .catchup-duration {
    font-family: 'JetBrains Mono', monospace;
    font-size: 14px;
    color: #ff9800;
    font-weight: 600;
  }

  .catchup-details {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px 24px;
    margin-bottom: 20px;
  }

  .catchup-detail-item {
    display: flex;
    flex-direction: column;
    gap: 6px;
    min-width: 0; /* Allow items to shrink below content size */
  }

  .catchup-label {
    font-size: 11px;
    color: rgba(255, 255, 255, 0.5);
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .catchup-value {
    font-size: 14px;
    color: rgba(255, 255, 255, 0.88);
    font-weight: 500;
    word-wrap: break-word;
    overflow-wrap: break-word;
  }

  .peer-id-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    color: #1878ff;
    font-weight: 600;
  }

  .url-value {
    font-size: 12px;
    color: rgba(255, 255, 255, 0.66);
    word-break: break-all;
    line-height: 1.4;
  }

  .hash-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: rgba(255, 255, 255, 0.75);
  }

  .height-badge {
    display: inline-block;
    background: rgba(255, 152, 0, 0.2);
    color: #ff9800;
    padding: 3px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
    margin-left: 6px;
    vertical-align: middle;
  }

  .height-badge-small {
    display: inline-block;
    background: rgba(255, 152, 0, 0.15);
    color: #ffa500;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 600;
    margin-left: 6px;
    vertical-align: middle;
  }

  .progress-value {
    font-variant-numeric: tabular-nums;
    line-height: 1.6;
  }

  .progress-percentage {
    color: #ff9800;
    font-weight: 700;
    margin-left: 4px;
    display: inline-block;
  }

  .fork-depth {
    color: #ffa500 !important;
    font-weight: 700;
  }

  .progress-bar {
    width: 100%;
    height: 8px;
    background: rgba(255, 152, 0, 0.15);
    border-radius: 4px;
    overflow: hidden;
    margin-top: 4px;
  }

  .progress-bar-fill {
    height: 100%;
    background: linear-gradient(90deg, #ff9800 0%, #ffc107 100%);
    transition: width 0.3s ease-in-out;
    border-radius: 4px;
  }

  /* Previous attempt styles */
  .previous-attempt {
    margin-top: 20px;
    padding-top: 16px;
    border-top: 1px solid rgba(255, 152, 0, 0.2);
  }

  .previous-attempt-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
  }

  .previous-attempt-title {
    font-size: 14px;
    font-weight: 600;
    color: #ffa500;
  }

  .previous-attempt-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 12px 20px;
    margin-bottom: 16px;
  }

  .previous-attempt-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .attempt-label {
    font-size: 11px;
    color: rgba(255, 255, 255, 0.5);
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .attempt-value {
    font-size: 13px;
    color: rgba(255, 255, 255, 0.88);
    font-weight: 500;
    word-wrap: break-word;
    overflow-wrap: break-word;
    word-break: break-all;
  }

  .attempt-value.peer-id-value {
    word-break: break-all;
    line-height: 1.4;
  }

  .error-value {
    color: #ff6b6b !important;
    font-weight: 600;
  }

  .error-message-container {
    margin-top: 8px;
  }

  .error-message-label {
    font-size: 11px;
    color: rgba(255, 255, 255, 0.5);
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
  }

  .error-message-box {
    background: rgba(255, 107, 107, 0.1);
    border-left: 3px solid #ff6b6b;
    padding: 10px 12px;
    border-radius: 4px;
    font-size: 12px;
    color: rgba(255, 255, 255, 0.8);
    line-height: 1.5;
    font-family: 'JetBrains Mono', monospace;
    word-wrap: break-word;
    overflow-wrap: break-word;
    white-space: pre-wrap;
    max-height: 120px;
    overflow-y: auto;
    cursor: help;
  }

  .error-message-box::-webkit-scrollbar {
    width: 6px;
  }

  .error-message-box::-webkit-scrollbar-track {
    background: rgba(255, 107, 107, 0.1);
    border-radius: 3px;
  }

  .error-message-box::-webkit-scrollbar-thumb {
    background: rgba(255, 107, 107, 0.3);
    border-radius: 3px;
  }

  .error-message-box::-webkit-scrollbar-thumb:hover {
    background: rgba(255, 107, 107, 0.5);
  }

  /* Clear sort button styling */
  .clear-sort-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    padding: 0;
    background: transparent;
    border: none;
    color: rgba(255, 255, 255, 0.66);
    cursor: pointer;
    transition: all 0.2s ease;
    border-radius: 4px;
  }

  .clear-sort-btn:hover {
    background: rgba(255, 255, 255, 0.1);
    color: rgba(255, 255, 255, 0.9);
  }

  .clear-sort-btn:active {
    background: rgba(255, 255, 255, 0.15);
  }
</style>
