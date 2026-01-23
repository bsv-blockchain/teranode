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
  import RenderLink from '$lib/components/table/renderers/render-link/index.svelte'

  $: t = $i18n.t

  const pageKey = 'page.peers'

  interface PeerData {
    id: string
    client_name?: string
    height: number
    block_hash: string
    data_hub_url: string
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
    last_catchup_error: string
    last_catchup_error_time: number
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

  // Modal state
  let showCatchupModal = false
  let selectedPeer: PeerData | null = null

  // Persistent pagination state
  let currentPage = 1
  let currentPageSize = 25

  // Persistent sort state
  let sortColumn = ''
  let sortOrder = ''

  // Memoized sorted data
  let sortedData: PeerData[] = []

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
      // Validate that the saved sort column still exists
      const validColumns = ['id', 'is_connected', 'height', 'catchup_reputation_score', 'bytes_received', 'data_hub_url']
      if (validColumns.includes(savedSort.sortColumn)) {
        sortColumn = savedSort.sortColumn
        sortOrder = savedSort.sortOrder
      } else {
        // Clear invalid sort from localStorage
        localStorage.removeItem(PEERS_SORT_KEY)
        sortColumn = ''
        sortOrder = ''
      }
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

  // Apply sorting to data and memoize the result
  function applySorting(dataToSort: PeerData[]): PeerData[] {
    if (!sortColumn || !sortOrder) {
      return [...dataToSort]
    }

    const sorted = [...dataToSort]
    const multiplier = sortOrder === 'asc' ? 1 : -1

    sorted.sort((a, b) => {
      const aVal = a[sortColumn]
      const bVal = b[sortColumn]

      // Handle null/undefined values
      if (aVal === null || aVal === undefined) return 1 * multiplier
      if (bVal === null || bVal === undefined) return -1 * multiplier

      // Numeric comparison
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return (aVal - bVal) * multiplier
      }

      // String comparison
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return aVal.localeCompare(bVal) * multiplier
      }

      // Boolean comparison
      if (typeof aVal === 'boolean' && typeof bVal === 'boolean') {
        return (aVal === bVal ? 0 : aVal ? -1 : 1) * multiplier
      }

      return 0
    })

    return sorted
  }

  function updatePaginatedData() {
    // Apply sorting to allData and memoize
    sortedData = applySorting(allData)

    // Paginate the sorted data
    const startIndex = (currentPage - 1) * currentPageSize
    const endIndex = startIndex + currentPageSize
    data = sortedData.slice(startIndex, endIndex)
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
    sortColumn = colId || ''
    sortOrder = value || ''

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

    // Re-apply sorting and update the displayed data
    updatePaginatedData()
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
        name: 'Peer',
        type: 'string',
        props: {
          width: '16%',
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
          width: '10%',
        },
      },
      {
        id: 'catchup',
        name: 'Metrics',
        type: 'string',
        sortable: false,  // Disable sorting for the catchup column
        props: {
          width: '8%',
        },
      },
      {
        id: 'bytes_received',
        name: 'Bytes Received',
        type: 'number',
        props: {
          width: '13%',
        },
      },
      {
        id: 'data_hub_url',
        name: 'DataHub URL',
        type: 'string',
        props: {
          width: '27%',
        },
      },
    ]
  }

  $: colDefs = getColDefs()

  // Custom render functions
  const renderCells = {
    id: (idField, item, colId) => {
      // Prefer client_name over peer ID for display
      const peerId = item[colId] || ''
      const displayValue = item.client_name || peerId

      // If using client name, show full name; if using peer ID, abbreviate it
      let shortValue = displayValue
      if (!item.client_name && displayValue.length > 16) {
        shortValue = `${displayValue.slice(0, 8)}...${displayValue.slice(-8)}`
      }

      // Always show the peer ID in the tooltip, and client name if available
      let tooltip = peerId
      if (item.client_name) {
        tooltip = `${item.client_name}\n${peerId}`
      }

      return {
        component: RenderSpanWithTooltip,
        props: {
          value: shortValue,
          tooltip: tooltip,
          className: 'peer-name',
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
    catchup: (idField, item, colId) => {
      return {
        component: RenderSpan,
        props: {
          value: 'View',
          className: 'catchup-view-link',
        },
        value: '',
      }
    },
  }

  // Auto-refresh every 10 seconds for peers, every 3 seconds for catchup status
  onMount(() => {
    // Validate sort column exists in current columns
    const validColumns = ['id', 'is_connected', 'height', 'catchup_reputation_score', 'bytes_received', 'data_hub_url']
    if (sortColumn && !validColumns.includes(sortColumn)) {
      // Clear invalid sort
      sortColumn = ''
      sortOrder = ''
      localStorage.removeItem(PEERS_SORT_KEY)
    }

    fetchPeers()
    fetchCatchupStatus()
    refreshInterval = window.setInterval(fetchPeers, 10000)
    // Check catchup status more frequently to provide real-time updates
    catchupRefreshInterval = window.setInterval(fetchCatchupStatus, 3000)

    // Add event listener for catchup view buttons
    document.addEventListener('click', handleCatchupButtonClick)
  })

  // Handle clicks on catchup view buttons
  function handleCatchupButtonClick(event: MouseEvent) {
    const target = event.target as HTMLElement
    if (target.classList.contains('catchup-view-link')) {
      // Find the row element
      let row = target.closest('tr')
      if (row) {
        // Get the row index
        const tbody = row.parentElement
        const rowIndex = Array.from(tbody?.children || []).indexOf(row)
        // Get the peer from the displayed data
        const peer = data[rowIndex]
        if (peer) {
          selectedPeer = peer
          showCatchupModal = true
        }
      }
    }
  }

  onDestroy(() => {
    if (refreshInterval) {
      clearInterval(refreshInterval)
    }
    if (catchupRefreshInterval) {
      clearInterval(catchupRefreshInterval)
    }
    // Remove event listener
    document.removeEventListener('click', handleCatchupButtonClick)
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
          <span class="catchup-value peer-name-value">{catchupStatus.peer_id || 'Unknown'}</span>
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
              <span class="attempt-value peer-name-value">{catchupStatus.previous_attempt.peer_id}</span>
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
          <span class="stat-label">Good Reputation:</span>
          <span class="stat-value"
            >{allData.filter((p) => p.catchup_reputation_score >= 50 && p.is_connected && !p.is_banned).length}</span
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
        serverSort={true}
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

{#if showCatchupModal && selectedPeer}
  <!-- svelte-ignore a11y-no-static-element-interactions -->
  <div
    class="modal-overlay"
    role="presentation"
    on:click={() => {
      showCatchupModal = false
      selectedPeer = null
    }}
    on:keydown={(e) => {
      if (e.key === 'Escape') {
        showCatchupModal = false
        selectedPeer = null
      }
    }}
  >
    <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
    <div
      class="modal-content"
      role="dialog"
      aria-modal="true"
      aria-labelledby="modal-title"
      on:click|stopPropagation
      on:keydown|stopPropagation
    >
      <div class="modal-header">
        <h2 id="modal-title" class="modal-title">Catchup Details - {selectedPeer.client_name || selectedPeer.id}</h2>
        <button class="modal-close" on:click={() => {
          showCatchupModal = false
          selectedPeer = null
        }}>×</button>
      </div>
      <div class="modal-body">
        <div class="modal-section">
          <h3 class="section-title">Performance Metrics</h3>
        <div class="metrics-grid">
          <div class="metric-item">
            <span class="metric-label">Reputation Score</span>
            <span class="metric-value reputation-score" data-score="{selectedPeer.catchup_reputation_score}">
              {selectedPeer.catchup_reputation_score ? selectedPeer.catchup_reputation_score.toFixed(1) : '0.0'}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Success Rate</span>
            <span class="metric-value">
              {#if selectedPeer.catchup_attempts > 0}
                {((selectedPeer.catchup_successes / (selectedPeer.catchup_successes + selectedPeer.catchup_failures)) * 100).toFixed(1)}%
              {:else}
                -
              {/if}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Total Attempts</span>
            <span class="metric-value">{selectedPeer.catchup_attempts || 0}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Successes</span>
            <span class="metric-value success">{selectedPeer.catchup_successes || 0}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Failures</span>
            <span class="metric-value failure">{selectedPeer.catchup_failures || 0}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Malicious Count</span>
            <span class="metric-value malicious">{selectedPeer.catchup_malicious_count || 0}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Avg Response Time</span>
            <span class="metric-value">
              {selectedPeer.catchup_avg_response_ms ? formatDuration(selectedPeer.catchup_avg_response_ms) : '-'}
            </span>
          </div>
        </div>
      </div>

      <div class="modal-section">
        <h3 class="section-title">Last Activity</h3>
        <div class="metrics-grid">
          <div class="metric-item">
            <span class="metric-label">Last Attempt</span>
            <span class="metric-value">
              {selectedPeer.catchup_last_attempt ? new Date(selectedPeer.catchup_last_attempt * 1000).toLocaleString() : 'Never'}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Last Success</span>
            <span class="metric-value">
              {selectedPeer.catchup_last_success ? new Date(selectedPeer.catchup_last_success * 1000).toLocaleString() : 'Never'}
            </span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Last Failure</span>
            <span class="metric-value">
              {selectedPeer.catchup_last_failure ? new Date(selectedPeer.catchup_last_failure * 1000).toLocaleString() : 'Never'}
            </span>
          </div>
        </div>
      </div>

      {#if selectedPeer.last_catchup_error}
        <div class="modal-section">
          <h3 class="section-title error-title">Last Catchup Error</h3>
          <div class="error-details">
            <div class="error-time">
              {selectedPeer.last_catchup_error_time ? new Date(selectedPeer.last_catchup_error_time * 1000).toLocaleString() : 'Unknown time'}
            </div>
            <div class="error-message">
              {selectedPeer.last_catchup_error}
            </div>
          </div>
        </div>
      {/if}

      <div class="modal-section">
        <h3 class="section-title">Peer Information</h3>
        <div class="metrics-grid">
          <div class="metric-item">
            <span class="metric-label">Peer ID</span>
            <span class="metric-value peer-id">{selectedPeer.id}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Client Name</span>
            <span class="metric-value">{selectedPeer.client_name || '-'}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Height</span>
            <span class="metric-value">#{selectedPeer.height?.toLocaleString() || '0'}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">DataHub URL</span>
            <span class="metric-value url">{selectedPeer.data_hub_url || '-'}</span>
          </div>
        </div>
      </div>
      </div>
    </div>
  </div>
{/if}

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
  :global(.peer-name) {
    font-family: 'Satoshi', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
    font-size: 13px;
    color: rgba(255, 255, 255, 0.88);
    font-weight: 500;
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

  /* Right-align numeric column headers and data */
  /* Height (column 2) */
  :global(th:nth-child(2)),
  :global(td:nth-child(2)) {
    text-align: right !important;
  }

  :global(th:nth-child(2) .table-cell-row),
  :global(td:nth-child(2) .table-cell-row) {
    justify-content: flex-end !important;
  }

  /* Reputation (column 3) */
  :global(th:nth-child(3)),
  :global(td:nth-child(3)) {
    text-align: right !important;
  }

  :global(th:nth-child(3) .table-cell-row),
  :global(td:nth-child(3) .table-cell-row) {
    justify-content: flex-end !important;
  }

  /* Metrics (column 4) - center align */
  :global(th:nth-child(4)),
  :global(td:nth-child(4)) {
    text-align: center !important;
  }

  :global(th:nth-child(4) .table-cell-row),
  :global(td:nth-child(4) .table-cell-row) {
    justify-content: center !important;
  }

  /* Bytes Received (column 5) */
  :global(th:nth-child(5)),
  :global(td:nth-child(5)) {
    text-align: right !important;
  }

  :global(th:nth-child(5) .table-cell-row),
  :global(td:nth-child(5) .table-cell-row) {
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
    width: 100%;
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

  .peer-name-value {
    font-family: 'Satoshi', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
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

  .attempt-value.peer-name-value {
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
    word-break: break-all;
    white-space: pre-wrap;
    max-height: 120px;
    max-width: 100%;
    overflow-x: auto;
    overflow-y: auto;
    cursor: help;
  }

  .error-message-box::-webkit-scrollbar {
    width: 6px;
    height: 6px;
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

  /* Modal styles */
  .modal-overlay {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.7);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 9999;
    animation: fadeIn 0.2s ease-out;
  }

  @keyframes fadeIn {
    from {
      opacity: 0;
    }
    to {
      opacity: 1;
    }
  }

  .modal-content {
    background: #1a1b23;
    border-radius: 8px;
    padding: 0;
    max-height: 80vh;
    min-width: 600px;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
    position: relative;
    z-index: 1002;
  }

  .modal-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 20px 24px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    background: #15161d;
  }

  .modal-title {
    margin: 0;
    font-size: 18px;
    font-weight: 600;
    color: rgba(255, 255, 255, 0.9);
  }

  .modal-close {
    background: transparent;
    border: none;
    color: rgba(255, 255, 255, 0.5);
    font-size: 28px;
    cursor: pointer;
    padding: 0;
    width: 32px;
    height: 32px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 4px;
    transition: all 0.2s;
  }

  .modal-close:hover {
    background: rgba(255, 255, 255, 0.1);
    color: rgba(255, 255, 255, 0.8);
  }

  .modal-body {
    padding: 24px;
    overflow-y: auto;
    flex: 1;
  }

  .modal-section {
    margin-bottom: 32px;
  }

  .modal-section:last-child {
    margin-bottom: 0;
  }

  .section-title {
    font-size: 14px;
    font-weight: 600;
    color: rgba(255, 255, 255, 0.9);
    margin: 0 0 16px 0;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .section-title.error-title {
    color: #ff6b6b;
  }

  .metrics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
  }

  .metric-item {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .metric-label {
    font-size: 11px;
    color: rgba(255, 255, 255, 0.5);
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .metric-value {
    font-size: 14px;
    color: rgba(255, 255, 255, 0.88);
    font-weight: 500;
    word-wrap: break-word;
    overflow-wrap: break-word;
  }

  .metric-value.reputation-score[data-score] {
    font-weight: 600;
  }

  .metric-value.reputation-score[data-score]:where([data-score^="9"], [data-score^="8"]) {
    color: #15b241;
  }

  .metric-value.reputation-score[data-score]:where([data-score^="7"], [data-score^="6"]) {
    color: #ff9800;
  }

  .metric-value.reputation-score[data-score]:where([data-score^="5"], [data-score^="4"]) {
    color: #ffa500;
  }

  .metric-value.reputation-score[data-score]:where([data-score^="3"], [data-score^="2"], [data-score^="1"], [data-score^="0"]) {
    color: #ff6b6b;
  }

  .metric-value.success {
    color: #15b241;
  }

  .metric-value.failure {
    color: #ff6b6b;
  }

  .metric-value.malicious {
    color: #ce1722;
    font-weight: 600;
  }

  .metric-value.peer-id {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    word-break: break-all;
  }

  .metric-value.url {
    font-size: 12px;
    word-break: break-all;
  }

  .error-details {
    background: rgba(255, 107, 107, 0.1);
    border-left: 3px solid #ff6b6b;
    padding: 12px;
    border-radius: 4px;
  }

  .error-time {
    font-size: 11px;
    color: rgba(255, 255, 255, 0.5);
    margin-bottom: 8px;
  }

  .error-message {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: rgba(255, 255, 255, 0.8);
    line-height: 1.5;
    word-wrap: break-word;
    overflow-wrap: break-word;
    word-break: break-all;
    white-space: pre-wrap;
  }

  :global(.catchup-view-link) {
    color: #1878ff;
    cursor: pointer;
    font-weight: 500;
    text-decoration: underline;
    transition: color 0.2s;
    display: inline-block;
    padding: 2px 4px;
  }

  :global(.catchup-view-link:hover) {
    color: #4a94ff;
    text-decoration: underline;
  }
</style>
