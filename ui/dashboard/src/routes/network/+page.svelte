<script lang="ts">
  import { onMount } from 'svelte'
  import { page } from '$app/stores'
  import { goto } from '$app/navigation'
  import { browser } from '$app/environment'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import ConnectedNodesCard from '$internal/components/page/network/connected-nodes-card/index.svelte'
  import { calculateChainworkScores } from '$internal/components/page/network/connected-nodes-card/data'

  import { miningNodes, sock, currentNodePeerID } from '$internal/stores/p2pStore'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  $: connected = $sock !== null

  let nodes: any[] = []
  let allNodes: any[] = [] // Full dataset
  let paginatedNodes: any[] = [] // Sliced data for current page
  
  // Persistent pagination state that survives data updates
  let currentPage = 1
  let currentPageSize = 10
  
  // Persistent sort state
  let sortColumn = ''
  let sortOrder = ''
  
  // Load sort from localStorage on mount
  if (browser) {
    const savedSort = loadSortFromStorage()
    if (savedSort) {
      sortColumn = savedSort.sortColumn
      sortOrder = savedSort.sortOrder
    }
  }
  
  // Cache for chainwork calculations to avoid recalculation
  let chainworkCache = new Map<string, Map<string, number>>()
  
  // Force update of paginatedNodes when tick changes to update relative time displays
  $: if (tickCounter >= 0 && allNodes.length > 0) {
    updatePaginatedNodes()
  }
  
  // Local storage keys for persistence
  const NETWORK_PAGE_SIZE_KEY = 'teranode-network-pagesize'
  const NETWORK_SORT_KEY = 'teranode-network-sort'

  // Load pageSize from localStorage
  function loadPageSizeFromStorage(): number {
    if (browser) {
      try {
        const stored = localStorage.getItem(NETWORK_PAGE_SIZE_KEY)
        if (stored) {
          const parsed = parseInt(stored, 10)
          if (parsed > 0 && parsed <= 100) { // Reasonable bounds
            return parsed
          }
        }
      } catch (error) {
        console.warn('Failed to load pageSize from localStorage:', error)
      }
    }
    return 10 // Default value
  }
  
  // Load sort preferences from localStorage
  function loadSortFromStorage(): { sortColumn: string; sortOrder: string } | null {
    if (browser) {
      try {
        const stored = localStorage.getItem(NETWORK_SORT_KEY)
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
        localStorage.setItem(NETWORK_PAGE_SIZE_KEY, size.toString())
      } catch (error) {
        console.warn('Failed to save pageSize to localStorage:', error)
      }
    }
  }
  
  // Save sort to localStorage
  function saveSortToStorage(sortColumn: string, sortOrder: string) {
    if (browser) {
      try {
        localStorage.setItem(NETWORK_SORT_KEY, JSON.stringify({ sortColumn, sortOrder }))
      } catch (error) {
        console.warn('Failed to save sort to localStorage:', error)
      }
    }
  }

  // Get pagination from URL and localStorage with priority: URL > localStorage > Default
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
    updatePaginatedNodes()
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

  function onPageChange(e) {
    const data = e.detail
    const newPage = data.value.page
    const newPageSize = data.value.pageSize
    
    // Save pageSize to localStorage if it changed (but only when user actively changes it)
    if (newPageSize !== currentPageSize) {
      savePageSizeToStorage(newPageSize)
    }
    
    currentPage = newPage
    currentPageSize = newPageSize
    updatePaginatedNodes()
    updateURL(newPage, newPageSize)
  }
  
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
          localStorage.removeItem(NETWORK_SORT_KEY)
        } catch (error) {
          console.warn('Failed to remove sort from localStorage:', error)
        }
      }
    }
  }

  function updatePaginatedNodes() {
    const startIndex = (currentPage - 1) * currentPageSize
    const endIndex = startIndex + currentPageSize
    paginatedNodes = allNodes.slice(startIndex, endIndex)
  }
  
  // Deep equality check for node objects (ignoring receivedAt)
  function nodesEqual(a: any, b: any): boolean {
    if (a.peer_id !== b.peer_id) return false
    if (a.base_url !== b.base_url) return false
    if (a.chain_work !== b.chain_work) return false
    if (a.best_height !== b.best_height) return false
    if (a.best_block_hash !== b.best_block_hash) return false
    if (a.version !== b.version) return false
    if (a.fsm_state !== b.fsm_state) return false
    if (a.tx_count !== b.tx_count) return false
    if (a.subtree_count !== b.subtree_count) return false
    if (a.uptime !== b.uptime) return false
    if (a.listen_mode !== b.listen_mode) return false
    if (a.client_name !== b.client_name) return false
    if (a.miner_name !== b.miner_name) return false
    if (a.start_time !== b.start_time) return false
    if (a.min_mining_tx_fee !== b.min_mining_tx_fee) return false
    if (a.connected_peers_count !== b.connected_peers_count) return false
    return true
  }

  function updateData() {
    const mNodes: any[] = []
    Object.values($miningNodes).forEach((node) => {
      // Filter out nodes without version or height information
      if (node.version || node.best_height || node.height) {
        mNodes.push(node)
      }
    })

    // Build cache key from chainwork values
    const chainworkKey = mNodes.map(n => `${n.peer_id}:${n.chain_work || ''}`).sort().join('|')
    
    // Use cached chainwork scores if available
    let chainworkScores: Map<string, number>

    if (chainworkCache.has(chainworkKey)) {
      chainworkScores = chainworkCache.get(chainworkKey)!
    } else {
      // Calculate and cache chainwork scores
      chainworkScores = calculateChainworkScores(mNodes)
      chainworkCache.set(chainworkKey, chainworkScores)

      // Keep cache size manageable (only last 10 states)
      if (chainworkCache.size > 10) {
        const firstKey = chainworkCache.keys().next().value
        chainworkCache.delete(firstKey)
      }
    }
    
    // Get current node peer ID
    const currentPeerID = $currentNodePeerID

    // Add scores to each node and update isCurrentNode flag
    mNodes.forEach((node) => {
      const key = node.peer_id
      node.chainwork_score = chainworkScores.get(key) || 0
      // Update isCurrentNode based on reactive store
      node.isCurrentNode = node.peer_id === currentPeerID
    })

    // Sort: current node first (only when no user sorting is active), then by base_url, then by peer_id (case-insensitive)
    const sorted = mNodes.sort((a: any, b: any) => {
      // Only put current node at the top when there's no active user sorting
      if (!sortColumn || !sortOrder) {
        if (a.isCurrentNode) return -1
        if (b.isCurrentNode) return 1
      }

      const aUrl = (a.base_url || '').toLowerCase()
      const bUrl = (b.base_url || '').toLowerCase()
      const aPeerId = (a.peer_id || '').toLowerCase()
      const bPeerId = (b.peer_id || '').toLowerCase()

      if (aUrl < bUrl) {
        return -1
      } else if (aUrl > bUrl) {
        return 1
      } else {
        // Secondary sort by peer_id for stability
        if (aPeerId < bPeerId) {
          return -1
        } else if (aPeerId > bPeerId) {
          return 1
        }
        return 0
      }
    })

    // Only update if data actually changed (deep equality check)
    let hasChanges = false
    if (allNodes.length !== sorted.length) {
      hasChanges = true
    } else {
      // Check if any node has changed
      for (let i = 0; i < sorted.length; i++) {
        if (!nodesEqual(sorted[i], allNodes[i])) {
          hasChanges = true
          break
        }
      }
    }
    
    if (hasChanges) {
      allNodes = sorted
      nodes = sorted // Keep for backward compatibility
      updatePaginatedNodes()
    }
  }

  // Subscribe to miningNodes changes from WebSocket
  $: if ($miningNodes) {
    updateData()
  }
  
  // Also subscribe to currentNodePeerID changes to update isCurrentNode flags
  $: if ($currentNodePeerID) {
    updateData()
  }
  
  // Add a ticker ONLY for re-rendering the relative time display
  // This doesn't recalculate data, just forces component updates
  let tickCounter = 0
  onMount(() => {
    const ticker = setInterval(() => {
      tickCounter++
    }, 1000)
    
    return () => clearInterval(ticker)
  })
</script>

<PageWithMenu>
  <ConnectedNodesCard 
    data={paginatedNodes}
    allData={allNodes}
    {connected} 
    page={currentPage}
    pageSize={currentPageSize}
    {sortColumn}
    {sortOrder}
    on:pagechange={onPageChange}
    on:sort={onSort}
  />
</PageWithMenu>
