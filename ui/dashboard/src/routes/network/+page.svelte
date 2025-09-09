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
  
  // Local storage key for pageSize persistence
  const NETWORK_PAGE_SIZE_KEY = 'teranode-network-pagesize'

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

  function updatePaginatedNodes() {
    const startIndex = (currentPage - 1) * currentPageSize
    const endIndex = startIndex + currentPageSize
    paginatedNodes = allNodes.slice(startIndex, endIndex)
  }

  function updateData() {
    const mNodes: any[] = []
    Object.values($miningNodes).forEach((node) => {
      mNodes.push(node)
    })

    // Calculate chainwork scores
    const chainworkScores = calculateChainworkScores(mNodes)
    const maxScore = Math.max(...Array.from(chainworkScores.values()))
    
    // Get current node peer ID
    const currentPeerID = $currentNodePeerID

    // Add scores to each node and update isCurrentNode flag
    mNodes.forEach((node) => {
      const key = node.peer_id
      node.chainwork_score = chainworkScores.get(key) || 0
      node.maxChainworkScore = maxScore
      // Update isCurrentNode based on reactive store
      node.isCurrentNode = node.peer_id === currentPeerID
    })

    const sorted = mNodes.sort((a: any, b: any) => {
      if (a.base_url < b.base_url) {
        return -1
      } else if (a.base_url > b.base_url) {
        return 1
      } else {
        return 0
      }
    })

    allNodes = sorted
    nodes = sorted // Keep for backward compatibility
    updatePaginatedNodes()
  }

  onMount(() => {
    updateData()

    const interval = setInterval(() => {
      updateData()
    }, 1000)

    return () => clearInterval(interval)
  })
</script>

<PageWithMenu>
  <ConnectedNodesCard 
    data={paginatedNodes}
    allData={allNodes}
    {connected} 
    page={currentPage}
    pageSize={currentPageSize}
    on:pagechange={onPageChange}
  />
</PageWithMenu>
