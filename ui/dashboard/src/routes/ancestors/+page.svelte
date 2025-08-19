<script lang="ts">
  import { onMount } from 'svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import Table from '$lib/components/table/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import Icon from '$lib/components/icon/index.svelte'
  import { Button } from '$lib/components'
  import { miningNodes, currentNodePeerID, sock } from '$internal/stores/p2pStore'
  import { calculateChainworkScores } from '$internal/components/page/network/connected-nodes-card/data'
  import i18n from '$internal/i18n'
  import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'
  import RenderSpanWithTooltip from '$lib/components/table/renderers/render-span-with-tooltip/index.svelte'
  import RenderHashWithMiner from '$lib/components/table/renderers/render-hash-with-miner/index.svelte'
  
  $: t = $i18n.t
  
  const pageKey = 'page.ancestors'
  const fieldKey = `${pageKey}.fields`
  
  let blockLocator: string[] = []
  let isLoadingLocator = false
  let data: any[] = []
  let findingAncestors = false
  let ancestorErrors: Map<string, string> = new Map()
  let ancestorData: Map<string, {hash: string, height: number | null, miner?: string}> = new Map()
  let hasInitiallyFetchedAncestors = false
  let fetchedPeers: Set<string> = new Set()
  
  $: connected = $sock !== null
  
  // Update table data
  function updateData() {
    const nodes: any[] = []
    
    // Only process if we have real peer data
    if ($miningNodes && Object.keys($miningNodes).length > 0) {
      Object.values($miningNodes).forEach((node) => {
        nodes.push(node)
      })
    }

    // Calculate chainwork scores if we have data
    if (nodes.length > 0) {
      const chainworkScores = calculateChainworkScores(nodes)
      const maxScore = Math.max(...Array.from(chainworkScores.values()))
      
      // Get current node peer ID
      const currentPeerID = $currentNodePeerID

      // Add scores and restore ancestor data if we have it
      nodes.forEach((node) => {
        const key = node.peer_id
        node.chainwork_score = chainworkScores.get(key) || 0
        node.maxChainworkScore = maxScore
        node.isCurrentNode = node.peer_id === currentPeerID
        
        // For current node, immediately populate with its best hash
        if (node.isCurrentNode) {
          node.common_block_hash = node.best_block_hash || ''
          node.common_block_height = node.best_height
          // Also store it in ancestorData so it persists
          ancestorData.set(node.peer_id, {hash: node.best_block_hash || '', height: node.best_height})
          // Mark as fetched so we don't process it again
          fetchedPeers.add(node.peer_id)
        } else {
          // Restore ancestor data from our map if we have it
          const ancestor = ancestorData.get(node.peer_id)
          if (ancestor) {
            node.common_block_hash = ancestor.hash
            node.common_block_height = ancestor.height
            node.common_block_miner = ancestor.miner || ''
          } else {
            // Add placeholder common ancestor fields if not already set
            node.common_block_hash = ''
            node.common_block_height = null
            node.common_block_miner = ''
          }
        }
      })
    }

    // Sort: current node first, then by chainwork score
    const sorted = nodes.sort((a: any, b: any) => {
      if (a.isCurrentNode) return -1
      if (b.isCurrentNode) return 1
      return (b.chainwork_score || 0) - (a.chainwork_score || 0)
    })

    data = sorted
  }
  
  // Fetch block locator from backend
  async function fetchBlockLocator() {
    isLoadingLocator = true
    
    try {
      // Use the Asset Server's block_locator API endpoint directly
      const response = await fetch('/api/v1/block_locator')
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }
      
      const data = await response.json()
      
      if (data.block_locator && Array.isArray(data.block_locator)) {
        blockLocator = data.block_locator
      } else if (Array.isArray(data)) {
        blockLocator = data
      } else {
        throw new Error('Invalid block locator format')
      }
    } catch (error) {
      console.error('Failed to fetch block locator:', error)
      blockLocator = []
    } finally {
      isLoadingLocator = false
    }
  }
  
  // Refresh everything - block locator, peer data, and common ancestors
  async function refreshAll() {
    // Clear the fetched peers set to re-fetch all
    fetchedPeers.clear()
    
    // Update peer data first
    updateData()
    
    // Fetch fresh block locator
    await fetchBlockLocator()
    
    // Find common ancestors
    if (blockLocator.length > 0 && data.length > 0) {
      await findCommonAncestors()
    }
  }
  
  // Fetch common ancestor for a single peer
  async function fetchAncestorForPeer(peer: any, index: number) {
    if (!blockLocator.length) {
      return
    }
    
    // Mark this peer as fetched
    fetchedPeers.add(peer.peer_id)
    
    // Create block locator hashes string (concatenated without separators)
    const blockLocatorHashesStr = blockLocator.join('')
    
    // Skip ourselves - use our best hash as the common ancestor
    if (peer.isCurrentNode) {
      peer.common_block_hash = peer.best_block_hash || ''
      peer.common_block_height = peer.best_height
      ancestorData.set(peer.peer_id, {hash: peer.best_block_hash || '', height: peer.best_height})
      data[index] = { ...peer }
      data = [...data]
      return
    }
    
    // Use base_url instead of data_hub_url (that's what comes from the WebSocket)
    const dataHubUrl = peer.base_url || peer.data_hub_url
    
    // Debug: uncomment to see peer data
    // console.log(`Processing peer ${peer.peer_id}:`, {
    //   base_url: peer.base_url,
    //   data_hub_url: peer.data_hub_url,
    //   dataHubUrl: dataHubUrl,
    //   best_block_hash: peer.best_block_hash,
    //   best_height: peer.best_height,
    //   isCurrentNode: peer.isCurrentNode
    // })
    
    if (!peer.best_block_hash) {
      peer.common_block_hash = 'No best hash'
      peer.common_block_height = null
      ancestorData.set(peer.peer_id, {hash: 'No best hash', height: null})
      data[index] = { ...peer }
      data = [...data]
      return
    }
    
    if (!dataHubUrl) {
      peer.common_block_hash = 'No DataHub URL'
      peer.common_block_height = null
      ancestorData.set(peer.peer_id, {hash: 'No DataHub URL', height: null})
      data[index] = { ...peer }
      data = [...data]
      return
    }
    
    try {
      // Call the peer's DataHub directly (CORS is enabled on all nodes)
      // base_url already includes /api/v1
      const targetUrl = `${dataHubUrl}/headers_from_common_ancestor/${peer.best_block_hash}/json?block_locator_hashes=${blockLocatorHashesStr}&n=1`
      
      // Debug: uncomment to see request details
      // console.log(`Calling peer ${peer.peer_id} directly:`, {
      //   targetUrl: targetUrl,
      //   blockLocatorLength: blockLocator.length,
      //   blockLocatorHashesLength: blockLocatorHashesStr.length
      // })
      
      const response = await fetch(targetUrl, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
        },
      })
      
      if (!response.ok) {
        const errorText = await response.text()
        
        // Check if it's a 404 due to no common block found
        if (response.status === 404) {
          try {
            const errorData = JSON.parse(errorText)
            // Check for error message in the response
            const errorMessage = errorData.error || errorData.message || ''
            if (errorMessage.includes('BLOCK_NOT_FOUND') || 
                errorMessage.includes('not found') || 
                errorMessage.includes('Not Found')) {
              // This is expected when no common ancestor is found
              peer.common_block_hash = 'No common block'
              peer.common_block_height = null
              ancestorData.set(peer.peer_id, {hash: 'No common block', height: null})
              data[index] = { ...peer }
              data = [...data]
              return
            }
          } catch (e) {
            // Not JSON or doesn't match expected format - treat as no common block
            peer.common_block_hash = 'No common block'
            peer.common_block_height = null
            ancestorData.set(peer.peer_id, {hash: 'No common block', height: null})
            data[index] = { ...peer }
            data = [...data]
            return
          }
        }
          
        // Debug: uncomment to see error details
        // console.error(`Request failed for peer ${peer.peer_id}:`, {
        //   status: response.status,
        //   statusText: response.statusText,
        //   errorBody: errorText,
        //   targetUrl: targetUrl
        // })
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }
        
      const headers = await response.json()
      
      if (headers && headers.length > 0) {
        // The first header is the common ancestor
        const commonAncestor = headers[0]
        peer.common_block_hash = commonAncestor.hash || ''
        peer.common_block_height = commonAncestor.height || null
        peer.common_block_miner = commonAncestor.miner || ''
        ancestorData.set(peer.peer_id, {
          hash: commonAncestor.hash || '', 
          height: commonAncestor.height || null,
          miner: commonAncestor.miner || ''
        })
      } else {
        // No common ancestor found
        peer.common_block_hash = 'No common block'
        peer.common_block_height = null
        ancestorData.set(peer.peer_id, {hash: 'No common block', height: null})
      }
    } catch (error) {
      // Debug: uncomment to see error details
      // console.error(`Error finding common ancestor with peer ${peer.peer_id}:`, error)
      ancestorErrors.set(peer.peer_id, error instanceof Error ? error.message : 'Unknown error')
      peer.common_block_hash = 'Error'
      peer.common_block_height = null
      ancestorData.set(peer.peer_id, {hash: 'Error', height: null})
    }
      
    // Update the specific row in the data array to trigger reactive update
    data[index] = { ...peer }
    data = [...data]
  }
  
  // Find common ancestors with all peers
  async function findCommonAncestors() {
    if (!blockLocator.length || !data.length) {
      return
    }
    
    findingAncestors = true
    ancestorErrors.clear()
    
    // Process all peers concurrently using Promise.allSettled
    const promises = data.map((peer, index) => 
      fetchAncestorForPeer(peer, index)
    )
    
    // Wait for all promises to complete (whether they succeed or fail)
    await Promise.allSettled(promises)
    
    findingAncestors = false
  }
  
  // Column definitions for the table
  function getColDefs() {
    return [
      {
        id: 'client_name',
        name: 'Node Name',
        type: 'string',
        props: {
          width: '15%',
        },
      },
      {
        id: 'best_height',
        name: 'Best Height',
        type: 'number',
        props: {
          width: '12%',
        },
      },
      {
        id: 'best_block_hash',
        name: 'Best Hash',
        type: 'string',
        props: {
          width: '18%',
        },
      },
      {
        id: 'chainwork_score',
        name: 'Chainwork',
        type: 'number',
        props: {
          width: '10%',
        },
      },
      {
        id: 'common_block_hash',
        name: 'Common Block',
        type: 'string',
        props: {
          width: '18%',
        },
      },
      {
        id: 'common_block_height',
        name: 'Common Height',
        type: 'number',
        props: {
          width: '15%',
        },
      },
    ]
  }
  
  $: colDefs = getColDefs()
  
  // Custom render functions using RenderSpan component
  const renderCells = {
    client_name: (idField, item, colId) => {
      const clientName = item[colId] || item.client_name || '(not set)'
      const url = item.base_url || '-'
      const isCurrentNode = item.isCurrentNode === true
      
      return {
        component: RenderSpanWithTooltip,
        props: {
          value: clientName,
          className: isCurrentNode ? 'current-node-name' : '',
          tooltip: url,
        },
        value: '',
      }
    },
    best_height: (idField, item, colId) => {
      const value = item[colId]
      return {
        component: RenderSpan,
        props: {
          value: value ? value.toLocaleString() : '-',
          className: 'num',
        },
        value: '',
      }
    },
    best_block_hash: (idField, item, colId) => {
      const value = item[colId]
      const shortHash = value ? (value.length > 16 ? `${value.slice(0, 8)}...${value.slice(-8)}` : value) : ''
      const miner = item.miner_name || ''
      return {
        component: value ? RenderHashWithMiner : null,
        props: {
          hash: value,
          hashUrl: '',  // No link for ancestors page
          shortHash: shortHash,
          miner: miner,
          showCopyButton: true,
          copyTooltip: 'Copy hash',
          tooltip: value,
        },
        value: '',
      }
    },
    chainwork_score: (idField, item, colId) => {
      const score = item[colId] || 0
      const maxScore = item.maxChainworkScore || 0
      const isTopScore = score > 0 && score === maxScore
      
      let displayValue = '-'
      let className = 'num'
      
      if (score > 0) {
        displayValue = score.toString()
        // Use same CSS classes as network tab for coloring
        className = isTopScore ? 'chainwork-score-top num' : 'chainwork-score-other num'
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
    common_block_hash: (idField, item, colId) => {
      const value = item[colId]
      
      // Handle special cases (errors, no common block)
      if (value === 'Error' || value === 'No common block' || value === 'No DataHub URL' || value === 'No best hash') {
        return {
          component: RenderSpan,
          props: {
            value: value,
            className: value === 'Error' ? 'error-text' : 'warning-text',
          },
          value: '',
        }
      }
      
      // Handle normal hash display
      const shortHash = value ? (value.length > 16 ? `${value.slice(0, 8)}...${value.slice(-8)}` : value) : ''
      const miner = item.common_block_miner || ''
      return {
        component: value ? RenderHashWithMiner : null,
        props: {
          hash: value,
          hashUrl: '',  // No link for ancestors page
          shortHash: shortHash,
          miner: miner,  // Use miner if available from the common block metadata
          showCopyButton: true,
          copyTooltip: 'Copy hash',
          tooltip: value,
        },
        value: '',
      }
    },
    common_block_height: (idField, item, colId) => {
      const value = item[colId]
      const hashValue = item.common_block_hash
      
      // Don't show height if there was an error or no common block
      if (hashValue === 'Error' || hashValue === 'No common block') {
        return {
          component: RenderSpan,
          props: {
            value: '-',
            className: 'muted',
          },
          value: '',
        }
      }
      
      return {
        component: RenderSpan,
        props: {
          value: value ? value.toLocaleString() : '-',
          className: value ? 'num' : 'muted',
        },
        value: '',
      }
    },
    base_url: (idField, item, colId) => {
      const value = item[colId]
      return {
        component: RenderSpan,
        props: {
          value: value || '-',
          className: value ? 'url' : 'muted',
        },
        value: '',
      }
    },
    best_height: (idField, item, colId) => {
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
  }
  
  // React to p2pStore changes - table stays reactive
  $: {
    if ($miningNodes) {
      updateData()
    }
  }
  
  // Fetch ancestors for any peers that haven't been fetched yet
  $: {
    if (blockLocator.length > 0 && data.length > 0) {
      data.forEach((peer, index) => {
        // If we haven't fetched ancestor for this peer yet, do it now
        if (!fetchedPeers.has(peer.peer_id) && !peer.isCurrentNode) {
          fetchAncestorForPeer(peer, index)
        }
      })
      
      // Mark that we've done initial fetching
      if (!hasInitiallyFetchedAncestors) {
        hasInitiallyFetchedAncestors = true
      }
    }
  }
  
  onMount(async () => {
    updateData()
    // Fetch the block locator on mount
    await fetchBlockLocator()
  })
</script>

<PageWithMenu>
  <Card contentPadding="0">
    <div class="title" slot="title">
      <Typo variant="title" size="h4" value={t(`${pageKey}.title`, { defaultValue: 'Common Ancestors' })} />
    </div>
    <svelte:fragment slot="header-tools">
      {#if data.length > 0}
        <Button
          size="small"
          on:click={refreshAll}
          disabled={findingAncestors || isLoadingLocator}
        >
          {findingAncestors || isLoadingLocator ? 'Refreshing...' : 'Refresh'}
        </Button>
      {/if}
      <div class="live">
        <div class="live-icon" class:connected>
          <Icon name="icon-status-light-glow-solid" size={14} />
        </div>
        <div class="live-label">{t(`page.network.live`)}</div>
      </div>
    </svelte:fragment>
    {#if !connected}
      <div class="no-data">
        <Icon name="icon-status-light-glow-solid" size={48} color="rgba(255, 255, 255, 0.2)" />
        <p>WebSocket connection unavailable</p>
        <p class="sub">Please ensure the WebSocket service is running</p>
      </div>
    {:else if data.length === 0}
      <div class="no-data">
        <Icon name="icon-status-light-glow-solid" size={48} color="rgba(255, 255, 255, 0.2)" />
        <p>No peers available</p>
        <p class="sub">Waiting for peer connections...</p>
      </div>
    {:else}
      <Table
        name="ancestors"
        variant="dynamic"
        idField="peer_id"
        {colDefs}
        {data}
        pagination={{
          page: 1,
          pageSize: -1,
        }}
        i18n={{ t, baseKey: 'comp.pager' }}
        pager={false}
        expandUp={true}
        {renderCells}
        getRowIconActions={null}
        on:action={() => {}}
      />
    {/if}
  </Card>
</PageWithMenu>

<style>
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

  .title {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  
  /* Custom styles for table cells */
  :global(.current-node-name) {
    color: #4a9eff !important;
    font-weight: bold;
  }
  
  /* Right-align numeric columns */
  :global(.num) {
    text-align: right !important;
    display: block !important;
    width: 100% !important;
  }
  
  :global(.chainwork-score-top.num),
  :global(.chainwork-score-other.num) {
    text-align: right !important;
  }
  
  /* Right-align numeric column headers */
  :global(th:nth-child(2)), /* Best Height */
  :global(th:nth-child(4)), /* Chainwork */
  :global(th:nth-child(6)) { /* Common Height */
    text-align: right !important;
  }
  
  :global(th:nth-child(2) .table-cell-row),
  :global(th:nth-child(4) .table-cell-row),
  :global(th:nth-child(6) .table-cell-row) {
    justify-content: flex-end !important;
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
  
  :global(.muted) {
    color: rgba(255, 255, 255, 0.44);
  }
  
  :global(.error-text) {
    color: #ff6b6b;
  }
  
  :global(.warning-text) {
    color: #ffa500;
  }
  
  /* Chainwork score coloring - same as network tab */
  :global(.chainwork-score-top) {
    color: #4caf50 !important;
    font-weight: bold;
  }
  
  :global(.chainwork-score-other) {
    color: rgba(255, 255, 255, 0.66);
  }
  
  :global(.num) {
    text-align: right;
    font-variant-numeric: tabular-nums;
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
</style>