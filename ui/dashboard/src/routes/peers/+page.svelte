<script lang="ts">
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import { onMount, onDestroy } from 'svelte'
  import { messages, sock, connectionAttempts } from '$internal/stores/p2pStore'
  import { drawPeerNetwork, cleanupPeerNetwork } from './peerNetwork'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  let vis // binding with div for visualization
  let peerDataMap = new Map() // Store all peer data, indexed by peer_id
  let syncConnectionMap = new Map() // Store when we first connected to each sync peer
  let processedMessageCount = 0 // Track how many messages we've processed
  let hasReceivedMessages = false // Track if we've ever received any messages
  let currentNodePeerID = '' // Store our node's peer ID
  let messageHashes = new Map() // Track message hashes to detect duplicates
  let mounted = false // Track if component is mounted
  let firstNodeStatusReceived = false // Track if we've received the first node_status

  onMount(() => {
    mounted = true
    
    // Force a redraw if we have data
    if (peerDataMap.size > 0) {
      const nodes = Array.from(peerDataMap.values())
      
      if (vis && nodes.length > 0) {
        drawPeerNetwork(vis, nodes, currentNodePeerID, syncConnectionMap)
      }
    }
  })

  onDestroy(() => {
    mounted = false
    cleanupPeerNetwork()
  })

  // Process all P2P messages to discover and update peer data
  $: {
    // Only process new messages (messages added since last run)
    const currentMessageCount = $messages.length
    
    // If message count decreased (e.g., array was truncated), rebuild from scratch
    if (currentMessageCount < processedMessageCount) {
      peerDataMap.clear()
      processedMessageCount = 0
      currentNodePeerID = ''
      firstNodeStatusReceived = false
    }
    
    // Process only unprocessed messages
    // Messages are stored newest first, so new messages are at the beginning
    const newMessages = $messages.slice(0, currentMessageCount - processedMessageCount)
    
    // Track if any data actually changed
    let dataChanged = false
    
    // Process new messages (they're already in newest-first order)
    newMessages.forEach(msg => {
      // Extract peer_id from various message formats - normalize to ensure uniqueness
      let peerId = msg.peer_id || msg.peerID || msg.peer || msg.from
      
      // Skip messages without a valid peer identifier
      if (!peerId || peerId === 'undefined' || peerId === 'null') {
        console.warn('Skipping message with invalid peer_id:', msg)
        return
      }
      
      // Normalize peer ID format (trim whitespace, ensure consistent format)
      peerId = String(peerId).trim()
      
      // Additional validation
      if (!peerId || peerId.length === 0) {
        console.warn('Skipping message with empty peer_id after normalization')
        return
      }
      
      // Get or create peer data entry
      let peerData = peerDataMap.get(peerId)
      if (!peerData) {
        // First time seeing this peer - create entry
        peerData = {
          peer_id: peerId,
          first_seen: Date.now(),
          last_seen: Date.now()
        }
        peerDataMap.set(peerId, peerData)
        dataChanged = true
      }
      
      // Always update last_seen
      peerData.last_seen = Date.now()
      
      // Update peer data based on message type
      if (msg.type === 'node_status') {
        // Check for duplicate node_status
        const messageHash = `node_status:${peerId}:${msg.best_height || ''}:${msg.best_block_hash || ''}:${msg.fsm_state || ''}:${msg.sync_peer_id || ''}`
        
        const isDuplicate = peerData.best_height === msg.best_height && 
                           peerData.best_block_hash === msg.best_block_hash &&
                           peerData.fsm_state === msg.fsm_state &&
                           peerData.sync_peer_id === msg.sync_peer_id
        
        if (!isDuplicate) {
          // Track sync peer connections
          if (msg.sync_peer_id && msg.sync_peer_id !== peerData.sync_peer_id) {
            // New sync peer connection - record the time
            const connectionKey = `${peerId}->${msg.sync_peer_id}`
            if (!syncConnectionMap.has(connectionKey)) {
              syncConnectionMap.set(connectionKey, Date.now())
            }
          } else if (!msg.sync_peer_id && peerData.sync_peer_id) {
            // Sync has finished - remove the connection
            const connectionKey = `${peerId}->${peerData.sync_peer_id}`
            syncConnectionMap.delete(connectionKey)
          }
          
          // Update all node_status fields
          // IMPORTANT: We explicitly set sync_peer_id even if it's null/undefined
          // This ensures arrows disappear when syncing is complete
          Object.assign(peerData, {
            base_url: msg.base_url,
            version: msg.version,
            commit_hash: msg.commit_hash,
            best_block_hash: msg.best_block_hash,
            best_height: msg.best_height,
            tx_count_in_assembly: msg.tx_count_in_assembly,
            fsm_state: msg.fsm_state,
            start_time: msg.start_time,
            uptime: msg.uptime,
            miner_name: msg.miner_name,
            listen_mode: msg.listen_mode,
            sync_peer_id: msg.sync_peer_id || null,  // Explicitly set to null if undefined
            sync_peer_height: msg.sync_peer_height,
            sync_peer_block_hash: msg.sync_peer_block_hash,
            sync_connected_at: msg.sync_connected_at // Now coming from server
          })
          messageHashes.set(messageHash, Date.now())
          dataChanged = true
          
          // The very first node_status message we receive is from our own node
          // (sent immediately upon WebSocket connection)
          if (!firstNodeStatusReceived) {
            currentNodePeerID = peerId
            firstNodeStatusReceived = true
            console.log('Identified current node from first node_status:', currentNodePeerID)
          }
        }
        
      } else if (msg.type === 'miningon' || msg.type === 'mining_on') {
        // Check for duplicate miningOn
        const messageHash = `miningon:${peerId}:${msg.height || ''}:${msg.hash || ''}`
        
        const isDuplicate = peerData.height === msg.height && peerData.hash === msg.hash
        
        if (!isDuplicate) {
          // Update mining-related fields
          if (msg.height !== undefined) peerData.height = msg.height
          if (msg.hash) peerData.hash = msg.hash
          if (msg.miner) peerData.miner = msg.miner
          messageHashes.set(messageHash, Date.now())
          dataChanged = true
        }
        
      } else if (msg.type === 'PING' || msg.type === 'ping') {
        // PING just updates last_seen (already done above)
        dataChanged = true
        
      } else if (msg.type === 'block') {
        // Update block-related fields
        if (msg.height !== undefined) peerData.latest_block_height = msg.height
        if (msg.hash) peerData.latest_block_hash = msg.hash
        dataChanged = true
        
      } else if (msg.type === 'verack' || msg.type === 'version') {
        // Handshake messages - update connection info
        if (msg.best_height !== undefined) peerData.handshake_height = msg.best_height
        if (msg.best_hash) peerData.handshake_hash = msg.best_hash
        if (msg.user_agent) peerData.user_agent = msg.user_agent
        dataChanged = true
        
      } else if (msg.type === 'tx' || msg.type === 'transaction') {
        // Transaction propagation - just track activity
        peerData.last_tx_time = Date.now()
        dataChanged = true
        
      } else {
        // Unknown message type - still track that we saw activity from this peer
        dataChanged = true
      }
      
      hasReceivedMessages = true
    })
    
    // Clean up old message hashes periodically (older than 5 minutes)
    const now = Date.now()
    const fiveMinutesAgo = now - 5 * 60 * 1000
    for (const [hash, timestamp] of messageHashes.entries()) {
      if (timestamp < fiveMinutesAgo) {
        messageHashes.delete(hash)
      }
    }
    
    processedMessageCount = currentMessageCount

    // Only update visualization if data actually changed
    if (dataChanged) {
      // Convert peer data to nodes array for visualization
      const nodes = Array.from(peerDataMap.values())
      
      
      if (mounted && vis && nodes.length > 0) {
        drawPeerNetwork(vis, nodes, currentNodePeerID, syncConnectionMap)
      } else if (mounted && vis && nodes.length === 0) {
        // Clear the visualization if no nodes
        drawPeerNetwork(vis, [], currentNodePeerID, syncConnectionMap)
      }
    }
  }

  // Helper function to format uptime
  function formatUptime(startTime: number): string {
    if (!startTime) return 'Unknown'
    
    const now = Date.now() / 1000
    const uptime = now - startTime
    
    if (uptime < 60) return `${Math.floor(uptime)}s`
    if (uptime < 3600) return `${Math.floor(uptime / 60)}m`
    if (uptime < 86400) return `${Math.floor(uptime / 3600)}h`
    return `${Math.floor(uptime / 86400)}d`
  }
</script>

<PageWithMenu>
  <div class="content">
    <div class="header">
      <h1>
        {t('page.peers.title')}
        {#if $connectionAttempts > 0 && !$sock}
          <span class="connection-error">P2P connection failed. Attempt {$connectionAttempts}/5</span>
        {/if}
      </h1>
      <div class="stats">
        <span class="stat-item">
          <span class="stat-label">Total Peers:</span>
          <span class="stat-value">{peerDataMap.size}</span>
        </span>
        <span class="stat-item">
          <span class="stat-label">Active:</span>
          <span class="stat-value">{Array.from(peerDataMap.values()).filter(p => Date.now() - p.last_seen < 60000).length}</span>
        </span>
        <span class="stat-item">
          <span class="stat-label">Connected:</span>
          <span class="stat-value">{$sock ? '✓' : '✗'}</span>
        </span>
      </div>
    </div>
    <div id="peer-vis" bind:this={vis}>
      {#if !hasReceivedMessages && $sock}
        <div class="connection-status">
          <div class="status-message">Waiting for peer messages...</div>
          <div class="status-hint">Node status messages are sent every 10 seconds</div>
        </div>
      {/if}
    </div>
  </div>
</PageWithMenu>

<style>
  .content {
    width: 100%;
    height: calc(100vh - 100px); /* Full height minus nav */
    max-width: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    gap: 20px;
    padding: 20px;
  }

  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 20px;
    background: linear-gradient(0deg, rgba(255, 255, 255, 0.04) 0%, rgba(255, 255, 255, 0.04) 100%), #0a1018;
    border-radius: 12px;
  }

  h1 {
    color: rgba(255, 255, 255, 0.88);
    font-family: var(--font-family);
    font-size: 24px;
    font-weight: 700;
    margin: 0;
  }

  .connection-error {
    color: #ff6b6b;
    font-size: 14px;
    font-weight: 400;
  }

  .stats {
    display: flex;
    gap: 30px;
  }

  .stat-item {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .stat-label {
    color: rgba(255, 255, 255, 0.66);
    font-size: 14px;
  }

  .stat-value {
    color: #1878ff;
    font-size: 16px;
    font-weight: 600;
  }

  #peer-vis {
    flex: 1;
    width: 100%;
    background: linear-gradient(0deg, rgba(255, 255, 255, 0.02) 0%, rgba(255, 255, 255, 0.02) 100%), #0a1018;
    border-radius: 12px;
    padding: 20px;
    overflow: auto;
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .connection-status {
    text-align: center;
    padding: 40px;
  }

  .status-message {
    color: rgba(255, 255, 255, 0.88);
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 10px;
  }

  .status-hint {
    color: rgba(255, 255, 255, 0.5);
    font-size: 14px;
  }

  /* D3 styles */
  :global(.peer-node) {
    fill: #1a2332;
    stroke: #1878ff;
    stroke-width: 2px;
    cursor: pointer;
    transition: all 0.3s ease;
  }

  :global(.peer-node.connected) {
    fill: #1a2332;
    stroke: #1878ff;
    stroke-width: 2px;
  }

  :global(.peer-node.disconnected) {
    fill: #1a1a1a;
    stroke: #666;
    stroke-width: 1px;
    opacity: 0.6;
  }

  :global(.peer-node.current-node) {
    fill: #2a3f5a;
    stroke: #3d8eff;
    stroke-width: 3px;
    box-shadow: 0 0 20px rgba(61, 142, 255, 0.6);
  }

  :global(.peer-node:hover) {
    fill: #243040;
    stroke: #3d8eff;
    stroke-width: 3px;
    opacity: 1;
  }

  :global(.peer-node.selected) {
    fill: #2a3a4a;
    stroke: #3d8eff;
    stroke-width: 3px;
  }

  :global(.sync-link) {
    fill: none;
    /* Styles are now set dynamically in peerNetwork.ts for animation */
  }

  :global(.sync-link.inactive) {
    stroke: #666;
    stroke-dasharray: 5, 5;
    opacity: 0.3;
  }

  :global(.peer-text) {
    fill: rgba(255, 255, 255, 0.88);
    font-family: var(--font-family);
    font-size: 12px;
    pointer-events: none;
  }

  :global(.peer-text.label) {
    font-size: 10px;
    fill: rgba(255, 255, 255, 0.66);
  }

  :global(.peer-text.highlight) {
    fill: #1878ff;
    font-weight: 600;
  }

  :global(.uptime-text) {
    fill: #15B241;
    font-size: 10px;
    font-weight: 500;
  }

  :global(.connection-time) {
    fill: #ffa500;
    font-size: 10px;
    font-weight: 500;
  }

  :global(.tooltip) {
    position: absolute;
    text-align: left;
    padding: 12px;
    font-size: 12px;
    background: rgba(0, 0, 0, 0.9);
    border: 1px solid #1878ff;
    border-radius: 8px;
    pointer-events: none;
    color: white;
    z-index: 1000;
  }

  :global(.tooltip .label) {
    color: rgba(255, 255, 255, 0.66);
    font-size: 10px;
    margin-bottom: 4px;
  }

  :global(.tooltip .value) {
    color: rgba(255, 255, 255, 0.88);
    font-size: 12px;
    margin-bottom: 8px;
    word-break: break-all;
  }
</style>