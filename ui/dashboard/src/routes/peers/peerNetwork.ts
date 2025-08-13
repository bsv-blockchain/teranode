import * as d3 from 'd3'
import { humanTime } from '$internal/utils/format'

interface PeerNode {
  peer_id: string
  base_url?: string
  best_block_hash?: string
  best_height?: number
  start_time?: number
  uptime?: number
  sync_peer_id?: string
  sync_peer_height?: number
  sync_peer_block_hash?: string
  sync_connected_at?: number
  fsm_state?: string
  miner_name?: string
  version?: string
  receivedAt?: string
  // From miningOn messages
  height?: number
  hash?: string
}

interface ConnectionInfo {
  connectedAt?: number
  lastSeen?: number
}

// Store connection information between peers
const connectionMap = new Map<string, ConnectionInfo>()

// Store the simulation and visualization state
let currentSimulation: d3.Simulation<any, any> | null = null
let svg: d3.Selection<any, any, any, any> | null = null
let g: d3.Selection<any, any, any, any> | null = null
let nodeGroup: d3.Selection<any, any, any, any> | null = null
let linkGroup: d3.Selection<any, any, any, any> | null = null
let tooltip: d3.Selection<any, any, any, any> | null = null

export function drawPeerNetwork(
  selector: HTMLElement,
  nodes: PeerNode[],
  currentNodePeerID?: string,
  persistentConnectionMap?: Map<string, number>,
) {
  // Check if selector is still in DOM
  if (!selector || !document.body.contains(selector)) {
    return
  }

  // Filter out any nodes with invalid peer_ids
  const validNodes = nodes.filter((node) => {
    if (
      !node ||
      !node.peer_id ||
      node.peer_id === 'undefined' ||
      node.peer_id === 'null' ||
      String(node.peer_id).trim().length === 0
    ) {
      console.warn('Filtering out invalid node:', node)
      return false
    }
    return true
  })

  // Use validNodes instead of nodes from here on
  nodes = validNodes

  if (!nodes || nodes.length === 0) {
    if (svg) {
      svg.remove()
      svg = null
      g = null
      nodeGroup = null
      linkGroup = null
      currentSimulation = null
      if (tooltip) {
        tooltip.remove()
        tooltip = null
      }
    }
    return
  }

  const width = selector.clientWidth || 1200
  const height = selector.clientHeight || 600
  const nodeWidth = 220
  const nodeHeight = 140

  // Create a map for quick lookup
  const nodeMap = new Map<
    string,
    PeerNode & { x?: number; y?: number; fx?: number | null; fy?: number | null }
  >()

  // Preserve positions from existing nodes if we have them
  const existingNodes = currentSimulation ? currentSimulation.nodes() : []
  const existingPositions = new Map<
    string,
    { x: number; y: number; fx?: number | null; fy?: number | null }
  >()
  existingNodes.forEach((n: any) => {
    if (n.peer_id) {
      existingPositions.set(n.peer_id, { x: n.x, y: n.y, fx: n.fx, fy: n.fy })
    }
  })

  // Prepare nodes with preserved positions
  nodes.forEach((node) => {
    if (node.peer_id) {
      // Check for duplicates before adding to map
      if (nodeMap.has(node.peer_id)) {
        return // Skip duplicate
      }
      const position = existingPositions.get(node.peer_id)
      // If no existing position, create a better initial position
      // For disconnected nodes, place them in a grid pattern on the sides
      let initialX = width / 2 + (Math.random() - 0.5) * 200
      let initialY = height / 2 + (Math.random() - 0.5) * 200

      if (!position) {
        // Check if this is our current node - if so, ALWAYS center it regardless of sync status
        if (currentNodePeerID && node.peer_id === currentNodePeerID) {
          initialX = width / 2
          initialY = height / 2
        } else {
          // Check if this will be a disconnected node
          const willBeDisconnected =
            !node.sync_peer_id && !nodes.some((n) => n.sync_peer_id === node.peer_id)

          if (willBeDisconnected) {
            // Place disconnected nodes in a grid pattern on the right side
            const disconnectedCount = nodes
              .filter(
                (n) => !n.sync_peer_id && !nodes.some((other) => other.sync_peer_id === n.peer_id),
              )
              .indexOf(node)

            const cols = 2
            const rowSpacing = 150
            const colSpacing = 250
            const startX = width - 300 // Right side of screen
            const startY = 100

            const col = disconnectedCount % cols
            const row = Math.floor(disconnectedCount / cols)

            initialX = startX + col * colSpacing
            initialY = startY + row * rowSpacing
          }
        }
      }

      // Always fix our node to the center
      const isCurrentNode = currentNodePeerID && node.peer_id === currentNodePeerID

      const nodeWithPosition = {
        ...node,
        x: isCurrentNode ? width / 2 : position?.x ?? initialX,
        y: isCurrentNode ? height / 2 : position?.y ?? initialY,
        fx: isCurrentNode ? width / 2 : position?.fx,
        fy: isCurrentNode ? height / 2 : position?.fy,
      }
      nodeMap.set(node.peer_id, nodeWithPosition)

      // Update connection tracking using persistent connection times
      if (node.sync_peer_id) {
        const connectionKey = `${node.peer_id}->${node.sync_peer_id}`

        // Use the persistent connection time if available
        if (persistentConnectionMap && persistentConnectionMap.has(connectionKey)) {
          const connectedAt = persistentConnectionMap.get(connectionKey)!
          if (!connectionMap.has(connectionKey)) {
            connectionMap.set(connectionKey, { connectedAt, lastSeen: Date.now() })
          } else {
            connectionMap.get(connectionKey)!.lastSeen = Date.now()
          }
        } else if (!connectionMap.has(connectionKey)) {
          // Fallback to current time if no persistent time available
          const now = Date.now()
          connectionMap.set(connectionKey, { connectedAt: now, lastSeen: now })
        } else {
          // Just update lastSeen for existing connections
          connectionMap.get(connectionKey)!.lastSeen = Date.now()
        }
      }
    }
  })

  // Create links based on sync_peer_id
  // Note: Data flows FROM the sync peer TO the node that's catching up
  // The arrow points AT the receiving node (the one catching up)
  const links: any[] = []
  nodeMap.forEach((node) => {
    if (node.sync_peer_id && nodeMap.has(node.sync_peer_id)) {
      links.push({
        source: node.sync_peer_id, // The peer providing data (source)
        target: node.peer_id, // The node that is catching up (receiver)
        connectionInfo: connectionMap.get(`${node.peer_id}->${node.sync_peer_id}`),
      })
    }
  })

  // Initialize SVG if not exists
  if (!svg) {
    svg = d3
      .select(selector)
      .append('svg')
      .attr('width', width)
      .attr('height', height)
      .attr('viewBox', `0 0 ${width} ${height}`)

    // Define arrow marker for directed edges
    const defs = svg.append('defs')

    // Arrow marker
    defs
      .append('marker')
      .attr('id', 'arrowhead')
      .attr('viewBox', '-0 -5 10 10')
      .attr('refX', 8)
      .attr('refY', 0)
      .attr('orient', 'auto')
      .attr('markerWidth', 8)
      .attr('markerHeight', 8)
      .append('svg:path')
      .attr('d', 'M 0,-5 L 10,0 L 0,5')
      .attr('fill', '#15B241')
      .style('stroke', 'none')

    // Define animated gradient for flowing data effect
    const gradient = defs
      .append('linearGradient')
      .attr('id', 'flow-gradient')
      .attr('gradientUnits', 'userSpaceOnUse')

    gradient
      .append('stop')
      .attr('offset', '0%')
      .attr('stop-color', '#15B241')
      .attr('stop-opacity', 0)

    gradient
      .append('stop')
      .attr('offset', '50%')
      .attr('stop-color', '#15B241')
      .attr('stop-opacity', 1)

    gradient
      .append('stop')
      .attr('offset', '100%')
      .attr('stop-color', '#15B241')
      .attr('stop-opacity', 0)

    // Animate the gradient
    gradient
      .append('animate')
      .attr('attributeName', 'x1')
      .attr('values', '0%;100%')
      .attr('dur', '2s')
      .attr('repeatCount', 'indefinite')

    gradient
      .append('animate')
      .attr('attributeName', 'x2')
      .attr('values', '100%;200%')
      .attr('dur', '2s')
      .attr('repeatCount', 'indefinite')

    // Create container for zoom
    g = svg.append('g')

    // Add zoom behavior
    const zoom = d3
      .zoom()
      .scaleExtent([0.1, 4])
      .on('zoom', (event) => {
        g!.attr('transform', event.transform)
      })

    svg.call(zoom as any)

    // Create groups for links and nodes
    linkGroup = g.append('g').attr('class', 'links')
    nodeGroup = g.append('g').attr('class', 'nodes')

    // Create tooltip
    tooltip = d3
      .select('body')
      .append('div')
      .attr('class', 'tooltip')
      .style('opacity', 0)
      .style('position', 'absolute')
  }

  const nodesArray = Array.from(nodeMap.values())

  // Update or create simulation
  if (!currentSimulation) {
    currentSimulation = d3
      .forceSimulation(nodesArray)
      .force(
        'link',
        d3
          .forceLink(links)
          .id((d: any) => d.peer_id)
          .distance(350) // Increased distance for better arrow visibility
          .strength((d: any) => {
            // Weaker link force for our own node to keep it centered
            if (
              currentNodePeerID &&
              (d.source.peer_id === currentNodePeerID || d.target.peer_id === currentNodePeerID)
            ) {
              return 0.1
            }
            return 0.8 // Slightly weaker to allow more flexible positioning
          }),
      )
      .force(
        'charge',
        d3.forceManyBody().strength((d: any) => {
          // No charge for our own node (keeps it centered)
          if (currentNodePeerID && d.peer_id === currentNodePeerID) {
            return 0
          }
          // Stronger repulsion to keep nodes apart
          const isDisconnected =
            !d.sync_peer_id && !nodesArray.some((n) => n.sync_peer_id === d.peer_id)
          return isDisconnected ? -400 : -800
        }),
      )
      .force('center', d3.forceCenter(width / 2, height / 2).strength(0.05))
      .force('collision', d3.forceCollide(nodeWidth / 2 + 80)) // Much larger collision radius
      .force('boundary', () => {
        // Keep nodes within bounds
        nodesArray.forEach((d) => {
          const margin = nodeWidth / 2 + 20
          if (d.x !== undefined) {
            d.x = Math.max(margin, Math.min(width - margin, d.x))
          }
          if (d.y !== undefined) {
            d.y = Math.max(margin, Math.min(height - margin, d.y))
          }
        })
      })
      .alphaDecay(0.02) // Slower decay to settle more gently
  } else {
    // Update simulation with new data
    currentSimulation.nodes(nodesArray)
    ;(currentSimulation.force('link') as d3.ForceLink<any, any>)
      .links(links)
      .id((d: any) => d.peer_id)
      .distance(350) // Keep consistent distance
      .strength((d: any) => {
        // Weaker link force for our own node to keep it centered
        if (
          currentNodePeerID &&
          ((typeof d.source === 'object' && d.source.peer_id === currentNodePeerID) ||
            (typeof d.target === 'object' && d.target.peer_id === currentNodePeerID) ||
            d.source === currentNodePeerID ||
            d.target === currentNodePeerID)
        ) {
          return 0.1
        }
        return 0.8
      })

    // Update charge force strengths for disconnected nodes
    ;(currentSimulation.force('charge') as d3.ForceManyBody<any>).strength((d: any) => {
      // No charge for our own node (keeps it centered)
      if (currentNodePeerID && d.peer_id === currentNodePeerID) {
        return 0
      }
      const isDisconnected =
        !d.sync_peer_id && !nodesArray.some((n) => n.sync_peer_id === d.peer_id)
      return isDisconnected ? -400 : -800 // Stronger repulsion
    })

    // Update collision force
    ;(currentSimulation.force('collision') as d3.ForceCollide<any>).radius(nodeWidth / 2 + 80) // Keep consistent collision radius

    // Only restart if there are new nodes or removed nodes
    const oldNodeIds = new Set(currentSimulation.nodes().map((n: any) => n.peer_id))
    const newNodeIds = new Set(nodesArray.map((n) => n.peer_id))
    const hasChanges =
      oldNodeIds.size !== newNodeIds.size || [...newNodeIds].some((id) => !oldNodeIds.has(id))

    if (hasChanges) {
      // Restart with very low alpha only when topology changes
      currentSimulation.alpha(0.1).restart()
    }
  }

  // DATA JOIN for links
  const link = linkGroup!.selectAll('g.sync-link-group').data(links, (d: any) => {
    // Use a consistent key for the link (now reversed: sync_peer -> receiving node)
    const sourceId = typeof d.source === 'string' ? d.source : d.source.peer_id
    const targetId = typeof d.target === 'string' ? d.target : d.target.peer_id
    return `${sourceId}->${targetId}`
  })

  // EXIT
  link.exit().remove()

  // ENTER - Create a group for each link to hold multiple animated elements
  const linkEnter = link.enter().append('g').attr('class', 'sync-link-group')

  // Main dotted line
  const mainLine = linkEnter
    .append('line')
    .attr('class', 'sync-link')
    .attr('stroke-width', 3)
    .attr('stroke', '#15B241')
    .attr('stroke-dasharray', '8, 4') // Dotted line pattern
    .attr('marker-end', 'url(#arrowhead)')
    .attr('opacity', 0.7)

  // Add a second line for the glow effect
  linkEnter
    .append('line')
    .attr('class', 'sync-link-glow')
    .attr('stroke-width', 6)
    .attr('stroke', '#15B241')
    .attr('stroke-dasharray', '8, 4')
    .attr('opacity', 0.3)
    .attr('filter', 'blur(2px)')

  // Add animation for the dotted line movement
  mainLine
    .append('animate')
    .attr('attributeName', 'stroke-dashoffset')
    .attr('from', 0)
    .attr('to', -12) // Negative value makes dots flow from source to target
    .attr('dur', '0.8s')
    .attr('repeatCount', 'indefinite')

  // Add pulsing opacity animation
  mainLine
    .append('animate')
    .attr('attributeName', 'opacity')
    .attr('values', '0.5;0.9;0.5')
    .attr('dur', '2s')
    .attr('repeatCount', 'indefinite')

  // MERGE
  const linkMerge = linkEnter.merge(link as any)

  // DATA JOIN for nodes
  const node = nodeGroup!.selectAll('g.peer-node-group').data(nodesArray, (d: any) => d.peer_id)

  // EXIT
  node.exit().remove()

  // ENTER
  const nodeEnter = node
    .enter()
    .append('g')
    .attr('class', 'peer-node-group')
    .call(d3.drag().on('start', dragstarted).on('drag', dragged).on('end', dragended) as any)
    .on('dblclick', function (_event: any, d: any) {
      // Don't allow releasing our own node's fixed position
      if (currentNodePeerID && d.peer_id === currentNodePeerID) {
        return
      }
      // Double-click to release fixed position
      d.fx = null
      d.fy = null
      if (currentSimulation) {
        currentSimulation.alpha(0.3).restart()
      }
    })

  // Add all the node elements
  nodeEnter
    .append('rect')
    .attr('class', (d: any) => {
      // Check if this is our current node
      if (currentNodePeerID && d.peer_id === currentNodePeerID) {
        return 'peer-node current-node'
      }
      // Check if this node is connected (has sync relationship or is being synced from)
      const hasOutgoingSync = d.sync_peer_id && nodeMap.has(d.sync_peer_id)
      const hasIncomingSync = Array.from(nodeMap.values()).some((n) => n.sync_peer_id === d.peer_id)
      return hasOutgoingSync || hasIncomingSync ? 'peer-node connected' : 'peer-node disconnected'
    })
    .attr('width', nodeWidth)
    .attr('height', nodeHeight)
    .attr('x', -nodeWidth / 2)
    .attr('y', -nodeHeight / 2)
    .attr('rx', 8)
    .attr('ry', 8)

  nodeEnter
    .append('text')
    .attr('class', 'uptime-text')
    .attr('x', -nodeWidth / 2 + 10)
    .attr('y', -nodeHeight / 2 + 20)

  nodeEnter
    .append('text')
    .attr('class', 'connection-time')
    .attr('x', nodeWidth / 2 - 10)
    .attr('y', -nodeHeight / 2 + 20)
    .attr('text-anchor', 'end')

  nodeEnter
    .append('text')
    .attr('class', 'peer-text peer-id')
    .attr('x', 0)
    .attr('y', -40)
    .attr('text-anchor', 'middle')

  nodeEnter
    .append('text')
    .attr('class', 'peer-text label base-url')
    .attr('x', 0)
    .attr('y', -20)
    .attr('text-anchor', 'middle')

  nodeEnter
    .append('text')
    .attr('class', 'peer-text block-height')
    .attr('x', 0)
    .attr('y', 0)
    .attr('text-anchor', 'middle')

  nodeEnter
    .append('text')
    .attr('class', 'peer-text label block-hash')
    .attr('x', 0)
    .attr('y', 20)
    .attr('text-anchor', 'middle')

  nodeEnter
    .append('text')
    .attr('class', 'peer-text label fsm-state')
    .attr('x', 0)
    .attr('y', 40)
    .attr('text-anchor', 'middle')

  nodeEnter
    .append('text')
    .attr('class', 'peer-text label miner-name')
    .attr('x', 0)
    .attr('y', 60)
    .attr('text-anchor', 'middle')

  // MERGE and UPDATE
  const nodeMerge = nodeEnter.merge(node as any)

  // Update node class based on connection status
  nodeMerge.select('rect').attr('class', function (this: any, d: any) {
    // Check if this is our current node
    if (currentNodePeerID && d.peer_id === currentNodePeerID) {
      return 'peer-node current-node'
    }
    const hasOutgoingSync = d.sync_peer_id && nodeMap.has(d.sync_peer_id)
    const hasIncomingSync = Array.from(nodeMap.values()).some((n) => n.sync_peer_id === d.peer_id)

    // Check if this node just finished syncing (was syncing before, not anymore)
    const rect = d3.select(this)
    const wasSyncing = rect.classed('connected')
    const isNowSynced = !hasOutgoingSync && !hasIncomingSync && wasSyncing

    if (isNowSynced) {
      // Flash green briefly when sync completes
      rect
        .transition()
        .duration(300)
        .style('fill', '#15B241')
        .style('opacity', 0.8)
        .transition()
        .duration(300)
        .style('fill', null)
        .style('opacity', null)
    }

    return hasOutgoingSync || hasIncomingSync ? 'peer-node connected' : 'peer-node disconnected'
  })

  // Update text content for all nodes
  nodeMerge.select('.uptime-text').text((d: any) => {
    if (d.start_time) {
      // Convert Unix timestamp to milliseconds for humanTime
      return humanTime(d.start_time * 1000)
    }
    return ''
  })

  nodeMerge.select('.connection-time').text((d: any) => {
    if (d.sync_peer_id && d.sync_connected_at) {
      // Use server-provided sync connection time (Unix timestamp in seconds)
      return '→ ' + humanTime(d.sync_connected_at * 1000)
    }
    return ''
  })

  nodeMerge.select('.peer-id').text((d: any) => (d?.peer_id ? truncatePeerId(d.peer_id) : ''))

  nodeMerge.select('.base-url').text((d: any) => d?.base_url || 'No URL')

  nodeMerge.select('.block-height').text((d: any) => {
    if (!d) return 'Height: 0'
    const height = d.best_height || d.height || 0
    return `Height: ${height.toLocaleString()}`
  })

  nodeMerge.select('.block-hash').text((d: any) => {
    if (!d) return 'No hash'
    const hash = d.best_block_hash || d.hash || ''
    return hash ? `${hash.slice(0, 8)}...${hash.slice(-8)}` : 'No hash'
  })

  nodeMerge.select('.fsm-state').text((d: any) => d?.fsm_state || '')

  nodeMerge
    .select('.miner-name')
    .text((d: any) => {
      if (!d) return ''
      const minerName = d.miner_name || d.miner || ''
      if (minerName && minerName.length > 20) {
        return minerName.slice(0, 17) + '...'
      }
      return minerName
    })
    .style('font-weight', 'bold')
    .style('fill', '#4a9eff')

  // Add hover effects
  nodeMerge
    .on('mouseover', function (event: any, d: any) {
      if (!d) return // Guard against undefined data

      d3.select(this).select('rect').classed('selected', true)

      // Show tooltip with full information
      tooltip!.transition().duration(200).style('opacity', 0.9)

      let html = `
        <div class="label">Peer ID:</div>
        <div class="value">${d.peer_id || 'Unknown'}</div>
        <div class="label">Base URL:</div>
        <div class="value">${d.base_url || 'N/A'}</div>
        <div class="label">Block Height:</div>
        <div class="value">${(d.best_height || d.height || 0).toLocaleString()}</div>
        <div class="label">Block Hash:</div>
        <div class="value">${d.best_block_hash || d.hash || 'N/A'}</div>
      `

      if (d.miner_name || d.miner) {
        html += `
          <div class="label">Miner:</div>
          <div class="value">${d.miner_name || d.miner}</div>
        `
      }

      if (d.sync_peer_id) {
        html += `
          <div class="label">Sync Peer:</div>
          <div class="value">${d.sync_peer_id}</div>
          <div class="label">Sync Height:</div>
          <div class="value">${(d.sync_peer_height || 0).toLocaleString()}</div>
        `
      }

      if (d.fsm_state) {
        html += `
          <div class="label">FSM State:</div>
          <div class="value">${d.fsm_state}</div>
        `
      }

      if (d.miner_name) {
        html += `
          <div class="label">Miner:</div>
          <div class="value">${d.miner_name}</div>
        `
      }

      if (d.version) {
        html += `
          <div class="label">Version:</div>
          <div class="value">${d.version}</div>
        `
      }

      tooltip!
        .html(html)
        .style('left', event.pageX + 10 + 'px')
        .style('top', event.pageY - 28 + 'px')
    })
    .on('mouseout', function (_event: any, _d: any) {
      d3.select(this).select('rect').classed('selected', false)
      tooltip!.transition().duration(500).style('opacity', 0)
    })

  // Update positions on simulation tick
  currentSimulation.on('tick', () => {
    // Apply boundary constraints
    nodesArray.forEach((d) => {
      // Always keep our node centered (check both currentNodePeerID and is_self flag)
      if (currentNodePeerID && d.peer_id === currentNodePeerID) {
        d.x = width / 2
        d.y = height / 2
        d.fx = width / 2
        d.fy = height / 2
      } else {
        const margin = nodeWidth / 2 + 20
        if (d.x !== undefined && !d.fx) {
          d.x = Math.max(margin, Math.min(width - margin, d.x))
        }
        if (d.y !== undefined && !d.fy) {
          d.y = Math.max(margin, Math.min(height - margin, d.y))
        }
      }
    })

    // Update all lines within each link group
    linkMerge
      .selectAll('line')
      .attr('x1', function (this: any) {
        const d = d3.select(this.parentNode).datum() as any
        const sourceId = typeof d.source === 'string' ? d.source : d.source.peer_id || d.source.id
        const source = nodesArray.find((n) => n.peer_id === sourceId)
        return source?.x || 0
      })
      .attr('y1', function (this: any) {
        const d = d3.select(this.parentNode).datum() as any
        const sourceId = typeof d.source === 'string' ? d.source : d.source.peer_id || d.source.id
        const source = nodesArray.find((n) => n.peer_id === sourceId)
        return source?.y || 0
      })
      .attr('x2', function (this: any) {
        const d = d3.select(this.parentNode).datum() as any
        const targetId = typeof d.target === 'string' ? d.target : d.target.peer_id || d.target.id
        const sourceId = typeof d.source === 'string' ? d.source : d.source.peer_id || d.source.id
        const target = nodesArray.find((n) => n.peer_id === targetId)
        const source = nodesArray.find((n) => n.peer_id === sourceId)
        const dx = (target?.x || 0) - (source?.x || 0)
        const dy = (target?.y || 0) - (source?.y || 0)
        const angle = Math.atan2(dy, dx)
        // Shorten line to not overlap with target node
        return (target?.x || 0) - Math.cos(angle) * (nodeWidth / 2 + 15)
      })
      .attr('y2', function (this: any) {
        const d = d3.select(this.parentNode).datum() as any
        const targetId = typeof d.target === 'string' ? d.target : d.target.peer_id || d.target.id
        const sourceId = typeof d.source === 'string' ? d.source : d.source.peer_id || d.source.id
        const target = nodesArray.find((n) => n.peer_id === targetId)
        const source = nodesArray.find((n) => n.peer_id === sourceId)
        const dx = (target?.x || 0) - (source?.x || 0)
        const dy = (target?.y || 0) - (source?.y || 0)
        const angle = Math.atan2(dy, dx)
        // Shorten line to not overlap with target node
        return (target?.y || 0) - Math.sin(angle) * (nodeHeight / 2 + 15)
      })

    nodeMerge.attr('transform', (d: any) => `translate(${d.x},${d.y})`)
  })

  function dragstarted(event: any, d: any) {
    // Don't allow dragging our own node
    if (currentNodePeerID && d.peer_id === currentNodePeerID) {
      return
    }
    if (!event.active) currentSimulation!.alphaTarget(0.3).restart()
    d.fx = d.x
    d.fy = d.y
  }

  function dragged(event: any, d: any) {
    // Don't allow dragging our own node
    if (currentNodePeerID && d.peer_id === currentNodePeerID) {
      return
    }
    d.fx = event.x
    d.fy = event.y
  }

  function dragended(event: any, d: any) {
    // Don't allow dragging our own node
    if (currentNodePeerID && d.peer_id === currentNodePeerID) {
      return
    }
    if (!event.active) currentSimulation!.alphaTarget(0)
    // Keep the node fixed in place after dragging
    // User can double-click to release if needed
    // d.fx = null
    // d.fy = null
  }
}

function truncatePeerId(peerId: string): string {
  if (!peerId) return 'Unknown'
  if (peerId.length <= 16) return peerId
  return `${peerId.slice(0, 8)}...${peerId.slice(-8)}`
}

// Cleanup function to be called when component is destroyed
export function cleanupPeerNetwork() {
  if (currentSimulation) {
    currentSimulation.stop()
  }
  if (svg) {
    svg.remove()
    svg = null
  }
  if (tooltip) {
    tooltip.remove()
    tooltip = null
  }
  g = null
  nodeGroup = null
  linkGroup = null
  currentSimulation = null
  connectionMap.clear()
}
