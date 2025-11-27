<script lang="ts">
  import { onMount } from 'svelte'
  import * as d3 from 'd3'
  import type { MerkleProofData } from '$internal/api'

  export let merkleProof: MerkleProofData | null = null
  export let loading: boolean = false
  export let error: string | null = null

  let container: HTMLDivElement
  let svg: d3.Selection<SVGSVGElement, unknown, null, undefined>
  let svgGroup: d3.Selection<SVGGElement, unknown, null, undefined>
  let zoomBehavior: d3.ZoomBehavior<SVGSVGElement, unknown>
  let mounted = false
  
  // Navigation state
  let currentTransform = d3.zoomIdentity
  let showNavigationHelp = false

  // Tree visualization constants
  const NODE_WIDTH = 160
  const NODE_HEIGHT = 40
  const LEVEL_HEIGHT = 80
  const margin = { top: 20, right: 20, bottom: 20, left: 20 }

  // Colors for different node types
  const colors = {
    transaction: '#28a745',    // Green for the target transaction
    subtreeProof: '#007bff',   // Blue for subtree proof path nodes
    subtreeRoot: '#17a2b8',    // Cyan for the subtree root
    blockProof: '#6f42c1',     // Purple for block-level proof path nodes
    sibling: '#ffc107',        // Yellow for sibling nodes (known hashes)
    placeholder: '#e9ecef',    // Light gray for placeholder nodes
    root: '#dc3545'            // Red for the final merkle root
  }

  interface TreeNode {
    id: string
    hash: string
    level: number
    index: number
    type: 'transaction' | 'subtreeProof' | 'subtreeRoot' | 'blockProof' | 'sibling' | 'placeholder' | 'root'
    isProofPath: boolean
    x: number
    y: number
  }

  interface TreeLink {
    source: TreeNode
    target: TreeNode
  }

  let nodes: TreeNode[] = []
  let links: TreeLink[] = []

  // Helper function to convert hex string to byte array
  function hexToBytes(hex: string): Uint8Array {
    // Validate input
    if (typeof hex !== 'string' || hex.length === 0) {
      return new Uint8Array() // Return empty array for empty input
    }

    // Remove any 0x prefix if present
    const cleanHex = hex.startsWith('0x') ? hex.slice(2) : hex

    if (cleanHex.length % 2 !== 0) {
      throw new Error(`Invalid hex input: must have even length (got ${cleanHex.length})`)
    }

    if (!/^[0-9a-fA-F]*$/.test(cleanHex)) {
      throw new Error('Invalid hex input: must contain only hexadecimal characters')
    }

    const matches = cleanHex.match(/.{1,2}/g)
    if (!matches) return new Uint8Array()

    return new Uint8Array(matches.map(byte => parseInt(byte, 16)))
  }

  // Helper function to convert byte array to hex string
  function bytesToHex(bytes: Uint8Array): string {
    return Array.from(bytes)
      .map(b => b.toString(16).padStart(2, '0'))
      .join('')
  }

  // SHA256 double hash function for computing intermediate hashes
  async function sha256Double(bytes: Uint8Array): Promise<string> {
    // Validate input
    if (!(bytes instanceof Uint8Array)) {
      throw new Error('sha256Double requires a Uint8Array input')
    }

    if (bytes.length === 0) {
      throw new Error('sha256Double requires non-empty input')
    }

    // First SHA256
    let hash = await crypto.subtle.digest('SHA-256', bytes)

    // Second SHA256
    hash = await crypto.subtle.digest('SHA-256', hash)

    // Convert to hex string
    return bytesToHex(new Uint8Array(hash))
  }

  onMount(() => {
    mounted = true
    
    // Add keyboard shortcuts
    const handleKeyDown = (event: KeyboardEvent) => {
      if (!merkleProof) return
      
      switch (event.key) {
        case 'r':
        case 'R':
          if (event.shiftKey) {
            resetZoom()
            event.preventDefault()
          }
          break
        case 'f':
        case 'F':
          if (event.shiftKey) {
            zoomToFit()
            event.preventDefault()
          }
          break
        case 't':
        case 'T':
          if (event.shiftKey) {
            focusOnTransaction()
            event.preventDefault()
          }
          break
      }
    }
    
    window.addEventListener('keydown', handleKeyDown)
    
    return () => {
      window.removeEventListener('keydown', handleKeyDown)
    }
  })

  $: if (mounted && merkleProof && container) {
    // Wait for container to be properly sized
    setTimeout(async () => {
      await buildTree()
      renderVisualization()
    }, 50)
  }

  // Helper function to extract visualization data from BUMP format
  function extractVisualizationData(bumpData: MerkleProofData) {
    // For visualization purposes, we need to reconstruct the tree structure
    // from the BUMP path data. This is a simplified approach for display.
    
    const proofHashes: string[] = []
    const allOffsets: number[] = []
    
    // Extract all hashes and offsets from the BUMP path
    bumpData.path.forEach((level, levelIndex) => {
      level.forEach(node => {
        if (node.hash) {
          proofHashes.push(node.hash)
        }
        allOffsets.push(node.offset)
      })
    })
    
    return {
      proofHashes,
      levels: bumpData.path.length,
      blockHeight: bumpData.blockHeight
    }
  }

  async function buildTree() {
    if (!merkleProof) return

    nodes = []
    links = []

    // Extract data from BUMP format
    const vizData = extractVisualizationData(merkleProof)
    
    // For now, create a simplified visualization showing the BUMP path
    await buildBUMPTree(merkleProof)

    // Calculate positions
    calculatePositions()
  }

  async function buildBUMPTree(bumpData: MerkleProofData) {
    // Create a simplified tree visualization based on BUMP path data
    // Each level in the BUMP path represents a level in the merkle tree
    
    const totalLevels = bumpData.path.length
    if (totalLevels === 0) return
    
    // Create nodes for each level of the BUMP path
    bumpData.path.forEach((level, levelIndex) => {
      level.forEach((node, nodeIndex) => {
        // Determine node type based on BUMP flags
        let nodeType: 'transaction' | 'subtreeProof' | 'subtreeRoot' | 'blockProof' | 'sibling' | 'placeholder' | 'root'
        let isProofPath = false
        
        if (levelIndex === 0 && node.txid) {
          // This is the target transaction
          nodeType = 'transaction'
          isProofPath = true
        } else if (node.hash) {
          // This is a hash node (sibling or proof path)
          if (levelIndex === totalLevels - 1) {
            nodeType = 'root' // Final merkle root
          } else if (levelIndex < Math.floor(totalLevels / 2)) {
            nodeType = 'subtreeProof' // Subtree-level proof
          } else {
            nodeType = 'blockProof' // Block-level proof
          }
          isProofPath = false
        } else if (node.duplicate) {
          nodeType = 'sibling'
          isProofPath = false
        } else {
          nodeType = 'placeholder'
          isProofPath = false
        }
        
        // Create display hash
        let displayHash = node.hash || (node.txid ? 'TX_ID' : (node.duplicate ? 'DUPLICATE' : `NODE_${levelIndex}_${nodeIndex}`))
        
        const treeNode: TreeNode = {
          id: `bump-${levelIndex}-${nodeIndex}`,
          hash: displayHash,
          level: levelIndex,
          index: node.offset,
          type: nodeType,
          isProofPath: isProofPath,
          x: 0,
          y: 0
        }
        
        nodes.push(treeNode)
      })
    })
    
    // Create simplified links showing the proof path
    // Connect nodes at adjacent levels that form the proof path
    for (let levelIndex = 0; levelIndex < totalLevels - 1; levelIndex++) {
      const currentLevelNodes = nodes.filter(n => n.level === levelIndex)
      const nextLevelNodes = nodes.filter(n => n.level === levelIndex + 1)
      
      // For now, create a simple linear connection between levels
      // In a full implementation, you'd calculate the actual tree relationships
      if (currentLevelNodes.length > 0 && nextLevelNodes.length > 0) {
        const sourceNode = currentLevelNodes[0]
        const targetNode = nextLevelNodes[0]
        
        links.push({
          source: sourceNode,
          target: targetNode
        })
      }
    }
  }

  async function buildCompleteSubtree(levels, txIndex, txHash, proofHashes, rootHash) {
    // Build a complete binary tree with 2^(levels-1) leaves
    const maxLeafIndex = Math.pow(2, levels - 1) - 1
    
    // Build all leaf nodes (level 0)
    for (let leafIdx = 0; leafIdx <= maxLeafIndex; leafIdx++) {
      let nodeData
      if (leafIdx === txIndex) {
        nodeData = {
          id: `tx-${txHash}`,
          hash: txHash,
          type: 'transaction',
          isProofPath: true
        }
      } else {
        nodeData = {
          id: `leaf-${leafIdx}`,
          hash: `TX_${leafIdx}`,
          type: 'placeholder',
          isProofPath: false
        }
      }
      
      nodes.push({
        ...nodeData,
        level: 0,
        index: leafIdx,
        x: 0,
        y: 0
      })
    }
    
    // Build intermediate levels bottom-up, computing real hashes for proof path
    let currentTxIndex = txIndex
    let currentHash = txHash
    
    for (let level = 1; level < levels; level++) {
      const nodesAtThisLevel = Math.pow(2, levels - 1 - level)
      const proofLevel = level - 1 // Proof array is 0-indexed
      
      for (let nodeIdx = 0; nodeIdx < nodesAtThisLevel; nodeIdx++) {
        const leftChildIdx = nodeIdx * 2
        const rightChildIdx = nodeIdx * 2 + 1
        
        let nodeData
        const isOnProofPath = isNodeOnProofPath(currentTxIndex, level, nodeIdx)
        
        if (level === levels - 1) {
          // This is the subtree root
          nodeData = {
            id: `subtree-root`,
            hash: rootHash,
            type: 'subtreeRoot',
            isProofPath: true
          }
        } else if (isOnProofPath) {
          // This node is on the proof path - compute its hash if we have enough info
          let computedHash = `H_${level}_${nodeIdx}`
          
          if (proofLevel < proofHashes.length) {
            // We can compute the real hash using the current proof path hash and sibling
            const siblingHash = proofHashes[proofLevel]
            const parentIndex = Math.floor(currentTxIndex / Math.pow(2, level))
            
            // Determine if current hash is left or right child at the PREVIOUS level
            const childIndex = currentTxIndex
            try {
              if (childIndex % 2 === 0) {
                // Current is left child, sibling is right
                const leftBytes = hexToBytes(currentHash)
                const rightBytes = hexToBytes(siblingHash)
                const combined = new Uint8Array([...leftBytes, ...rightBytes])
                computedHash = await sha256Double(combined)
              } else {
                // Current is right child, sibling is left
                const leftBytes = hexToBytes(siblingHash)
                const rightBytes = hexToBytes(currentHash)
                const combined = new Uint8Array([...leftBytes, ...rightBytes])
                computedHash = await sha256Double(combined)
              }
            } catch (err) {
              console.error(`Error computing hash at level ${level}, node ${nodeIdx}:`, err)
              computedHash = `ERROR_${level}_${nodeIdx}`
            }
            
            // Update currentHash for next level
            if (nodeIdx === parentIndex) {
              currentHash = computedHash
            }
          }
          
          nodeData = {
            id: `proof-${level}-${nodeIdx}`,
            hash: computedHash,
            type: 'subtreeProof',
            isProofPath: true
          }
        } else {
          // Check if this is a sibling node with known hash
          const siblingIdx = getSiblingAtLevel(currentTxIndex, level, nodeIdx)
          
          if (siblingIdx === nodeIdx && proofLevel < proofHashes.length) {
            nodeData = {
              id: `sibling-${level}-${nodeIdx}`,
              hash: proofHashes[proofLevel],
              type: 'sibling',
              isProofPath: false
            }
          } else {
            nodeData = {
              id: `placeholder-${level}-${nodeIdx}`,
              hash: `H_${level}_${nodeIdx}`,
              type: 'placeholder',
              isProofPath: false
            }
          }
        }
        
        nodes.push({
          ...nodeData,
          level: level,
          index: nodeIdx,
          x: 0,
          y: 0
        })
        
        // Create links to children if they exist
        const leftChild = nodes.find(n => n.level === level - 1 && n.index === leftChildIdx)
        const rightChild = nodes.find(n => n.level === level - 1 && n.index === rightChildIdx)
        const parent = nodes[nodes.length - 1] // Current node just added
        
        if (leftChild) {
          links.push({ source: leftChild, target: parent })
        }
        if (rightChild) {
          links.push({ source: rightChild, target: parent })
        }
      }
      
      // Move up the tree for next level
      currentTxIndex = Math.floor(currentTxIndex / 2)
    }
  }

  async function buildBlockProof(subtreeLevels, subtreeIndex, subtreeRoot, blockProof, merkleRoot) {
    // Build complete block-level binary tree structure
    const blockLevels = blockProof.length + 1 // +1 for the subtree root level
    const totalSubtrees = Math.pow(2, blockProof.length)
    
    // First, add all subtree root placeholders at the block base level
    const blockBaseLevel = subtreeLevels
    for (let i = 0; i < totalSubtrees; i++) {
      let nodeData
      if (i === subtreeIndex) {
        // This is our actual subtree root - link to existing subtree root
        continue // Skip adding duplicate, we'll link to existing subtree-root node
      } else {
        // Placeholder for other subtrees
        nodeData = {
          id: `block-subtree-${i}`,
          hash: `SUBTREE_${i}`,
          level: blockBaseLevel,
          index: i,
          type: 'placeholder',
          isProofPath: false,
          x: 0,
          y: 0
        }
        nodes.push(nodeData)
      }
    }
    
    // Build intermediate block levels with proper tree structure
    let currentHash = subtreeRoot
    let currentSubtreeIndex = subtreeIndex
    
    for (let level = 1; level <= blockProof.length; level++) {
      const currentLevel = subtreeLevels + level
      const nodesAtThisLevel = Math.pow(2, blockProof.length - level)
      const proofLevel = level - 1
      
      // Add all nodes at this level
      for (let nodeIdx = 0; nodeIdx < nodesAtThisLevel; nodeIdx++) {
        const leftChildIdx = nodeIdx * 2
        const rightChildIdx = nodeIdx * 2 + 1
        
        const isOnProofPath = isNodeOnBlockProofPath(currentSubtreeIndex, level, nodeIdx, blockProof.length)
        const isLastLevel = level === blockProof.length
        
        let nodeData
        if (isLastLevel) {
          // This is the final merkle root
          nodeData = {
            id: 'block-merkle-root',
            hash: merkleRoot,
            level: currentLevel,
            index: nodeIdx,
            type: 'root',
            isProofPath: true,
            x: 0,
            y: 0
          }
        } else if (isOnProofPath) {
          // This node is on the proof path - compute its hash
          const siblingHash = blockProof[proofLevel]
          
          // Determine if current subtree is left or right child
          const childIndex = Math.floor(currentSubtreeIndex / Math.pow(2, level - 1))
          let computedHash
          try {
            if (childIndex % 2 === 0) {
              // Current is left child, sibling is right
              const leftBytes = hexToBytes(currentHash)
              const rightBytes = hexToBytes(siblingHash)
              const combined = new Uint8Array([...leftBytes, ...rightBytes])
              computedHash = await sha256Double(combined)
            } else {
              // Current is right child, sibling is left
              const leftBytes = hexToBytes(siblingHash)
              const rightBytes = hexToBytes(currentHash)
              const combined = new Uint8Array([...leftBytes, ...rightBytes])
              computedHash = await sha256Double(combined)
            }
          } catch (err) {
            console.error(`Error computing block hash at level ${level}, node ${nodeIdx}:`, err)
            computedHash = `BLOCK_ERROR_${level}_${nodeIdx}`
          }
          
          // Update currentHash for next level
          if (nodeIdx === Math.floor(currentSubtreeIndex / Math.pow(2, level))) {
            currentHash = computedHash
          }
          
          nodeData = {
            id: `block-proof-${level}-${nodeIdx}`,
            hash: computedHash,
            level: currentLevel,
            index: nodeIdx,
            type: 'blockProof',
            isProofPath: true,
            x: 0,
            y: 0
          }
        } else {
          // Check if this is a sibling node with known hash
          const siblingIdx = getSiblingAtBlockLevel(currentSubtreeIndex, level, nodeIdx, blockProof.length)
          
          if (siblingIdx === nodeIdx && proofLevel < blockProof.length) {
            nodeData = {
              id: `block-sibling-${level}-${nodeIdx}`,
              hash: blockProof[proofLevel],
              level: currentLevel,
              index: nodeIdx,
              type: 'sibling',
              isProofPath: false,
              x: 0,
              y: 0
            }
          } else {
            nodeData = {
              id: `block-placeholder-${level}-${nodeIdx}`,
              hash: `BH_${level}_${nodeIdx}`,
              level: currentLevel,
              index: nodeIdx,
              type: 'placeholder',
              isProofPath: false,
              x: 0,
              y: 0
            }
          }
        }
        
        nodes.push(nodeData)
        
        // Create links to children
        const leftChild = nodes.find(n => n.level === currentLevel - 1 && n.index === leftChildIdx)
        const rightChild = nodes.find(n => n.level === currentLevel - 1 && n.index === rightChildIdx)
        const parent = nodes[nodes.length - 1]
        
        if (leftChild) {
          links.push({ source: leftChild, target: parent })
        }
        if (rightChild) {
          links.push({ source: rightChild, target: parent })
        }
      }
      
      // Update for next level
      currentSubtreeIndex = Math.floor(currentSubtreeIndex / 2)
    }
  }
  
  function isNodeOnBlockProofPath(subtreeIndex, level, nodeIndex, totalBlockLevels) {
    // Calculate which node at this level contains our subtree
    const subtreeNodeAtLevel = Math.floor(subtreeIndex / Math.pow(2, level))
    return subtreeNodeAtLevel === nodeIndex
  }
  
  function getSiblingAtBlockLevel(subtreeIndex, level, nodeIndex, totalBlockLevels) {
    // Calculate the sibling index at this level
    const subtreeNodeAtLevel = Math.floor(subtreeIndex / Math.pow(2, level))
    return subtreeNodeAtLevel % 2 === 0 ? subtreeNodeAtLevel + 1 : subtreeNodeAtLevel - 1
  }

  function isNodeOnProofPath(txIndex, level, nodeIndex) {
    // Calculate which node at this level contains our transaction
    const txNodeAtLevel = Math.floor(txIndex / Math.pow(2, level))
    return txNodeAtLevel === nodeIndex
  }

  function getSiblingAtLevel(txIndex, level, nodeIndex) {
    // Calculate the sibling index at this level
    const txNodeAtLevel = Math.floor(txIndex / Math.pow(2, level))
    return txNodeAtLevel % 2 === 0 ? txNodeAtLevel + 1 : txNodeAtLevel - 1
  }
  

  function calculatePositions() {
    const maxLevel = Math.max(...nodes.map(n => n.level))
    const width = container?.clientWidth || 800
    
    // Calculate leaf node spacing
    const leafNodes = nodes.filter(n => n.level === 0)
    const leafCount = leafNodes.length
    const minSpacing = NODE_WIDTH + 20
    const availableWidth = width - margin.left - margin.right
    const leafSpacing = Math.max(minSpacing, availableWidth / Math.max(leafCount - 1, 1))
    
    // Position leaf nodes first (level 0)
    leafNodes.sort((a, b) => a.index - b.index)
    const leafStartX = margin.left + (availableWidth - (leafCount - 1) * leafSpacing) / 2
    
    leafNodes.forEach((node, i) => {
      node.x = leafStartX + i * leafSpacing
      node.y = margin.top + maxLevel * LEVEL_HEIGHT
    })
    
    // Position parent nodes level by level, working up from leaves
    for (let level = 1; level <= maxLevel; level++) {
      const nodesAtLevel = nodes.filter(n => n.level === level)
      const y = margin.top + (maxLevel - level) * LEVEL_HEIGHT
      
      nodesAtLevel.sort((a, b) => a.index - b.index)
      
      nodesAtLevel.forEach(node => {
        // Find the children of this node
        const leftChildIdx = node.index * 2
        const rightChildIdx = node.index * 2 + 1
        
        const leftChild = nodes.find(n => n.level === level - 1 && n.index === leftChildIdx)
        const rightChild = nodes.find(n => n.level === level - 1 && n.index === rightChildIdx)
        
        if (leftChild && rightChild) {
          // Center between both children
          const leftCenter = leftChild.x + NODE_WIDTH / 2
          const rightCenter = rightChild.x + NODE_WIDTH / 2
          node.x = (leftCenter + rightCenter) / 2 - NODE_WIDTH / 2
        } else if (leftChild) {
          // Only left child exists, center above it
          node.x = leftChild.x + NODE_WIDTH / 2 - NODE_WIDTH / 2
        } else if (rightChild) {
          // Only right child exists, center above it
          node.x = rightChild.x + NODE_WIDTH / 2 - NODE_WIDTH / 2
        } else {
          // No children found, fallback positioning
          const expectedX = margin.left + (node.index / Math.pow(2, maxLevel - level)) * leafSpacing
          node.x = expectedX
        }
        
        node.y = y
      })
    }
  }

  function renderVisualization() {
    if (!container || nodes.length === 0) return

    // Clear previous visualization
    d3.select(container).selectAll('*').remove()

    // Ensure container has proper dimensions
    const containerRect = container.getBoundingClientRect()
    const width = containerRect.width > 0 ? containerRect.width : 800
    const height = Math.max(400, margin.top + (Math.max(...nodes.map(n => n.level)) + 1) * LEVEL_HEIGHT + margin.bottom)

    // Create SVG with zoom capabilities
    svg = d3.select(container)
      .append('svg')
      .attr('width', '100%')
      .attr('height', height)
      .attr('viewBox', `0 0 ${width} ${height}`)
      .style('cursor', 'grab')
      .style('background-color', 'white')

    // Create main group for zooming and panning
    svgGroup = svg.append('g')
      .attr('class', 'main-group')

    // Set up zoom behavior
    zoomBehavior = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 3])
      .on('zoom', (event) => {
        currentTransform = event.transform
        svgGroup.attr('transform', currentTransform.toString())
        updateNavigationInfo()
      })

    // Apply zoom to SVG
    svg.call(zoomBehavior)

    // Update cursor during drag
    svg.on('mousedown', () => svg.style('cursor', 'grabbing'))
    svg.on('mouseup', () => svg.style('cursor', 'grab'))

    const g = svgGroup

    // Add links first (so they appear behind nodes)
    const link = g.selectAll('.link')
      .data(links)
      .enter()
      .append('line')
      .attr('class', 'link')
      .attr('x1', d => d.source.x + NODE_WIDTH / 2)
      .attr('y1', d => d.source.y + NODE_HEIGHT)
      .attr('x2', d => d.target.x + NODE_WIDTH / 2)
      .attr('y2', d => d.target.y)
      .attr('stroke', '#6c757d')
      .attr('stroke-width', 2)
      .attr('opacity', 0.6)

    // Add nodes
    const node = g.selectAll('.node')
      .data(nodes)
      .enter()
      .append('g')
      .attr('class', 'node')
      .attr('transform', d => `translate(${d.x}, ${d.y})`)

    // Add node rectangles
    node.append('rect')
      .attr('width', NODE_WIDTH)
      .attr('height', NODE_HEIGHT)
      .attr('rx', 8)
      .attr('ry', 8)
      .attr('fill', d => colors[d.type])
      .attr('stroke', '#ffffff')
      .attr('stroke-width', 2)
      .style('cursor', 'pointer')

    // Add node labels
    node.append('text')
      .attr('x', NODE_WIDTH / 2)
      .attr('y', NODE_HEIGHT / 2)
      .attr('dy', '0.35em')
      .attr('text-anchor', 'middle')
      .attr('font-family', 'monospace')
      .attr('font-size', '12px')
      .attr('font-weight', 'bold')
      .attr('fill', d => d.type === 'placeholder' ? '#6c757d' : 'white')
      .text(d => d.type === 'placeholder' ? d.hash : truncateHash(d.hash))

    // Add tooltips
    node.append('title')
      .text(d => {
        const typeLabel = {
          transaction: 'Transaction',
          subtreeProof: 'Subtree Proof Node',
          subtreeRoot: 'Subtree Root',
          blockProof: 'Block Proof Node',
          sibling: 'Sibling Node',
          placeholder: 'Placeholder',
          root: 'Block Merkle Root'
        }[d.type]
        return `${typeLabel}\nLevel: ${d.level}\nIndex: ${d.index}\nHash: ${d.hash}`
      })

    // Add hover effects
    node.on('mouseover', function(event, d) {
        d3.select(this).select('rect')
          .attr('stroke', '#ffc107')
          .attr('stroke-width', 3)
      })
      .on('mouseout', function(event, d) {
        d3.select(this).select('rect')
          .attr('stroke', '#ffffff')
          .attr('stroke-width', 2)
      })
    
    // Auto-fit the tree after initial render with proper timing
    requestAnimationFrame(() => {
      setTimeout(() => {
        zoomToFit()
      }, 200)
    })
  }

  function truncateHash(hash: string): string {
    return hash.length > 12 ? `${hash.slice(0, 8)}...${hash.slice(-4)}` : hash
  }

  // Navigation helper functions
  function updateNavigationInfo() {
    // This could be used to show current zoom level, position, etc.
  }

  function resetZoom() {
    if (svg && zoomBehavior) {
      svg.transition().duration(750).call(
        zoomBehavior.transform,
        d3.zoomIdentity
      )
    }
  }

  function zoomToFit() {
    if (!svg || !svgGroup || !zoomBehavior || nodes.length === 0) return

    const bounds = svgGroup.node()?.getBBox()
    if (!bounds) return

    const containerRect = container.getBoundingClientRect()
    const fullWidth = containerRect.width
    const fullHeight = containerRect.height

    const width = bounds.width
    const height = bounds.height
    const midX = bounds.x + width / 2
    const midY = bounds.y + height / 2

    if (width === 0 || height === 0) return

    const scale = Math.min(fullWidth / width, fullHeight / height) * 0.9
    const translate = [fullWidth / 2 - scale * midX, fullHeight / 2 - scale * midY]
    
    svg.transition().duration(750).call(
      zoomBehavior.transform,
      d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale)
    )
  }

  function focusOnTransaction() {
    if (!svg || !zoomBehavior || !merkleProof) return

    const txNode = nodes.find(n => n.type === 'transaction')
    if (!txNode) return

    const containerRect = container.getBoundingClientRect()
    const scale = 1.5
    const translate = [
      containerRect.width / 2 - scale * (txNode.x + NODE_WIDTH / 2),
      containerRect.height / 2 - scale * (txNode.y + NODE_HEIGHT / 2)
    ]

    svg.transition().duration(750).call(
      zoomBehavior.transform,
      d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale)
    )
  }

  // Reactive statement to handle window resize
  async function handleResize() {
    if (mounted && merkleProof) {
      await buildTree()
      renderVisualization()
    }
  }
</script>

<svelte:window on:resize={handleResize} />

<div class="merkle-visualizer">
  {#if loading}
    <div class="loading">
      <div class="spinner"></div>
      <p>Loading merkle proof...</p>
    </div>
  {:else if error}
    <div class="error">
      <p><strong>Error:</strong> {error}</p>
    </div>
  {:else if merkleProof}
    <!-- Compact Info Header -->
    <div class="info-header">
      <div class="info-grid">
        <div class="info-item">
          <span class="label">Block Height:</span>
          <span class="value">{merkleProof.blockHeight}</span>
        </div>
        <div class="info-item">
          <span class="label">Path Levels:</span>
          <span class="value">{merkleProof.path.length}</span>
        </div>
        <div class="info-item">
          <span class="label">Proof Format:</span>
          <span class="value">BUMP (BRC-74)</span>
        </div>
        <div class="info-item">
          <span class="label">Tree Nodes:</span>
          <span class="value">{merkleProof.path.reduce((sum, level) => sum + level.length, 0)}</span>
        </div>
      </div>
      
      <div class="architecture-note">
        <strong>BUMP Format:</strong> This visualization shows the BSV Unified Merkle Path (BRC-74) for this transaction, providing an efficient proof of inclusion in the blockchain.
      </div>
    </div>

    <!-- Navigation Controls -->
    <div class="controls-bar">
      <div class="legend-compact">
        <div class="legend-item"><div class="legend-color" style="background-color: {colors.transaction}"></div><span>Your TX</span></div>
        <div class="legend-item"><div class="legend-color" style="background-color: {colors.subtreeProof}"></div><span>Proof Path</span></div>
        <div class="legend-item"><div class="legend-color" style="background-color: {colors.subtreeRoot}"></div><span>Subtree Root</span></div>
        <div class="legend-item"><div class="legend-color" style="background-color: {colors.sibling}"></div><span>Siblings</span></div>
        <div class="legend-item"><div class="legend-color" style="background-color: {colors.placeholder}"></div><span>Other TXs</span></div>
      </div>
      
      <div class="navigation-controls">
        <button class="nav-button" on:click={focusOnTransaction} title="Focus on Transaction">🎯</button>
        <button class="nav-button" on:click={zoomToFit} title="Zoom to Fit">🔍</button>
        <button class="nav-button" on:click={resetZoom} title="Reset Zoom">🏠</button>
        <button class="nav-button help-button" on:click={() => showNavigationHelp = !showNavigationHelp} title="Navigation Help">❓</button>
      </div>
    </div>

    {#if showNavigationHelp}
      <div class="navigation-help">
        <div class="help-content">
          <div class="help-section">
            <h4>Mouse Controls:</h4>
            <ul>
              <li><strong>Wheel:</strong> Zoom in/out</li>
              <li><strong>Drag:</strong> Pan around tree</li>
            </ul>
          </div>
          <div class="help-section">
            <h4>Buttons:</h4>
            <ul>
              <li><strong>🎯:</strong> Focus on transaction</li>
              <li><strong>🔍:</strong> Fit entire tree</li>
              <li><strong>🏠:</strong> Reset view</li>
            </ul>
          </div>
          <div class="help-section">
            <h4>Shortcuts:</h4>
            <ul>
              <li><strong>Shift+T:</strong> Focus TX</li>
              <li><strong>Shift+F:</strong> Fit all</li>
              <li><strong>Shift+R:</strong> Reset</li>
            </ul>
          </div>
        </div>
      </div>
    {/if}

    <!-- Tree Visualization -->
    <div class="tree-container" bind:this={container}></div>
  {:else}
    <div class="no-data">
      <p>No merkle proof data available for this transaction.</p>
    </div>
  {/if}
</div>

<style>
  .merkle-visualizer {
    width: 100%;
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  /* Loading and Error States */
  .loading, .error, .no-data {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 2rem;
    text-align: center;
    min-height: 200px;
  }

  .loading .spinner {
    width: 32px;
    height: 32px;
    border: 3px solid #f3f3f3;
    border-top: 3px solid #007bff;
    border-radius: 50%;
    animation: spin 1s linear infinite;
    margin-bottom: 1rem;
  }

  @keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  }

  .error {
    background-color: #f8d7da;
    border: 1px solid #f5c6cb;
    border-radius: 0.375rem;
    color: #721c24;
  }

  /* Compact Info Header */
  .info-header {
    background-color: #f8f9fa;
    border: 1px solid #dee2e6;
    border-radius: 0.375rem;
    padding: 1rem;
  }

  .info-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 0.75rem;
  }

  .info-item {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .info-item .label {
    font-weight: 600;
    color: #495057;
    min-width: 80px;
  }

  .info-item .value {
    font-family: monospace;
    background-color: #e9ecef;
    color: #212529;
    padding: 0.25rem 0.5rem;
    border-radius: 0.25rem;
    font-size: 0.875rem;
  }

  .architecture-note {
    margin-top: 0.75rem;
    padding: 0.75rem;
    background-color: #d1ecf1;
    border: 1px solid #bee5eb;
    border-radius: 0.25rem;
    font-size: 0.875rem;
    color: #0c5460;
  }

  /* Controls Bar */
  .controls-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    padding: 0.75rem;
    background-color: #ffffff;
    border: 1px solid #dee2e6;
    border-radius: 0.375rem;
  }

  .legend-compact {
    display: flex;
    gap: 1rem;
    flex-wrap: wrap;
  }

  .legend-item {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: 0.875rem;
    white-space: nowrap;
    color: #495057;
  }

  .legend-color {
    width: 12px;
    height: 12px;
    border-radius: 2px;
    border: 1px solid rgba(0,0,0,0.1);
  }

  .navigation-controls {
    display: flex;
    gap: 0.5rem;
  }

  .nav-button {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 36px;
    height: 36px;
    background-color: #007bff;
    color: white;
    border: none;
    border-radius: 0.25rem;
    cursor: pointer;
    font-size: 1rem;
    transition: all 0.2s;
  }

  .nav-button:hover {
    background-color: #0056b3;
    transform: translateY(-1px);
  }

  .nav-button:active {
    transform: translateY(0);
  }

  .help-button {
    background-color: #6c757d;
  }

  .help-button:hover {
    background-color: #545b62;
  }

  /* Help Section */
  .navigation-help {
    background-color: #f8f9fa;
    border: 1px solid #dee2e6;
    border-radius: 0.375rem;
    padding: 1rem;
  }

  .help-content {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1.5rem;
  }

  .help-section h4 {
    margin: 0 0 0.5rem 0;
    color: #495057;
    font-size: 0.9rem;
    font-weight: 600;
  }

  .help-section ul {
    margin: 0;
    padding-left: 1rem;
    list-style: none;
  }

  .help-section li {
    margin: 0.25rem 0;
    font-size: 0.875rem;
    padding-left: 0.5rem;
    border-left: 2px solid #dee2e6;
  }

  /* Tree Container */
  .tree-container {
    width: 100%;
    height: 500px;
    border: 1px solid #dee2e6;
    border-radius: 0.375rem;
    overflow: hidden;
    background-color: #ffffff;
  }

  /* Responsive Design */
  @media (max-width: 768px) {
    .merkle-visualizer {
      gap: 0.75rem;
    }

    .info-grid {
      grid-template-columns: 1fr;
      gap: 0.5rem;
    }

    .info-item {
      flex-direction: column;
      align-items: flex-start;
      gap: 0.25rem;
    }

    .info-item .label {
      min-width: unset;
      font-size: 0.875rem;
    }

    .controls-bar {
      flex-direction: column;
      align-items: stretch;
      gap: 0.75rem;
    }

    .legend-compact {
      justify-content: center;
      gap: 0.75rem;
    }

    .navigation-controls {
      justify-content: center;
    }

    .help-content {
      grid-template-columns: 1fr;
      gap: 1rem;
    }

    .tree-container {
      height: 400px;
    }
  }

  @media (max-width: 480px) {
    .legend-compact {
      grid-template-columns: 1fr 1fr;
      gap: 0.5rem;
    }

    .legend-item {
      font-size: 0.8rem;
    }

    .nav-button {
      width: 32px;
      height: 32px;
      font-size: 0.9rem;
    }

    .tree-container {
      height: 350px;
    }
  }
</style>