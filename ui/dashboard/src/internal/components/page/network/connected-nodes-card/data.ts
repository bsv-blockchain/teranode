import { formatNum, shortHash } from '$lib/utils/format'
import { getDetailsUrl, DetailType } from '$internal/utils/urls'
import { humanTime, humanTimeNoSeconds } from '$internal/utils/format'
import { valueSet } from '$lib/utils/types'
// eslint-ignore-next-line
import RenderLink from '$lib/components/table/renderers/render-link/index.svelte'
import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'
import RenderSpanWithTooltip from '$lib/components/table/renderers/render-span-with-tooltip/index.svelte'
import RenderHashWithMiner from '$lib/components/table/renderers/render-hash-with-miner/index.svelte'
import RenderClickableSpan from '$lib/components/table/renderers/render-clickable-span/index.svelte'
import { blockAssemblyModalStore } from '$internal/stores/blockAssemblyModalStore'
import { blockHashToMiner } from '$internal/stores/p2pStore'
import { get } from 'svelte/store'

const pageKey = 'page.network.nodes'
const fieldKey = `${pageKey}.fields`

// Function to calculate chainwork scores
export function calculateChainworkScores(nodes: any[]): Map<string, number> {
  const scoreMap = new Map<string, number>()
  
  // Collect unique chainwork values and filter out empty/invalid ones
  const chainworkSet = new Set<string>()
  nodes.forEach(node => {
    if (node.chain_work && node.chain_work.length > 0) {
      chainworkSet.add(node.chain_work)
    }
  })
  
  // Convert to array and sort in descending order (higher chainwork = lower score number)
  const sortedChainworks = Array.from(chainworkSet).sort((a, b) => {
    // Compare hex strings as big integers (reversed for descending)
    if (a.length !== b.length) {
      return b.length - a.length
    }
    return b.localeCompare(a)
  })

  // Assign scores (1 is highest chainwork, n is lowest)
  const chainworkToScore = new Map<string, number>()
  sortedChainworks.forEach((chainwork, index) => {
    chainworkToScore.set(chainwork, index + 1)
  })
  
  // Map each node to its score
  nodes.forEach(node => {
    const key = node.peer_id
    if (node.chain_work && chainworkToScore.has(node.chain_work)) {
      scoreMap.set(key, chainworkToScore.get(node.chain_work)!)
    } else {
      scoreMap.set(key, 0) // No chainwork = score 0
    }
  })
  
  return scoreMap
}

export const getColDefs = (t) => {
  return [
    {
      id: 'fsm_state',
      name: '',
      type: 'string',
      props: {
        width: '3%',
      },
    },
    {
      id: 'client_name',
      name: t(`${fieldKey}.client_name`),
      type: 'string',
      props: {
        width: '16%',
      },
    },
    {
      id: 'version',
      name: t(`${fieldKey}.version`),
      type: 'string',
      props: {
        width: '12%',
      },
    },
    {
      id: 'best_height',
      name: t(`${fieldKey}.height`),
      type: 'number',
      props: {
        width: '10%',
      },
    },
    {
      id: 'best_block_hash',
      name: t(`${fieldKey}.hash_and_miner`),
      type: 'string',
      props: {
        width: '20%',
      },
    },
    {
      id: 'chainwork_score',
      name: t(`${fieldKey}.chainwork_score`),
      type: 'number',
      props: {
        width: '8%',
      },
    },
    {
      id: 'tx_count',
      name: t(`${fieldKey}.tx_assembly`),
      type: 'number',
      props: {
        width: '6%',
      },
    },
    {
      id: 'subtree_count',
      name: t(`${fieldKey}.subtree_assembly`),
      type: 'number',
      props: {
        width: '6%',
      },
    },
    {
      id: 'min_mining_tx_fee',
      name: t(`${fieldKey}.min_mining_fee`),
      type: 'number',
      props: {
        width: '7%',
      },
    },
    {
      id: 'connected_peers_count',
      name: t(`${fieldKey}.connected_peers`),
      type: 'number',
      props: {
        width: '7%',
      },
    },
    {
      id: 'uptime',
      name: t(`${fieldKey}.uptime`),
      type: 'number',
      props: {
        width: '7%',
      },
    },
    {
      id: 'listen_mode',
      name: t(`${fieldKey}.listen_mode`),
      type: 'string',
      props: {
        width: '8%',
      },
    },
    {
      id: 'receivedAt',
      name: t(`${fieldKey}.last_update`),
      type: 'number',
      props: {
        width: '8%',
      },
    },
  ]
}

export const filters = {}

// Function to get render props for cells and rows
export const getRenderProps = (name: any, colDef: any, idField: any, item: any) => {
  // No special row styling needed
  return {}
}

export const renderCells = {
  client_name: (idField, item, colId) => {
    const clientName = item[colId] || item.client_name || '(not set)'
    const url = item.base_url || '-'
    const peerId = item.peer_id || '-'
    const isCurrentNode = item.isCurrentNode === true
    
    // Build tooltip with base URL and peer ID
    const tooltip = `${url}\n${peerId}`
    
    return {
      component: RenderSpanWithTooltip,
      props: {
        value: clientName,
        className: isCurrentNode ? 'current-node-name' : '',
        tooltip: tooltip,
      },
      value: '',
    }
  },
  version: (idField, item, colId) => {
    const fullVersion = item.version || '-'
    const commitHash = item.commit_hash || ''
    
    // Try to extract semantic version (e.g., "v1.2.3" from "v1.2.3-abc123")
    const semverMatch = fullVersion.match(/^(v?\d+\.\d+\.\d+)/)
    const displayVersion = semverMatch ? semverMatch[1] : fullVersion
    
    // Build tooltip with full version and commit
    let tooltipText = fullVersion
    if (commitHash) {
      tooltipText = `${fullVersion} (commit: ${commitHash})`
    }
    
    return {
      component: RenderSpanWithTooltip,
      props: {
        value: displayVersion,
        className: '',
        tooltip: tooltipText,
      },
      value: '',
    }
  },
  fsm_state: (idField, item, colId) => {
    const state = item[colId] || '-'
    let emoji = ''
    let tooltip = state
    
    // Add colorful emojis based on actual FSM states
    if (state === 'RUNNING') {
      emoji = '✅'
      tooltip = 'RUNNING'
    } else if (state === 'CATCHINGBLOCKS') {
      emoji = '🟠'
      tooltip = 'CATCHINGBLOCKS'
    } else if (state === 'LEGACYSYNC') {
      emoji = '🟡'
      tooltip = 'LEGACYSYNC'
    } else if (state === 'IDLE') {
      emoji = '⏸️'
      tooltip = 'IDLE'
    }
    
    return {
      component: RenderSpanWithTooltip,
      props: {
        value: emoji,
        className: '',
        tooltip: tooltip,
      },
      value: '',
    }
  },
  best_height: (idField, item, colId) => {
    // Support both best_height (from node_status) and height (from mining_on)
    const height = item[colId] || item.height
    return {
      component: height ? RenderLink : null,
      props: {
        href: getDetailsUrl(DetailType.block, item.best_block_hash || item.hash),
        external: false,
        text: formatNum(height),
        className: 'num',
      },
      value: '',
    }
  },
  chainwork_score: (idField, item, colId) => {
    // The score will be calculated and added to items in the parent component
    const score = item[colId] || 0
    const isTopScore = score === 1 // Score 1 is now the highest chainwork

    let displayValue = '-'
    let className = 'num'

    if (score > 0) {
      displayValue = score.toString()
      // Use CSS classes for coloring
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
  tx_count: (idField, item, colId) => {
    const txCount = item[colId] ?? item.tx_count ?? 0

    return {
      component: RenderSpan,
      props: {
        value: txCount !== undefined ? formatNum(txCount) : '-',
        className: 'num',
      },
      value: '',
    }
  },
  subtree_count: (idField, item, colId) => {
    const subtreeCount = item[colId] ?? item.subtree_count ?? 0

    return {
      component: RenderSpan,
      props: {
        value: subtreeCount !== undefined ? formatNum(subtreeCount) : '-',
        className: 'num',
      },
      value: '',
    }
  },
  min_mining_tx_fee: (idField, item, colId) => {
    // Use nullish coalescing to properly handle 0 as a valid value
    const fee = item[colId] ?? item.min_mining_tx_fee

    // Check if fee is truly undefined/null (0 is a valid value meaning "no minimum")
    if (fee === undefined || fee === null) {
      return {
        component: RenderSpan,
        props: { value: '-', className: 'num' },
        value: '',
      }
    }

    // Handle 0 as a special case - it means "no minimum fee"
    if (fee === 0) {
      return {
        component: RenderSpanWithTooltip,
        props: {
          value: '0 sat/kB',
          className: 'num',
          tooltip: 'No minimum fee required',
        },
        value: '',
      }
    }

    // The min_mining_tx_fee setting is in BSV per kilobyte
    // Convert from BSV/kB to satoshis per kilobyte
    // 1 BSV = 100,000,000 satoshis
    const satoshisPerKB = fee * 100000000

    // Format the display
    let displayValue = ''
    if (satoshisPerKB < 0.01) {
      // For very small values, show with more decimal places
      displayValue = `${satoshisPerKB.toFixed(4)} sat/kB`
    } else if (satoshisPerKB < 1) {
      // For values less than 1, show with 2 decimal places
      displayValue = `${satoshisPerKB.toFixed(2)} sat/kB`
    } else if (satoshisPerKB === Math.floor(satoshisPerKB)) {
      // For whole numbers, don't show decimal places
      displayValue = `${Math.floor(satoshisPerKB)} sat/kB`
    } else {
      // For other fractional values, show up to 2 decimal places
      displayValue = `${satoshisPerKB.toFixed(2)} sat/kB`
    }

    // Create a tooltip with both formats
    const bsvValue = fee.toFixed(8)
    const satPerByteStr = (satoshisPerKB / 1000).toFixed(4)
    const tooltip = `${displayValue}\n(${satPerByteStr} sat/B)\n(${bsvValue} BSV/kB)`

    return {
      component: RenderSpanWithTooltip,
      props: {
        value: displayValue,
        className: 'num',
        tooltip: tooltip,
      },
      value: '',
    }
  },
  connected_peers_count: (idField, item, colId) => {
    // Get the peer count value
    const peersCount = item[colId] ?? item.connected_peers_count ?? 0

    return {
      component: RenderSpan,
      props: {
        value: peersCount.toString(),
        className: 'num'
      },
      value: '',
    }
  },
  uptime: (idField, item, colId) => {
    if (!item[colId] || !item.start_time) {
      return {
        component: RenderSpan,
        props: { value: '-', className: 'num' },
        value: '',
      }
    }
    // Use humanTimeNoSeconds function to calculate uptime from start time
    const startTime = item.start_time * 1000 // Convert to milliseconds
    const uptimeStr = humanTimeNoSeconds(startTime)

    return {
      component: RenderSpan,
      props: {
        value: uptimeStr,
        className: 'num',
      },
      value: '',
    }
  },
  miner_name: (idField, item, colId) => {
    // Support both miner_name (from node_status) and miner (from mining_on)
    const miner = item[colId] || item.miner || '-'
    return {
      component: RenderSpan,
      props: {
        value: miner,
        className: '',
      },
      value: '',
    }
  },
  listen_mode: (idField, item, colId) => {
    const mode = item[colId] || '-'
    let className = ''
    let displayValue = mode

    // Make the display more user-friendly
    if (mode === 'full') {
      displayValue = 'Full'
      className = 'status-success'
    } else if (mode === 'listen_only') {
      displayValue = 'Listen Only'
      className = 'status-warning'
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
  best_block_hash: (idField, item, colId) => {
    // Support both best_block_hash (from node_status) and hash (from mining_on)
    const hash = item[colId] || item.hash
    let miner = item.miner_name || item.miner || ''
    
    // If miner is not available, lookup from block hash -> miner cache
    if (!miner && hash) {
      const minerCache = get(blockHashToMiner)
      miner = minerCache.get(hash) || ''
    }
    
    return {
      component: hash ? RenderHashWithMiner : null,
      props: {
        hash: hash,
        hashUrl: hash ? getDetailsUrl(DetailType.block, hash) : '',
        shortHash: hash ? shortHash(hash) : '',
        miner: miner,
        className: '',
        tooltip: hash ? `Full hash: ${hash}` : '',
        showCopyButton: true,
        copyTooltip: 'Copy hash',
      },
      value: '',
    }
  },
  receivedAt: (idField, item, colId) => {
    return {
      component: valueSet(item[colId]) ? RenderSpan : null,
      props: {
        value: humanTime(item[colId]),
        className: 'num',
      },
      value: '',
    }
  },
}
