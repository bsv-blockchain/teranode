import { formatNum, shortHash } from '$lib/utils/format'
import { getDetailsUrl, DetailType } from '$internal/utils/urls'
import { humanTime } from '$internal/utils/format'
import { valueSet } from '$lib/utils/types'
// eslint-ignore-next-line
import RenderLink from '$lib/components/table/renderers/render-link/index.svelte'
import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'

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
  
  // Convert to array and sort in ascending order (lower chainwork = lower score)
  const sortedChainworks = Array.from(chainworkSet).sort((a, b) => {
    // Compare hex strings as big integers
    if (a.length !== b.length) {
      return a.length - b.length
    }
    return a.localeCompare(b)
  })
  
  // Assign scores (1 is lowest, n is highest)
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
      id: 'base_url',
      name: t(`${fieldKey}.base_url`),
      type: 'string',
      props: {
        width: '14%',
      },
    },
    {
      id: 'version',
      name: t(`${fieldKey}.version`),
      type: 'string',
      props: {
        width: '9%',
      },
    },
    {
      id: 'fsm_state',
      name: t(`${fieldKey}.fsm_state`),
      type: 'string',
      props: {
        width: '8%',
      },
    },
    {
      id: 'best_height',
      name: t(`${fieldKey}.height`),
      type: 'number',
      props: {
        width: '8%',
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
      id: 'tx_count_in_assembly',
      name: t(`${fieldKey}.tx_assembly`),
      type: 'number',
      props: {
        width: '8%',
      },
    },
    {
      id: 'uptime',
      name: t(`${fieldKey}.uptime`),
      type: 'number',
      props: {
        width: '8%',
      },
    },
    {
      id: 'miner_name',
      name: t(`${fieldKey}.miner`),
      type: 'string',
      props: {
        width: '9%',
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
      id: 'best_block_hash',
      name: t(`${fieldKey}.hash`),
      type: 'string',
      props: {
        width: '14%',
      },
    },
    {
      id: 'receivedAt',
      name: t(`${fieldKey}.last_update`),
      type: 'number',
      props: {
        width: '14%',
      },
    },
  ]
}

export const filters = {}

// Function to get render props for cells and rows
export const getRenderProps = (name: any, colDef: any, idField: any, item: any) => {
  // Add special styling for the current node row
  if (item?.isCurrentNode) {
    return {
      className: 'current-node-row'
    }
  }
  return {}
}

export const renderCells = {
  base_url: (idField, item, colId) => {
    const url = item[colId] || '-'
    const isCurrentNode = item.isCurrentNode === true
    
    return {
      component: RenderSpan,
      props: {
        value: url,
        className: isCurrentNode ? 'current-node-url' : '',
        title: isCurrentNode ? 'This is your node' : url,
      },
      value: '',
    }
  },
  version: (idField, item, colId) => {
    const version = item.version || '-'
    const commitHash = item.commit_hash ? ` (${item.commit_hash.slice(0, 7)})` : ''
    return {
      component: RenderSpan,
      props: {
        value: version + commitHash,
        className: '',
      },
      value: '',
    }
  },
  fsm_state: (idField, item, colId) => {
    const state = item[colId] || '-'
    let className = ''
    // Color code based on actual FSM states
    if (state === 'RUNNING') {
      className = 'status-success'
    } else if (state === 'CATCHINGBLOCKS' || state === 'LEGACYSYNCING') {
      className = 'status-warning'
    } else if (state === 'IDLE') {
      className = 'status-info'
    }
    return {
      component: RenderSpan,
      props: {
        value: state,
        className: className,
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
    const maxScore = item.maxChainworkScore || 0
    const isTopScore = score > 0 && score === maxScore
    
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
  tx_count_in_assembly: (idField, item, colId) => {
    return {
      component: RenderSpan,
      props: {
        value: item[colId] !== undefined ? formatNum(item[colId]) : '-',
        className: 'num',
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
    // Use humanTime function to calculate uptime from start time
    const startTime = item.start_time * 1000 // Convert to milliseconds
    const uptimeStr = humanTime(startTime)

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
    return {
      component: hash ? RenderLink : null,
      props: {
        href: getDetailsUrl(DetailType.block, hash),
        external: false,
        text: shortHash(hash),
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
