import { formatNum, shortHash } from '$lib/utils/format'
import { getDetailsUrl, DetailType } from '$internal/utils/urls'
import { humanTime } from '$internal/utils/format'
import { valueSet } from '$lib/utils/types'
// eslint-ignore-next-line
import RenderLink from '$lib/components/table/renderers/render-link/index.svelte'
import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'

const pageKey = 'page.network.nodes'
const fieldKey = `${pageKey}.fields`

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

export const renderCells = {
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
