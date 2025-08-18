import { formatNum } from '$lib/utils/format'
import { valueSet } from '$lib/utils/types'
import { getDetailsUrl, DetailType, getHashLinkProps } from '$internal/utils/urls'
import { getTpsStrFromValue } from '$internal/utils/txs'
import { getHumanReadableTime } from '$internal/utils/format'
// eslint-ignore-next-line
import RenderLink from '$lib/components/table/renderers/render-link/index.svelte'
import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'
import RenderHashWithMiner from '$lib/components/table/renderers/render-hash-with-miner/index.svelte'

const pageKey = 'page.viewer'

export const getColDefs = (t) => {
  return [
    {
      id: 'height',
      name: t(`${pageKey}.col-defs-label.height`),
      type: 'number',
      props: {
        width: '8%',
      },
    },
    {
      id: 'hash',
      name: t(`${pageKey}.col-defs-label.hash`),
      type: 'string',
      props: {
        width: '26%',  // Increased width since miner is now included
      },
    },
    {
      id: 'timestamp',
      name: t(`${pageKey}.col-defs-label.timestamp`),
      type: 'dateStr',
      props: {
        width: '14%',
      },
    },
    {
      id: 'age',
      name: t(`${pageKey}.col-defs-label.age`),
      type: 'number',
      props: {
        width: '8%',
      },
    },
    {
      id: 'deltaTime',
      name: t(`${pageKey}.col-defs-label.deltaTime`),
      type: 'number',
      props: {
        width: '7%',
      },
    },
    {
      id: 'coinbaseValue',
      name: t(`${pageKey}.col-defs-label.coinbaseValue`),
      type: 'number',
      props: {
        width: '7%',
      },
    },
    {
      id: 'transactionCount',
      name: t(`${pageKey}.col-defs-label.transactionCount`),
      type: 'number',
      props: {
        width: '10%',
      },
    },
    {
      id: 'tps',
      name: t(`${pageKey}.col-defs-label.tps`),
      type: 'number',
      props: {
        width: '10%',
      },
    },
    {
      id: 'size',
      name: t(`${pageKey}.col-defs-label.size`),
      type: 'number',
      format: 'dataSize',
      props: {
        width: '10%',
      },
    },
  ]
}

export const filters = {}

export const getRenderCells = (t) => {
  return {
    height: (idField, item, colId) => {
      return {
        component: valueSet(item[colId]) ? RenderLink : null,
        props: {
          href: getDetailsUrl(DetailType.block, item.hash),
          external: false,
          text: formatNum(item[colId]),
          className: 'num',
        },
        value: '',
      }
    },
    hash: (idField, item, colId) => {
      const hash = item[colId]
      const shortHash = hash ? (hash.length > 16 ? `${hash.slice(0, 8)}...${hash.slice(-8)}` : hash) : ''
      return {
        component: item[colId] ? RenderHashWithMiner : null,
        props: {
          hash: hash,
          hashUrl: getDetailsUrl(DetailType.block, hash),
          shortHash: shortHash,
          miner: item.miner || '',
          showCopyButton: true,
          copyTooltip: t('tooltip.copy-hash-to-clipboard'),
          tooltip: hash,
        },
        value: '',
      }
    },
    age: (idField, item, colId) => {
      return {
        component: valueSet(item[colId]) ? RenderSpan : null,
        props: {
          value: getHumanReadableTime(item[colId]),
          className: 'num',
        },
        value: '',
      }
    },
    deltaTime: (idField, item, colId) => {
      return {
        component: valueSet(item[colId]) ? RenderSpan : null,
        props: {
          value: getHumanReadableTime(item[colId]),
          className: 'num',
        },
        value: '',
      }
    },
    coinbaseValue: (idField, item, colId) => {
      return {
        component: valueSet(item[colId]) ? RenderSpan : null,
        props: {
          value: (item[colId] / 1e8).toFixed(2),
          className: 'num',
        },
        value: '',
      }
    },
    tps: (idField, item, colId) => {
      return {
        component: valueSet(item[colId]) ? RenderSpan : null,
        props: {
          value: getTpsStrFromValue(item[colId]),
          className: 'num',
        },
        value: '',
      }
    },
  }
}
