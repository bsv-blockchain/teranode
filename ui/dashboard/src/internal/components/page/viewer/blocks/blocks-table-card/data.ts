import { formatNum } from '$lib/utils/format'
import { valueSet } from '$lib/utils/types'
import { getDetailsUrl, DetailType, getHashLinkProps } from '$internal/utils/urls'
// eslint-ignore-next-line
import RenderLink from '$lib/components/table/renderers/render-link/index.svelte'
import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'
import LinkHashCopy from '$internal/components/item-renderers/link-hash-copy/index.svelte'

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
        width: '15%',
      },
    },
    {
      id: 'timestamp',
      name: t(`${pageKey}.col-defs-label.timestamp`),
      type: 'dateStr',
      props: {
        width: '15%',
      },
    },
    {
      id: 'age',
      name: t(`${pageKey}.col-defs-label.age`),
      type: 'string',
      props: {
        width: '8%',
      },
    },
    {
      id: 'deltaTime',
      name: t(`${pageKey}.col-defs-label.deltaTime`),
      type: 'string',
      props: {
        width: '7%',
      },
    },
    {
      id: 'miner',
      name: t(`${pageKey}.col-defs-label.miner`),
      type: 'string',
      props: {
        width: '10%',
      },
    },
    {
      id: 'coinbaseValue',
      name: t(`${pageKey}.col-defs-label.coinbaseValue`),
      type: 'number',
      props: {
        width: '8%',
      },
    },
    {
      id: 'transactionCount',
      name: t(`${pageKey}.col-defs-label.transactionCount`),
      type: 'number',
      props: {
        width: '8%',
      },
    },
    {
      id: 'tps',
      name: t(`${pageKey}.col-defs-label.tps`),
      type: 'string',
      props: {
        width: '9%',
      },
    },
    {
      id: 'size',
      name: t(`${pageKey}.col-defs-label.size`),
      type: 'number',
      props: {
        width: '12%',
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
      return {
        component: item[colId] ? LinkHashCopy : null,
        props: getHashLinkProps(DetailType.block, item.hash, t),
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
  }
}
