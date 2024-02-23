import { formatNum, formatSatoshi } from '$lib/utils/format'
import { valueSet } from '$lib/utils/types'
import { getDetailsUrl, DetailType, getHashLinkProps } from '$internal/utils/urls'
// eslint-ignore-next-line
import RenderLink from '$lib/components/table/renderers/render-link/index.svelte'
import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'
import LinkHashCopy from '$internal/components/item-renderers/link-hash-copy/index.svelte'

const baseKey = 'page.viewer-block.subtrees'
const labelKey = `${baseKey}.col-defs-label`

export const getColDefs = (t) => {
  return [
    {
      id: 'index',
      name: t(`${labelKey}.index`),
      type: 'number',
      props: {
        width: '18%',
      },
    },
    {
      id: 'hash',
      name: t(`${labelKey}.hash`),
      type: 'string',
      props: {
        width: '22%',
      },
    },
    {
      id: 'txCount',
      name: t(`${labelKey}.txCount`),
      type: 'number',
      props: {
        width: '22%',
      },
    },
    {
      id: 'fee',
      name: t(`${labelKey}.fee`),
      type: 'string',
      props: {
        width: '23%',
      },
    },
    {
      id: 'size',
      name: t(`${labelKey}.size`),
      type: 'number',
      props: {
        width: '15%',
      },
    },
  ]
}

export const filters = {}

export const getRenderCells = (t, blockHash) => {
  return {
    index: (idField, item, colId) => {
      return {
        component: valueSet(item[colId]) ? RenderLink : null,
        props: {
          href: getDetailsUrl(DetailType.subtree, item.hash, { blockHash }),
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
        props: {
          ...getHashLinkProps(DetailType.subtree, item.hash, t),
          href: getDetailsUrl(DetailType.subtree, item.hash, { blockHash }),
        },
        value: '',
      }
    },
    fee: (idField, item, colId) => {
      return {
        value: formatSatoshi(item[colId]) + ' BSV',
      }
    },
    size: (idField, item, colId) => {
      return {
        component: valueSet(item[colId]) ? RenderSpan : null,
        props: {
          value: formatNum(item[colId] / 1000) + ' KB',
          className: 'num',
        },
        value: '',
      }
    },
  }
}
