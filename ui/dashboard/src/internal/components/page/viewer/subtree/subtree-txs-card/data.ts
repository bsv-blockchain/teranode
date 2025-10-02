import { DetailType, getHashLinkProps } from '$internal/utils/urls'
import { formatNum } from '$lib/utils/format'
import { valueSet } from '$lib/utils/types'

import LinkHashCopy from '$internal/components/item-renderers/link-hash-copy/index.svelte'
import RenderSpan from '$lib/components/table/renderers/render-span/index.svelte'

const baseKey = 'page.viewer-subtree.txs'
const labelKey = `${baseKey}.col-defs-label`

export const getColDefs = (t) => {
  return [
    {
      id: 'index',
      name: t(`${labelKey}.index`),
      type: 'number',
      props: {
        width: '10%',
      },
    },
    {
      id: 'txid',
      name: t(`${labelKey}.txid`),
      type: 'string',
      props: {
        width: '30%',
      },
    },
    {
      id: 'inputsCount',
      name: t(`${labelKey}.inputsCount`),
      type: 'number',
      props: {
        width: '15%',
      },
    },
    {
      id: 'outputsCount',
      name: t(`${labelKey}.outputsCount`),
      type: 'number',
      props: {
        width: '15%',
      },
    },
    {
      id: 'fee',
      name: t(`${labelKey}.fee`),
      type: 'number',
      props: {
        width: '15%',
      },
    },
    {
      id: 'size',
      name: t(`${labelKey}.size`),
      type: 'number',
      format: 'dataSize',
      props: {
        width: '15%',
      },
    },
  ]
}

export const filters = {}

export const getRenderCells = (t, blockHash = '', coinbaseTxId = '') => {
  return {
    txid: (idField, item, colId) => {
      if (item.txid === 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff') {
        // This is a coinbase placeholder
        if (coinbaseTxId) {
          // If we have the actual coinbase transaction ID, create a proper link
          return {
            component: LinkHashCopy,
            props: {
              ...getHashLinkProps(DetailType.tx, coinbaseTxId, t),
              text: 'COINBASE',
            },
            value: '',
          }
        }
        return { value: 'COINBASE' }
      }
      return {
        component: item[colId] ? LinkHashCopy : null,
        props: getHashLinkProps(DetailType.tx, item.txid, t),
        value: '',
      }
    },
    fee: (idField, item, colId) => {
      return {
        component: valueSet(item[colId]) ? RenderSpan : null,
        props: {
          value: formatNum(item[colId]) + ' sats',
          className: 'num',
        },
        value: '',
      }
    },
  }
}
