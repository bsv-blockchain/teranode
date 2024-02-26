import { DetailType, getHashLinkProps } from '$internal/utils/urls'

import LinkHashCopy from '$internal/components/item-renderers/link-hash-copy/index.svelte'

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
      type: 'string',
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

export const getRenderCells = (t) => {
  return {
    txid: (idField, item, colId) => {
      return item.txid === 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'
        ? { value: 'COINBASE' }
        : {
            component: item[colId] ? LinkHashCopy : null,
            props: getHashLinkProps(DetailType.tx, item.txid, t),
            value: '',
          }
    },
  }
}
