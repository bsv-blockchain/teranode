import { DetailType, getHashLinkProps } from '$internal/utils/urls'

import LinkHashCopy from '$internal/components/item-renderers/link-hash-copy/index.svelte'

const baseKey = 'page.viewer-subtree.txs'
const labelKey = `${baseKey}.col-defs-label`

export const mockData = {
  data: [
    {
      index: 0,
      txid: '0000000000000000084e2487ac4ab69e7418a5a4d19bb53ad3bc5dd859543680',
      inputsCount: Math.round(Math.random() * 100),
      outputsCount: Math.round(Math.random() * 100),
      kbFee: `${(Math.random() * 100).toFixed(2)} BSV`,
      size: Math.random() * 200,
      timestamp: 1701348374,
    },
    {
      index: 1,
      txid: '0000000000000000084e2487ac4ab69e7418a5a4d19bb53ad3bc5dd859543681',
      inputsCount: Math.round(Math.random() * 100),
      outputsCount: Math.round(Math.random() * 100),
      kbFee: `${(Math.random() * 100).toFixed(2)} BSV`,
      size: Math.random() * 200,
      timestamp: 1701348374,
    },
    {
      index: 2,
      txid: '0000000000000000084e2487ac4ab69e7418a5a4d19bb53ad3bc5dd859543682',
      inputsCount: Math.round(Math.random() * 100),
      outputsCount: Math.round(Math.random() * 100),
      kbFee: `${(Math.random() * 100).toFixed(2)} BSV`,
      size: Math.random() * 200,
      timestamp: 1701348374,
    },
    {
      index: 3,
      txid: '0000000000000000084e2487ac4ab69e7418a5a4d19bb53ad3bc5dd859543683',
      inputsCount: Math.round(Math.random() * 100),
      outputsCount: Math.round(Math.random() * 100),
      kbFee: `${(Math.random() * 100).toFixed(2)} BSV`,
      size: Math.random() * 200,
      timestamp: 1701348374,
    },
    {
      index: 4,
      txid: '0000000000000000084e2487ac4ab69e7418a5a4d19bb53ad3bc5dd859543684',
      inputsCount: Math.round(Math.random() * 100),
      outputsCount: Math.round(Math.random() * 100),
      kbFee: `${(Math.random() * 100).toFixed(2)} BSV`,
      size: Math.random() * 200,
      timestamp: 1701348374,
    },
    {
      index: 5,
      txid: '0000000000000000084e2487ac4ab69e7418a5a4d19bb53ad3bc5dd859543685',
      inputsCount: Math.round(Math.random() * 100),
      outputsCount: Math.round(Math.random() * 100),
      kbFee: `${(Math.random() * 100).toFixed(2)} BSV`,
      size: Math.random() * 200,
      timestamp: 1701348374,
    },
    {
      index: 6,
      txid: '0000000000000000084e2487ac4ab69e7418a5a4d19bb53ad3bc5dd859543686',
      inputsCount: Math.round(Math.random() * 100),
      outputsCount: Math.round(Math.random() * 100),
      kbFee: `${(Math.random() * 100).toFixed(2)} BSV`,
      size: Math.random() * 200,
      timestamp: 1701348374,
    },
  ],
}

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
        width: '20%',
      },
    },
    {
      id: 'inputsCount',
      name: t(`${labelKey}.inputsCount`),
      type: 'number',
      props: {
        width: '10%',
      },
    },
    {
      id: 'outputsCount',
      name: t(`${labelKey}.outputsCount`),
      type: 'number',
      props: {
        width: '10%',
      },
    },
    {
      id: 'kbFee',
      name: t(`${labelKey}.kbFee`),
      type: 'string',
      props: {
        width: '15%',
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
    {
      id: 'timestamp',
      name: t(`${labelKey}.timestamp`),
      type: 'dateStr',
      props: {
        width: '20%',
      },
    },
  ]
}

export const filters = {}

export const getRenderCells = (t) => {
  return {
    txid: (idField, item, colId) => {
      return item.txid === 'COINBASE'
        ? { value: 'COINBASE' }
        : {
            component: item[colId] ? LinkHashCopy : null,
            props: getHashLinkProps(DetailType.tx, item.txid, t),
            value: '',
          }
    },
  }
}
