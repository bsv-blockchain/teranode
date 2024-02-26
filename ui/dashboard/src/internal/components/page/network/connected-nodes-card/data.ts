import { formatNum, shortHash } from '$lib/utils/format'
import { getDetailsUrl, DetailType } from '$internal/utils/urls'
// eslint-ignore-next-line
import RenderLink from '$lib/components/table/renderers/render-link/index.svelte'

const pageKey = 'page.network.nodes'
const fieldKey = `${pageKey}.fields`

export const getColDefs = (t) => {
  return [
    {
      id: 'base_url',
      name: t(`${fieldKey}.base_url`),
      type: 'string',
      props: {
        width: '21%',
      },
    },
    {
      id: 'height',
      name: t(`${fieldKey}.height`),
      type: 'number',
      props: {
        width: '9%',
      },
    },
    {
      id: 'tx_count',
      name: t(`${fieldKey}.tx_count`),
      type: 'number',
      props: {
        width: '10%',
      },
    },
    {
      id: 'size_in_bytes',
      name: t(`${fieldKey}.size_in_bytes`),
      type: 'number',
      format: 'dataSize',
      props: {
        width: '10%',
      },
    },
    {
      id: 'miner',
      name: t(`${fieldKey}.miner`),
      type: 'string',
      props: {
        width: '12%',
      },
    },
    {
      id: 'hash',
      name: t(`${fieldKey}.hash`),
      type: 'string',
      props: {
        width: '14%',
      },
    },
    {
      id: 'previousblockhash',
      name: t(`${fieldKey}.previousblockhash`),
      type: 'string',
      props: {
        width: '14%',
      },
    },
    {
      id: 'receivedAt',
      name: t(`${fieldKey}.receivedAt`),
      type: 'string',
      props: {
        width: '10%',
      },
    },
  ]
}

export const filters = {}

export const renderCells = {
  height: (idField, item, colId) => {
    return {
      component: item[colId] ? RenderLink : null,
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
      component: item[colId] ? RenderLink : null,
      props: {
        href: getDetailsUrl(DetailType.block, item.hash),
        external: false,
        text: shortHash(item[colId]),
      },
      value: '',
    }
  },
  previousblockhash: (idField, item, colId) => {
    return {
      component: item[colId] ? RenderLink : null,
      props: {
        href: getDetailsUrl(DetailType.block, item[colId]),
        external: false,
        text: shortHash(item[colId]),
      },
      value: '',
    }
  },
}
