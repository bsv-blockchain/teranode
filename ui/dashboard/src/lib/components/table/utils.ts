import RenderLink from './renderers/render-link/index.svelte'
import RenderSpan from './renderers/render-span/index.svelte'
import { dataSize, formatDate, formatNum, formatSatoshi } from '../../utils/format'
import { valueSet } from '../../utils/types'

export const ColType = {
  string: 'string',
  number: 'number',
  dateStr: 'dateStr',
  boolean: 'boolean',
  phone: 'phone',
  email: 'email',
  url: 'url',
  satoshi: 'satoshi',
  percent: 'percent',
  terraHash: 'terraHash',
  dataSize: 'dataSize',
}

export const FilterType = {
  anyOf: 'anyOf',
  bool: 'bool',
}

export const SortOrder = {
  asc: 'asc',
  desc: 'desc',
}

export const getSortFunction = (colType, sortColumn, sortOrder) => {
  switch (colType) {
    case ColType.dateStr:
      return (a, b) =>
        sortOrder === SortOrder.asc
          ? new Date(a[sortColumn]).getTime() - new Date(b[sortColumn]).getTime()
          : new Date(b[sortColumn]).getTime() - new Date(a[sortColumn]).getTime()
    case ColType.number:
    case ColType.dataSize:
    case ColType.satoshi:
    case ColType.terraHash:
    case ColType.percent:
      return (a, b) =>
        sortOrder === SortOrder.asc ? a[sortColumn] - b[sortColumn] : b[sortColumn] - a[sortColumn]
    case ColType.boolean:
      return (a, b) => {
        const factor = sortOrder === SortOrder.asc ? -1 : 1
        return a[sortColumn] === b[sortColumn] ? 0 : a[sortColumn] ? factor * -1 : factor * 1
      }
    default:
      return (a, b) => {
        const value1 = (a[sortColumn] || '').toString()
        const value2 = (b[sortColumn] || '').toString()
        return sortOrder === SortOrder.asc
          ? value1.localeCompare(value2)
          : value2.localeCompare(value1)
      }
  }
}

const str = (value) => {
  return valueSet(value) ? value : ''
}

const defaultColTypeRenderers = {
  [ColType.string]: (idField, item, colId) => ({
    value: str(item[colId]),
  }),
  [ColType.dataSize]: (idField, item, colId) => ({
    component: RenderSpan,
    props: { value: str(dataSize(item[colId])), className: 'num' },
  }),
  [ColType.number]: (idField, item, colId) => ({
    component: RenderSpan,
    props: { value: str(formatNum(item[colId])), className: 'num' },
  }),
  [ColType.satoshi]: (idField, item, colId) => ({
    component: RenderSpan,
    props: {
      value: item[colId] || item[colId] === 0 ? str(formatSatoshi(item[colId])) : '',
      className: 'num',
    },
  }),
  [ColType.dateStr]: (idField, item, colId) => ({
    value: item[colId] ? formatDate(item[colId]) : '',
  }),
  [ColType.boolean]: (idField, item, colId) => ({
    component: RenderSpan,
    props: {
      value: item[colId] ? '✔' : '✘',
      className: item[colId] ? 'green' : 'red',
    },
  }),
  [ColType.phone]: (idField, item, colId) => ({
    component: item[colId] ? RenderLink : null,
    props: {
      href: `tel:${str(item[colId])}`,
      text: str(item[colId]),
      className: 'phone',
    },
    value: '',
  }),
  [ColType.email]: (idField, item, colId) => ({
    component: item[colId] ? RenderLink : null,
    props: {
      href: `mailto:${str(item[colId])}`,
      text: str(item[colId]),
      className: 'email',
    },
    value: '',
  }),
  [ColType.url]: (idField, item, colId) => ({
    component: item[colId] ? RenderLink : null,
    props: {
      href: str(item[colId]),
      text: null,
      className: 'url',
    },
    value: '',
  }),
  [ColType.percent]: (idField, item, colId) => ({
    component: valueSet(item[colId]) ? RenderSpan : null,
    props: {
      value: str(formatNum(item[colId].toFixed(2))) + '%',
      className: 'num',
    },
    value: '',
  }),
  [ColType.terraHash]: (idField, item, colId) => ({
    component: valueSet(item[colId]) ? RenderSpan : null,
    props: {
      value: str(formatNum((item[colId] / 1e12).toFixed(2))) + 'TH/s',
      className: 'num',
    },
    value: '',
  }),
}

export const getDisplay = (renderCells, renderTypes, colDef, idField, item) => {
  return renderCells && renderCells[colDef.id]
    ? renderCells[colDef.id](idField, item, colDef.id)
    : renderTypes && renderTypes[colDef.type]
      ? renderTypes[colDef.type](idField, item, colDef.id)
      : defaultColTypeRenderers[colDef.format]
        ? defaultColTypeRenderers[colDef.format](idField, item, colDef.id)
        : defaultColTypeRenderers[colDef.type]
          ? defaultColTypeRenderers[colDef.type](idField, item, colDef.id)
          : { value: item[colDef.id] }
}
