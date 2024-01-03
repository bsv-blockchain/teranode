import { valueSet } from '../../utils/types'
import { getSortFunction, FilterType } from './utils'

export const filterData = (data, filtersEnabled, serverFilters, filtersState) => {
  let d = data ? [...data] : []

  if (filtersEnabled) {
    if (!serverFilters) {
      Object.keys(filtersState).forEach((key) => {
        const filter = filtersState[key]
        if (filter.filterType === FilterType.bool) {
          if (filter.selectedValues.length) {
            d = d.filter((item) =>
              filter.selectedValues.includes(valueSet(item[key]) ? item[key] : false),
            )
          }
        }
        if (filter.filterType === FilterType.anyOf) {
          if (filter.selectedValues.length) {
            d = d.filter((item) => filter.selectedValues.includes(item[key]))
          }
        }
      })
    }
  }

  return d
}

export const sortData = (
  filteredData,
  sortEnabled,
  serverSort,
  sortState,
  colDefs,
  sortByTypeFunctions,
) => {
  if (
    !sortEnabled ||
    !sortState.sortColumn ||
    !sortState.sortOrder ||
    !colDefs ||
    colDefs.length === 0
  ) {
    return [...filteredData]
  } else {
    const d = [...filteredData]
    const { sortColumn, sortOrder } = sortState

    if (!serverSort) {
      const colDef = colDefs.filter((col) => col.id === sortColumn)[0]

      if (colDef.sortFunction) {
        d.sort(colDef.sortFunction(sortColumn, sortOrder))
      } else if (sortByTypeFunctions[colDef.type]) {
        d.sort(sortByTypeFunctions[colDef.type](sortColumn, sortOrder))
      } else {
        d.sort(getSortFunction(colDef.type, sortColumn, sortOrder))
      }
    }

    return d
  }
}

export const paginateData = (sortedData, paginationEnabled, serverPagination, paginationState) => {
  if (
    !paginationEnabled ||
    !valueSet(paginationState.page) ||
    !valueSet(paginationState.pageSize)
  ) {
    return [...sortedData]
  } else {
    const { page, pageSize } = paginationState
    let d = [...sortedData]

    if (!serverPagination) {
      // a pageSize of -1 indicates that page size is All and we should display all items
      if (pageSize !== -1) {
        const startIndex = (page - 1) * pageSize
        const endIndex = Math.min(startIndex + pageSize, sortedData.length)

        d = d.slice(startIndex, endIndex)
      }
    }

    return d
  }
}
