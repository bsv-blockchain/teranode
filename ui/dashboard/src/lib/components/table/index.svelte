<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { SortOrder } from './utils'
  import { filterData, sortData, paginateData } from './hooks'
  import type { I18n } from '$lib/types'
  import { TableVariant, type ColDef, type TableVariantType } from './types'

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  //   export let selectable = false
  //   export let i18n = {}
  //   export let selectedRowIds = []
  //   export let onRowSelect
  //   export let getRowClassName
  //
  //

  // core
  export let variant: string = TableVariant.dynamic
  export let name
  export let colDefs: ColDef[] = []
  export let data: any[] = []
  export let idField = 'id'
  export let renderCells = {}
  export let renderTypes = {}
  export let disabled = false
  export let expandUp = false
  export let wrapTitles = true

  // i18n
  export let i18n: I18n | null | undefined

  // cosmetics - legacy
  export let maxHeight = -1
  export let fullWidth = true
  export let bgColorTable = null // = '#FCFCFF'
  export let bgColorHead = null // = '#FFFFFF'
  // more legacy
  export let selectable = false
  export let selectedRowIds = []
  export let getRowIconActions
  export let getRenderProps
  // export let getRowClassName

  // filters
  export let filters = {}
  export let filtersEnabled = true
  export let useServerFilters = false
  // sort
  export let sort: { sortColumn?: string; sortOrder?: string } = {}
  export let sortEnabled = true
  export let useServerSort = false
  export let sortByTypeFunctions = {}
  // paginate
  export let pagination = { page: 1, pageSize: 10 }
  export let paginationEnabled = true
  export let useServerPagination = false
  export let hasBoundaryRight = true
  export let pager = true // show pager?
  export let alignPager = 'center'

  // use server catch-all
  export let useServerAll = false

  // define preferences for server interaction
  let serverPagination = false
  let serverSort = false
  let serverFilters = false

  $: {
    serverPagination = useServerAll || useServerPagination
    serverSort = useServerAll || useServerSort
    serverFilters = useServerAll || useServerFilters
  }

  // filter
  let filteredData: any[] = data ? [...data] : []
  $: filtersState = { ...filters }

  $: {
    filteredData = filterData(data, filtersEnabled, serverFilters, filtersState)
  }

  // if either data changes or our filters change the number of data items, we need to reset
  // page to 1, to avoid pointing to out-of-bounds pages
  $: if (filteredData) {
    pagination.page = 1
  }

  function updateFilters(key, filter) {
    if (!filtersEnabled) {
      return
    }
    filtersState = {
      ...filtersState,
      [key]: {
        ...(filtersState[key] || {}),
        ...filter,
      },
    }
    dispatch('filter', { name, filters: { ...filtersState } })
  }

  // sort
  let sortedData = [...filteredData]
  $: sortState = { ...sort }

  $: {
    sortedData = sortData(
      filteredData,
      sortEnabled,
      serverSort,
      sortState,
      colDefs,
      sortByTypeFunctions,
    )
  }

  function toggleSort(colId) {
    if (!sortEnabled) {
      return
    }
    let value: string | null = null
    if (sortState?.sortColumn === colId) {
      value = sortState?.sortOrder === SortOrder.asc ? SortOrder.desc : SortOrder.asc
    } else {
      value = SortOrder.desc
    }
    sortState = { sortColumn: colId, sortOrder: value }
    dispatch('sort', { name, colId, value })
  }

  // paginate
  let pageData = [...sortedData]
  $: paginationState = { ...pagination }

  $: {
    pageData = paginateData(sortedData, paginationEnabled, serverPagination, paginationState)
  }

  function updatePagination(state) {
    paginationState = { ...state }
    dispatch('paginate', { name, ...paginationState })
  }

  let renderComp: any = null

  async function setRenderComp(variant: string) {
    try {
      if (variant === TableVariant.div) {
        renderComp = (await import('./variant/div-table/index.svelte')).default
      } else {
        renderComp = (await import('./variant/standard-table/index.svelte')).default
      }
    } catch (e) {
      console.error('Error loading table variant:', e)
    }
  }
  $: {
    let useVariant = variant
    if (variant === TableVariant.dynamic) {
      useVariant = $mediaSize <= MediaSize.sm ? TableVariant.div : TableVariant.standard
    }
    setRenderComp(useVariant)
  }

  let renderProps = {}
  $: {
    renderProps = {
      name,
      colDefs,
      data: pageData,
      idField,
      renderCells,
      renderTypes,
      disabled,
      expandUp,
      wrapTitles,
      i18n,
      maxHeight,
      fullWidth,
      bgColorTable,
      bgColorHead,
      selectable,
      selectedRowIds,
      filtersEnabled,
      filtersState,
      sortEnabled,
      sortState,
      paginationEnabled,
      paginationState,
      totalItems: sortedData.length,
      hasBoundaryRight,
      pager,
      alignPager,
      getRowIconActions,
      getRenderProps,
      // getRowClassName,
    }
  }

  function onFilterClick(e) {
    const colId = e.detail.colId
    console.log('onFilterClick: colId =', colId)
  }

  function onHeaderClick(e) {
    toggleSort(e.detail.colId)
  }

  function onPaginate(e) {
    updatePagination(e.detail.value)
  }

  function onAction(e) {
    const { type, value } = e.detail
    dispatch('action', { name, type, value })
  }
</script>

<div class="tui-table" data-test-id={testId}>
  {#if renderComp}
    <!-- ts-lint-ignore -->
    <svelte:component
      this={renderComp}
      {...renderProps}
      on:filter={onFilterClick}
      on:header={onHeaderClick}
      on:paginate={onPaginate}
      on:action={onAction}
    />
  {:else}
    <div>Unknown table variant.</div>
  {/if}
</div>

<style>
  .tui-table {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    width: 100%;
  }
</style>
