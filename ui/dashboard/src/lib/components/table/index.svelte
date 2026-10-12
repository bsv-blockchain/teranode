<svelte:options runes={true} />

<script lang="ts">
  import { untrack } from 'svelte'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { SortOrder } from './utils'
  import { filterData, sortData, paginateData } from './hooks'
  import type { I18n } from '$lib/types'
  import { TableVariant, type ColDef } from './types'

  let {
    testId = null,
    // core
    variant = TableVariant.dynamic,
    name,
    colDefs = [],
    data = [],
    idField = 'id',
    renderCells = {},
    renderTypes = {},
    disabled = false,
    expandUp = false,
    wrapTitles = true,
    // i18n
    i18n,
    // cosmetics - legacy
    maxHeight = -1,
    fullWidth = true,
    bgColorTable = null, // = '#FCFCFF'
    bgColorHead = null, // = '#FFFFFF'
    // more legacy
    selectable = false,
    selectedRowIds = [],
    getRowIconActions,
    getRenderProps,
    // filters
    filters = {},
    filtersEnabled = true,
    useServerFilters = false,
    // sort
    sort = {},
    sortEnabled = true,
    useServerSort = false,
    sortByTypeFunctions = {},
    // paginate
    pagination = $bindable({ page: 1, pageSize: 10 }),
    paginationEnabled = true,
    useServerPagination = false,
    hasBoundaryRight = true,
    pager = true, // show pager?
    alignPager = 'center',
    // use server catch-all
    useServerAll = false,
    onfilter,
    onsort,
    onpaginate,
    onaction,
  }: {
    testId?: string | undefined | null
    variant?: string
    name?: any
    colDefs?: ColDef[]
    data?: any[]
    idField?: string
    renderCells?: any
    renderTypes?: any
    disabled?: boolean
    expandUp?: boolean
    wrapTitles?: boolean
    i18n?: I18n | null | undefined
    maxHeight?: number
    fullWidth?: boolean
    bgColorTable?: any
    bgColorHead?: any
    selectable?: boolean
    selectedRowIds?: any[]
    getRowIconActions?: any
    getRenderProps?: any
    filters?: any
    filtersEnabled?: boolean
    useServerFilters?: boolean
    sort?: { sortColumn?: string; sortOrder?: string }
    sortEnabled?: boolean
    useServerSort?: boolean
    sortByTypeFunctions?: any
    pagination?: { page: number; pageSize: number }
    paginationEnabled?: boolean
    useServerPagination?: boolean
    hasBoundaryRight?: boolean
    pager?: boolean
    alignPager?: string
    useServerAll?: boolean
    onfilter?: (e: { name: any; filters: any }) => void
    onsort?: (e: { name: any; colId: any; value: any }) => void
    onpaginate?: (e: any) => void
    onaction?: (e: { name: any; type: any; value: any }) => void
  } = $props()

  // define preferences for server interaction
  const serverPagination = $derived(useServerAll || useServerPagination)
  const serverSort = $derived(useServerAll || useServerSort)
  const serverFilters = $derived(useServerAll || useServerFilters)

  // filter
  // filtersState is derived from the `filters` prop but is also reassigned
  // imperatively by updateFilters(), so keep it as $state synced via $effect.
  // untrack() in the initializer makes the intentional initial-value capture
  // explicit (avoids the state_referenced_locally warning).
  let filtersState = $state(untrack(() => ({ ...filters })))
  $effect(() => {
    filtersState = { ...filters }
  })

  const filteredData: any[] = $derived(
    filterData(data, filtersEnabled, serverFilters, filtersState),
  )

  // if either data changes or our filters change the number of data items, we need to reset
  // page to 1, to avoid pointing to out-of-bounds pages
  $effect(() => {
    if (filteredData) {
      pagination.page = 1
    }
  })

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
    onfilter?.({ name, filters: { ...filtersState } })
  }

  // sort
  // sortState mirrors the `sort` prop but is also reassigned by toggleSort().
  let sortState = $state(untrack(() => ({ ...sort })))
  $effect(() => {
    sortState = { ...sort }
  })

  const sortedData = $derived(
    sortData(filteredData, sortEnabled, serverSort, sortState, colDefs, sortByTypeFunctions),
  )

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
    onsort?.({ name, colId, value })
  }

  // paginate
  // paginationState mirrors the `pagination` prop but is also reassigned by
  // updatePagination().
  let paginationState = $state(untrack(() => ({ ...pagination })))
  $effect(() => {
    paginationState = { ...pagination }
  })

  const pageData = $derived(
    paginateData(sortedData, paginationEnabled, serverPagination, paginationState),
  )

  function updatePagination(state) {
    paginationState = { ...state }
    onpaginate?.({ name, ...paginationState })
  }

  let renderComp: any = $state(null)

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
  $effect(() => {
    let useVariant = variant
    if (variant === TableVariant.dynamic) {
      useVariant = $mediaSize <= MediaSize.sm ? TableVariant.div : TableVariant.standard
    }
    setRenderComp(useVariant)
  })

  const renderProps = $derived({
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
  })

  function onFilterClick(e) {
    const colId = e.colId
    console.log('onFilterClick: colId =', colId)
  }

  function onHeaderClick(e) {
    toggleSort(e.colId)
  }

  function onPaginate(e) {
    updatePagination(e.value)
  }

  function onAction(e) {
    const { type, value } = e
    onaction?.({ name, type, value })
  }
</script>

<div class="tui-table" data-test-id={testId}>
  {#if renderComp}
    {@const RenderComp = renderComp}
    <RenderComp
      {...renderProps}
      onfilter={onFilterClick}
      onheader={onHeaderClick}
      onpaginate={onPaginate}
      onaction={onAction} />
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
