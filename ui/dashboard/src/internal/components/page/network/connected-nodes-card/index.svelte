<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import Table from '$lib/components/table/index.svelte'
  import Pager from '$internal/components/pager/index.svelte'
  import Icon from '$lib/components/icon/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import TableToggle from '$internal/components/table-toggle/index.svelte'
  import BlockAssemblyModal from '$internal/components/block-assembly-modal/index.svelte'
  import i18n from '$internal/i18n'
  import { tableVariant } from '$internal/stores/nav'
  import { getColDefs, renderCells, getRenderProps } from './data'

  const pageKey = 'page.network.nodes'
  const dispatch = createEventDispatcher()

  $: t = $i18n.t
  $: i18nLocal = { t, baseKey: 'comp.pager' }

  let colDefs: any[] = []
  $: colDefs = getColDefs(t) || []

  export let data: any[] = [] // Paginated data
  export let allData: any[] = [] // Full dataset for pagination calculation
  export let connected = false
  export let page = 1
  export let pageSize = 10
  export let sortColumn = ''
  export let sortOrder = ''

  function onPage(e) {
    // Forward pagination changes to parent component
    dispatch('pagechange', e.detail)
  }

  function onSort(e) {
    // Forward sort changes to parent component
    dispatch('sort', e.detail)
  }

  function clearSort() {
    // Dispatch a sort event with empty values to clear sorting
    dispatch('sort', {
      colId: '',
      value: ''
    })
  }

  $: hasSorting = sortColumn && sortOrder

  $: totalPages = Math.max(1, Math.ceil((allData?.length || 0) / pageSize))
  $: showPagerNav = totalPages > 1
  $: showPagerSize = showPagerNav || (totalPages === 1 && allData.length > 5)
  $: showTableFooter = showPagerSize

  let variant = 'dynamic'
  function onToggle(e) {
    const value = e.detail.value
    variant = $tableVariant = value
  }
</script>

<Card contentPadding="0" showFooter={showTableFooter}>
  <div class="title" slot="title">
    <Typo variant="title" size="h4" value={t(`${pageKey}.title`)} />
  </div>
  <svelte:fragment slot="header-tools">
    <Pager
      i18n={i18nLocal}
      expandUp={true}
      totalItems={allData?.length}
      showPageSize={false}
      showQuickNav={false}
      showNav={showPagerNav}
      value={{
        page,
        pageSize,
      }}
      hasBoundaryRight={true}
      on:change={onPage}
    />
    <TableToggle value={variant} on:change={onToggle} />
    {#if hasSorting}
      <button class="clear-sort-btn" on:click={clearSort} title="Clear sorting">
        <Icon name="icon-close-line" size={16} />
      </button>
    {/if}
    <div class="live">
      <div class="live-icon" class:connected>
        <Icon name="icon-status-light-glow-solid" size={14} />
      </div>
      <div class="live-label">{t(`page.network.live`)}</div>
    </div>
  </svelte:fragment>
  <Table
    name="nodes"
    {variant}
    idField="peer_id"
    {colDefs}
    {data}
    sort={{
      sortColumn,
      sortOrder,
    }}
    sortEnabled={true}
    pagination={{
      page: 1,
      pageSize: -1,
    }}
    paginationEnabled={false}
    i18n={{ t, baseKey: 'comp.pager' }}
    pager={false}
    expandUp={true}
    {renderCells}
    {getRenderProps}
    getRowIconActions={null}
    on:action={() => {}}
    on:sort={onSort}
  />
  <div slot="footer">
    <Pager
      i18n={i18nLocal}
      expandUp={true}
      totalItems={allData?.length}
      showPageSize={showPagerSize}
      showQuickNav={showPagerNav}
      showNav={showPagerNav}
      value={{
        page,
        pageSize,
      }}
      hasBoundaryRight={true}
      on:change={onPage}
    />
  </div>
</Card>

<BlockAssemblyModal />

<style>
  .clear-sort-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    padding: 0;
    background: transparent;
    border: none;
    color: rgba(255, 255, 255, 0.66);
    cursor: pointer;
    transition: all 0.2s ease;
    border-radius: 4px;
  }

  .clear-sort-btn:hover {
    background: rgba(255, 255, 255, 0.1);
    color: rgba(255, 255, 255, 0.9);
  }

  .clear-sort-btn:active {
    background: rgba(255, 255, 255, 0.15);
  }

  .live {
    display: flex;
    align-items: center;
    gap: 4px;

    color: rgba(255, 255, 255, 0.66);

    font-family: Satoshi;
    font-size: 13px;
    font-style: normal;
    font-weight: 700;
    line-height: 18px;
    letter-spacing: 0.26px;

    text-transform: uppercase;
  }
  .live-icon {
    color: #ce1722;
  }
  .live-icon.connected {
    color: #15b241;
  }
  .live-label {
    color: rgba(255, 255, 255, 0.66);
  }

  .title {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  
  /* Highlight the current node name */
  :global(.current-node-name) {
    color: #4a9eff !important;
    font-weight: bold;
  }
  
  /* Column header alignments */
  /* State column (1st) - center align */
  :global(th:nth-child(1)),
  :global(.th:nth-child(1)) {
    text-align: center !important;
  }
  
  :global(th:nth-child(1) .table-cell-row),
  :global(.th:nth-child(1) .table-cell-row) {
    justify-content: center !important;
  }
  
  /* Version (3rd column now) - explicitly left align */
  :global(th:nth-child(3)),
  :global(.th:nth-child(3)) {
    text-align: left !important;
  }
  
  :global(th:nth-child(3) .table-cell-row),
  :global(.th:nth-child(3) .table-cell-row) {
    text-align: left !important;
    justify-content: flex-start !important;
  }
  
  :global(th:nth-child(4)), /* Height - right align */
  :global(.th:nth-child(4)),
  :global(th:nth-child(6)), /* Chain Rank - right align */
  :global(.th:nth-child(6)),
  :global(th:nth-child(7)), /* TX Assembly - right align */
  :global(.th:nth-child(7)),
  :global(th:nth-child(8)), /* Min Mining Fee - right align */
  :global(.th:nth-child(8)),
  :global(th:nth-child(9)), /* Connected Peers - right align */
  :global(.th:nth-child(9)),
  :global(th:nth-child(10)), /* Uptime - right align */
  :global(.th:nth-child(10)),
  :global(th:nth-child(13)), /* Last Update - right align */
  :global(.th:nth-child(13)) {
    text-align: right !important;
  }

  :global(th:nth-child(4) .table-cell-row),
  :global(.th:nth-child(4) .table-cell-row),
  :global(th:nth-child(6) .table-cell-row),
  :global(.th:nth-child(6) .table-cell-row),
  :global(th:nth-child(7) .table-cell-row),
  :global(.th:nth-child(7) .table-cell-row),
  :global(th:nth-child(8) .table-cell-row),
  :global(.th:nth-child(8) .table-cell-row),
  :global(th:nth-child(9) .table-cell-row),
  :global(.th:nth-child(9) .table-cell-row),
  :global(th:nth-child(10) .table-cell-row),
  :global(.th:nth-child(10) .table-cell-row),
  :global(th:nth-child(13) .table-cell-row),
  :global(.th:nth-child(13) .table-cell-row) {
    justify-content: flex-end !important;
  }

  /* Prevent Chain Rank header from wrapping */
  :global(th:nth-child(6)),
  :global(.th:nth-child(6)) {
    white-space: nowrap !important;
  }
  
  /* Right-align numeric values */
  :global(.num) {
    text-align: right !important;
    display: block !important;
    width: 100% !important;
  }
  
  :global(.chainwork-score-top) {
    color: #15b241 !important;
    font-weight: bold;
    font-size: 16px;
  }

  :global(.chainwork-score-other) {
    color: #ffd700 !important;
    font-weight: bold;
    font-size: 16px;
  }

  /* Style for clickable TX Assembly */
  :global(.clickable-span.num) {
    text-align: right !important;
    display: block !important;
    width: 100% !important;
  }
</style>
