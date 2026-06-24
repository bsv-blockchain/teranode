<svelte:options runes={true} />

<script lang="ts">
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

  let {
    data = [], // Paginated data
    allData = [], // Full dataset for pagination calculation
    connected = false,
    page = 1,
    pageSize = 10,
    sortColumn = '',
    sortOrder = '',
    onpagechange,
    onsort,
  }: {
    data?: any[]
    allData?: any[]
    connected?: boolean
    page?: number
    pageSize?: number
    sortColumn?: string
    sortOrder?: string
    onpagechange?: (e: any) => void
    onsort?: (e: any) => void
  } = $props()

  const t = $derived($i18n.t)
  const i18nLocal = $derived({ t, baseKey: 'comp.pager' })

  const colDefs = $derived(getColDefs(t) || [])

  function onPage(e) {
    // Forward pagination changes to parent component
    onpagechange?.(e)
  }

  function onSort(e) {
    // Forward sort changes to parent component
    onsort?.(e)
  }

  function clearSort() {
    // Dispatch a sort event with empty values to clear sorting
    onsort?.({
      colId: '',
      value: ''
    })
  }

  const hasSorting = $derived(sortColumn && sortOrder)

  const totalPages = $derived(Math.max(1, Math.ceil((allData?.length || 0) / pageSize)))
  const showPagerNav = $derived(totalPages > 1)
  const showPagerSize = $derived(showPagerNav || (totalPages === 1 && allData.length > 5))
  const showTableFooter = $derived(showPagerSize)

  let variant = $state('dynamic')
  function onToggle(e) {
    const value = e.value
    variant = $tableVariant = value
  }
</script>

<Card contentPadding="0" showFooter={showTableFooter}>
  {#snippet title()}
    <div class="title">
      <Typo variant="title" size="h4" value={t(`${pageKey}.title`)} />
    </div>
  {/snippet}
  {#snippet headerTools()}
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
      onchange={onPage}
    />
    <TableToggle value={variant} onchange={onToggle} />
    {#if hasSorting}
      <button class="clear-sort-btn" onclick={clearSort} title="Clear sorting">
        <Icon name="icon-close-line" size={16} />
      </button>
    {/if}
    <div class="live">
      <div class="live-icon" class:connected>
        <Icon name="icon-status-light-glow-solid" size={14} />
      </div>
      <div class="live-label">{t(`page.network.live`)}</div>
    </div>
  {/snippet}
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
    onaction={() => {}}
    onsort={onSort}
  />
  {#snippet footer()}
    <div>
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
        onchange={onPage}
      />
    </div>
  {/snippet}
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
    color: var(--comp-label-color);
    cursor: pointer;
    transition: all 0.2s ease;
    border-radius: 4px;
  }

  .clear-sort-btn:hover {
    background: var(--app-overlay-color);
    color: var(--app-color);
  }

  .clear-sort-btn:active {
    background: var(--app-overlay-strong-color);
  }

  .live {
    display: flex;
    align-items: center;
    gap: 4px;

    color: var(--comp-label-color);

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
    color: var(--comp-label-color);
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
