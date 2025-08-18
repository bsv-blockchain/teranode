<script lang="ts">
  import Table from '$lib/components/table/index.svelte'
  import Pager from '$internal/components/pager/index.svelte'
  import Icon from '$lib/components/icon/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import TableToggle from '$internal/components/table-toggle/index.svelte'
  import i18n from '$internal/i18n'
  import { tableVariant } from '$internal/stores/nav'
  import { getColDefs, renderCells, getRenderProps } from './data'

  const pageKey = 'page.network.nodes'

  $: t = $i18n.t
  $: i18nLocal = { t, baseKey: 'comp.pager' }

  let colDefs: any[] = []
  $: colDefs = getColDefs(t) || []

  export let data: any[] = []
  export let connected = false

  let page = 1
  let pageSize = 10

  function onPage(e) {
    const data = e.detail
    page = data.value.page
    pageSize = data.value.pageSize
  }

  let totalPages = 0

  const onTotal = (e) => {
    totalPages = e.detail.total
  }

  $: showPagerNav = totalPages > 1
  $: showPagerSize = showPagerNav || (totalPages === 1 && data.length > 5)
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
      totalItems={data?.length}
      showPageSize={false}
      showQuickNav={false}
      showNav={showPagerNav}
      value={{
        page,
        pageSize,
      }}
      hasBoundaryRight={true}
      on:change={onPage}
      on:total={onTotal}
    />
    <TableToggle value={variant} on:change={onToggle} />
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
    pagination={{
      page: 1,
      pageSize: -1,
    }}
    i18n={{ t, baseKey: 'comp.pager' }}
    pager={false}
    expandUp={true}
    {renderCells}
    {getRenderProps}
    getRowIconActions={null}
    on:action={() => {}}
  />
  <div slot="footer">
    <Pager
      i18n={i18nLocal}
      expandUp={true}
      totalItems={data?.length}
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

<style>
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
  /* Version - explicitly left align */
  :global(th:nth-child(2)),
  :global(.th:nth-child(2)) {
    text-align: left !important;
  }
  
  :global(th:nth-child(2) .table-cell-row),
  :global(.th:nth-child(2) .table-cell-row) {
    text-align: left !important;
    justify-content: flex-start !important;
  }
  
  :global(th:nth-child(4)), /* Height - right align */
  :global(.th:nth-child(4)),
  :global(th:nth-child(6)), /* Chainwork - right align */
  :global(.th:nth-child(6)),
  :global(th:nth-child(7)), /* TX Assembly - right align */
  :global(.th:nth-child(7)),
  :global(th:nth-child(8)), /* Uptime - right align */
  :global(.th:nth-child(8)),
  :global(th:nth-child(10)), /* Last Update - right align */
  :global(.th:nth-child(10)) {
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
  :global(th:nth-child(10) .table-cell-row),
  :global(.th:nth-child(10) .table-cell-row) {
    justify-content: flex-end !important;
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
</style>
