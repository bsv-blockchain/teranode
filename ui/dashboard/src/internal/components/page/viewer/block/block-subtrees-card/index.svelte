<script lang="ts">
  import Table from '$lib/components/table/index.svelte'
  import Pager from '$internal/components/pager/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import TableToggle from '$internal/components/table-toggle/index.svelte'
  import i18n from '$internal/i18n'
  import { tableVariant } from '$internal/stores/nav'
  import { getColDefs, getRenderCells } from './data'
  import * as api from '$internal/api'
  import { failure } from '$lib/utils/notifications'

  const baseKey = 'page.viewer-block.subtrees'

  export let block: any

  let data: any[] = []

  $: t = $i18n.t
  $: i18nLocal = { t, baseKey: 'comp.pager' }

  let colDefs: any[] = []
  $: colDefs = getColDefs(t) || []

  $: renderCells = getRenderCells(t, block?.expandedHeader?.hash) || {}

  let page = 1
  let pageSize = 10
  let totalItems = 0

  function onPage(e) {
    const data = e.detail
    page = data.value.page
    pageSize = data.value.pageSize
  }

  $: totalPages = Math.max(1, Math.ceil(totalItems / pageSize))
  $: showPagerNav = totalPages > 1
  $: showPagerSize = showPagerNav || (totalPages === 1 && data.length > 5)
  $: showTableFooter = showPagerSize

  let variant = 'dynamic'
  function onToggle(e) {
    const value = e.detail.value
    variant = $tableVariant = value
  }

  async function fetchData(hash, page, pageSize) {
    const blockSubtrees: any = await api.getBlockSubtrees({
      hash,
      offset: (page - 1) * pageSize,
      limit: pageSize,
    })
    if (blockSubtrees.ok) {
      data = blockSubtrees.data.data
      const pagination = blockSubtrees.data.pagination
      pageSize = pagination.limit
      page = Math.floor(pagination.offset / pageSize) + 1
      totalItems = pagination.totalRecords
    } else {
      failure(blockSubtrees.error.message)
    }
  }

  $: if (block) {
    fetchData(block.expandedHeader.hash, page, pageSize)
  }
</script>

<Card
  title={t(`${baseKey}.title`, { height: block?.expandedHeader?.height })}
  headerPadding="20px 24px 16px 24px"
  contentPadding="0"
  showFooter={showTableFooter}
>
  <div slot="subtitle">
    {#if totalItems > pageSize}
      Viewing subtrees {((page - 1) * pageSize) + 1}-{Math.min(page * pageSize, totalItems)} of {totalItems} subtrees
    {:else if totalItems === 1}
      {t(`${baseKey}.subtitle_singular`, { count: totalItems || 0 })}
    {:else}
      {t(`${baseKey}.subtitle`, { count: totalItems || 0 })}
    {/if}
  </div>
  <svelte:fragment slot="header-tools">
    <Pager
      i18n={i18nLocal}
      expandUp={true}
      {totalItems}
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
  </svelte:fragment>
  <Table
    name="subtrees"
    {variant}
    idField="hash"
    {colDefs}
    {data}
    pagination={{
      page,
      pageSize,
    }}
    i18n={i18nLocal}
    expandUp={true}
    pager={false}
    useServerPagination={true}
    sortEnabled={false}
    {renderCells}
    getRenderProps={null}
    getRowIconActions={null}
    on:action={() => {}}
  />
  <div slot="footer">
    <Pager
      i18n={i18nLocal}
      expandUp={true}
      {totalItems}
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
