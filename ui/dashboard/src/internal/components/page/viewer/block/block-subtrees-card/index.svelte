<svelte:options runes={true} />

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

  let { block }: { block?: any } = $props()

  let data: any[] = $state([])

  const t = $derived($i18n.t)
  const i18nLocal = $derived({ t, baseKey: 'comp.pager' })

  const colDefs = $derived(getColDefs(t) || [])

  const renderCells = $derived(getRenderCells(t, block?.expandedHeader?.hash) || {})

  let page = $state(1)
  let pageSize = $state(10)
  let totalItems = $state(0)

  function onPage(e) {
    const data = e
    page = data.value.page
    pageSize = data.value.pageSize
  }

  const totalPages = $derived(Math.max(1, Math.ceil(totalItems / pageSize)))
  const showPagerNav = $derived(totalPages > 1)
  const showPagerSize = $derived(showPagerNav || (totalPages === 1 && data.length > 5))
  const showTableFooter = $derived(showPagerSize)

  let variant = $state('dynamic')
  function onToggle(e) {
    const value = e.value
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

  $effect(() => {
    if (block) {
      fetchData(block.expandedHeader.hash, page, pageSize)
    }
  })
</script>

<Card
  title={t(`${baseKey}.title`, { height: block?.expandedHeader?.height })}
  headerPadding="20px 24px 16px 24px"
  contentPadding="0"
  showFooter={showTableFooter}
>
  {#snippet subtitle()}
    <div>
      {#if totalItems > pageSize}
        Viewing subtrees {((page - 1) * pageSize) + 1}-{Math.min(page * pageSize, totalItems)} of {totalItems} subtrees
      {:else if totalItems === 1}
        {t(`${baseKey}.subtitle_singular`, { count: totalItems || 0 })}
      {:else}
        {t(`${baseKey}.subtitle`, { count: totalItems || 0 })}
      {/if}
    </div>
  {/snippet}
  {#snippet headerTools()}
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
      onchange={onPage}
    />
    <TableToggle value={variant} onchange={onToggle} />
  {/snippet}
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
    onaction={() => {}}
  />
  {#snippet footer()}
    <div>
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
        onchange={onPage}
      />
    </div>
  {/snippet}
</Card>
