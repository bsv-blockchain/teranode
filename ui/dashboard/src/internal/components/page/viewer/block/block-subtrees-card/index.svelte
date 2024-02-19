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
  export let data: any[] = []

  $: t = $i18n.t
  $: i18nLocal = { t, baseKey: 'comp.pager' }

  let colDefs: any[] = []
  $: colDefs = getColDefs(t) || []

  $: renderCells = getRenderCells(t, block?.expandedHeader?.hash) || {}

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

  let variant = $tableVariant
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
  contentPadding="0"
  showFooter={showTableFooter}
>
  <div slot="subtitle">
    {data?.length === 1
      ? t(`${baseKey}.subtitle_singular`, { count: data?.length || 0 })
      : t(`${baseKey}.subtitle`, { count: data?.length || 0 })}
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
  </svelte:fragment>
  <Table
    name="subtrees"
    {variant}
    idField="height"
    {colDefs}
    {data}
    pagination={{
      page,
      pageSize,
    }}
    i18n={i18nLocal}
    expandUp={true}
    pager={false}
    {renderCells}
    getRenderProps={null}
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
