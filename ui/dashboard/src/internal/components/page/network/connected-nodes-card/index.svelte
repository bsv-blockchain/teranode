<script lang="ts">
  import Table from '$lib/components/table/index.svelte'
  import Pager from '$internal/components/pager/index.svelte'
  import Icon from '$lib/components/icon/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import TableToggle from '$internal/components/table-toggle/index.svelte'
  import i18n from '$internal/i18n'
  import { tableVariant } from '$internal/stores/nav'
  import { getColDefs, renderCells } from './data'

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

  let variant = $tableVariant
  function onToggle(e) {
    const value = e.detail.value
    variant = $tableVariant = value
  }
</script>

<Card contentPadding="0" showFooter={showTableFooter}>
  <div class="title" slot="title">
    <Icon name="icon-status-light-glow-solid" size={14} color={connected ? '#15B241' : '#CE1722'} />
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
  </svelte:fragment>
  <Table
    name="nodes"
    {variant}
    idField="height"
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

<style>
  .title {
    display: flex;
    align-items: center;
    gap: 8px;
  }
</style>
