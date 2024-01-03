<script lang="ts">
  import Table from '$lib/components/table/index.svelte'
  import Pager from '$internal/components/pager/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import TableToggle from '$internal/components/table-toggle/index.svelte'
  import i18n from '$internal/i18n'

  import { tableVariant } from '$internal/stores/nav'
  import { addNumCommas } from '$lib/utils/format'
  import { getColDefs, getRenderCells } from './data'

  const baseKey = 'page.viewer'

  $: t = $i18n.t
  $: i18nLocal = { t, baseKey: 'comp.pager' }

  let colDefs: any[] = []
  $: colDefs = getColDefs(t) || []

  $: renderCells = getRenderCells(t) || {}

  export let data: any[] = []
  export let pageSize = 10

  $: hasData = data && data.length > 0

  let page = 1

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

<Card title={t(`${baseKey}.title`)} contentPadding="0" showFooter={showTableFooter}>
  <div slot="subtitle">
    {t(`${baseKey}.subtitle`, {
      fromHeight: hasData ? addNumCommas(data[data.length - 1].height) : 'N/A',
      toHeight: hasData ? addNumCommas(data[0].height) : 'N/A',
    })}
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
    name="blocks"
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
