<script lang="ts">
  import Button from '$lib/components/button/index.svelte'
  import Table from '$lib/components/table/index.svelte'
  import Pager from '$internal/components/pager/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import TableToggle from '$internal/components/table-toggle/index.svelte'
  import i18n from '$internal/i18n'

  import { assetHTTPAddress } from '$internal/stores/nodeStore'
  import { failure } from '$lib/utils/notifications'
  import * as api from '$internal/api'
  import { tableVariant } from '$internal/stores/nav'
  import { addNumCommas } from '$lib/utils/format'
  import { getColDefs, getRenderCells } from './data'
  import { getTps } from '$internal/utils/txs'
  import { getHumanReadableTime } from '$internal/utils/format'

  const baseKey = 'page.viewer'

  $: t = $i18n.t
  $: i18nLocal = { t, baseKey: 'comp.pager' }

  let colDefs: any[] = []
  $: colDefs = getColDefs(t) || []

  $: renderCells = getRenderCells(t) || {}

  let data: any[] = []

  $: hasData = data && data.length > 0

  let page = 1
  let pageSize = 20
  let totalItems = 0

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

  async function fetchData(page, pageSize) {
    try {
      if (!$assetHTTPAddress) {
        return
      }

      let b = []

      const result: any = await api.getBlocks({ offset: (page - 1) * pageSize, limit: pageSize })
      if (result.ok) {
        b = result.data.data
        const pagination = result.data.pagination
        pageSize = pagination.limit
        page = Math.floor(pagination.offset / pageSize) + 1
        totalItems = pagination.totalRecords
      } else {
        failure(result.error.message)
      }

      // Calculate delta time which is the time between blocks
      b.forEach((block: any, i: number) => {
        if (i === b.length - 1) {
          return
        }

        const prevBlock: any = b[i + 1]
        const prevBlockTime: any = new Date(prevBlock.timestamp)
        const blockTime: any = new Date(block.timestamp)
        const diff = blockTime - prevBlockTime

        block.tps = getTps(block.transactionCount, diff)

        block.deltaTime = getHumanReadableTime(diff) // The time diff in human readable format
      })

      // Calculate the age of the block
      b.forEach((block: any) => {
        const blockTime: any = new Date(block.timestamp)
        const now: any = new Date()
        const diff = now - blockTime
        block.age = getHumanReadableTime(diff)
      })

      data = b //b.slice(0, numberOfBlocks) // Only show the last 10 blocks
    } catch (err: any) {
      failure(err)
    }
  }

  // Fetch data when the selected node changes
  $: $assetHTTPAddress && fetchData(page, pageSize)

  function onKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    if (e.ctrlKey && keyCode === 'KeyR') {
      fetchData(page, pageSize)
    }
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
      on:total={onTotal}
    />
    <div style="height: 24px; width: 12px;" />
    <TableToggle value={variant} on:change={onToggle} />
    <Button
      size="small"
      ico={true}
      icon="icon-refresh-line"
      tooltip={t('tooltip.refresh')}
      on:click={() => fetchData(page, pageSize)}
    />
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

<svelte:window on:keydown={onKeyDown} />
