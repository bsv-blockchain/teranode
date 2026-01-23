<script lang="ts">
  import Table from '$lib/components/table/index.svelte'
  import Pager from '$internal/components/pager/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import TableToggle from '$internal/components/table-toggle/index.svelte'
  import i18n from '$internal/i18n'
  import { tableVariant } from '$internal/stores/nav'
  import { getColDefs, getRenderCells } from './data'
  import { failure } from '$lib/utils/notifications'
  import * as api from '$internal/api'

  const baseKey = 'page.viewer-subtree.txs'

  $: t = $i18n.t
  $: i18nLocal = { t, baseKey: 'comp.pager' }

  let colDefs: any[] = []
  $: colDefs = getColDefs(t) || []

  export let subtree: any
  export let blockHash = ''

  let data: any[] = []
  let coinbaseTxId = ''
  let fetchingCoinbase = false
  let fetchedBlockHashes = new Set()

  $: renderCells = getRenderCells(t, blockHash, coinbaseTxId) || {}

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
    const subtreeTxs: any = await api.getSubtreeTxs({
      hash,
      offset: (page - 1) * pageSize,
      limit: pageSize,
    })
    if (subtreeTxs.ok) {
      data = subtreeTxs.data.data
      const pagination = subtreeTxs.data.pagination
      pageSize = pagination.limit
      page = Math.floor(pagination.offset / pageSize) + 1
      totalItems = pagination.totalRecords
    } else {
      failure(subtreeTxs.error.message)
    }
  }

  $: if (subtree) {
    fetchData(subtree.expandedData.hash, page, pageSize)
  }

  // Reset coinbaseTxId when blockHash changes
  $: if (blockHash) {
    coinbaseTxId = ''
  }

  $: if (
    blockHash &&
    blockHash.length === 64 &&
    !coinbaseTxId &&
    !fetchedBlockHashes.has(blockHash)
  ) {
    fetchCoinbaseId(blockHash)
  }

  async function fetchCoinbaseId(hash) {
    if (fetchingCoinbase || fetchedBlockHashes.has(hash)) return

    fetchingCoinbase = true
    fetchedBlockHashes.add(hash)

    try {
      const blockData = await api.getItemData({ type: api.ItemType.block, hash })
      if (blockData.ok && blockData.data.coinbase_tx) {
        coinbaseTxId = blockData.data.coinbase_tx.TxID || ''
      }
    } catch (error) {
      console.warn('Failed to fetch coinbase transaction ID:', error)
    } finally {
      fetchingCoinbase = false
    }
  }
</script>

<Card
  title={t(`${baseKey}.title`, { height: subtree?.expandedData?.height })}
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
    name="txss"
    {variant}
    idField="index"
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
