<svelte:options runes={true} />

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

  const t = $derived($i18n.t)
  const i18nLocal = $derived({ t, baseKey: 'comp.pager' })

  const colDefs = $derived(getColDefs(t) || [])

  let {
    subtree,
    blockHash = '',
  }: {
    subtree: any
    blockHash?: string
  } = $props()

  let data: any[] = $state([])
  let coinbaseTxId = $state('')
  let fetchingCoinbase = false
  let fetchedBlockHashes = new Set()

  const renderCells = $derived(getRenderCells(t, blockHash, coinbaseTxId) || {})

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

  $effect(() => {
    if (subtree) {
      fetchData(subtree.expandedData.hash, page, pageSize)
    }
  })

  // Reset coinbaseTxId when blockHash changes
  $effect(() => {
    if (blockHash) {
      coinbaseTxId = ''
    }
  })

  $effect(() => {
    if (
      blockHash &&
      blockHash.length === 64 &&
      !coinbaseTxId &&
      !fetchedBlockHashes.has(blockHash)
    ) {
      fetchCoinbaseId(blockHash)
    }
  })

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
  {#snippet subtitle()}
    <div>
      {data?.length === 1
        ? t(`${baseKey}.subtitle_singular`, { count: data?.length || 0 })
        : t(`${baseKey}.subtitle`, { count: data?.length || 0 })}
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
