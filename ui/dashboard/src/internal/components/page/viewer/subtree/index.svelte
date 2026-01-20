<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import { page } from '$app/stores'
  import SubtreeDetailsCard from './subtree-details-card/index.svelte'
  import SubtreeTxsCard from './subtree-txs-card/index.svelte'
  import SubtreeMerkleVisualizer from './subtree-merkle-visualizer/index.svelte'

  import NoData from '../no-data-card/index.svelte'
  import { spinCount } from '$internal/stores/nav'
  import { assetHTTPAddress } from '$internal/stores/nodeStore'
  import { DetailTab, DetailType, setQueryParam } from '$internal/utils/urls'
  import { failure } from '$lib/utils/notifications'
  import * as api from '$internal/api'

  let ready = false
  beforeUpdate(() => {
    ready = true
  })

  const type = DetailType.subtree

  export let hash = ''

  $: blockHash = ready ? $page.url.searchParams.get('blockHash') ?? '' : ''

  let display: DetailTab

  $: tab = ready ? $page.url.searchParams.get('tab') ?? '' : ''
  $: display = tab === DetailTab.json ? DetailTab.json :
              tab === DetailTab.merkleproof ? DetailTab.merkleproof :
              DetailTab.overview

  let result: any = null

  // Track previous values to prevent unnecessary fetches
  let lastFetchParams = { assetHTTPAddress: '', hash: '', blockHash: '' }

  $: if ($assetHTTPAddress && type && hash && hash.length === 64) {
    // Only fetch if parameters actually changed
    if (
      lastFetchParams.assetHTTPAddress !== $assetHTTPAddress ||
      lastFetchParams.hash !== hash ||
      lastFetchParams.blockHash !== blockHash
    ) {
      lastFetchParams = { assetHTTPAddress: $assetHTTPAddress, hash, blockHash }
      fetchData()
    }
  }

  function onDisplay(e) {
    display = e.detail.value
    setQueryParam('tab', display)
  }

  async function fetchData() {
    let tmpData: any = {}
    let failed = false
    result = null

    // get subtree data - fetch a reasonable amount for the overview (100 nodes should be enough)
    // The JSON tab will fetch its own paginated data
    const r1: any = await api.getSubtreeNodes({ hash: hash, offset: 0, limit: 100 })
    if (r1.ok) {
      // Extract data from paginated response
      const subtreeData = r1.data.data
      const pagination = r1.data.pagination

      tmpData = {
        ...subtreeData,
        // Store pagination info for reference
        totalNodes: pagination.totalRecords,
      }
    } else {
      failed = true
      failure(r1.error.message)
    }

    // get block data if blockHash is defined
    // if (blockHash) {
    //   const r2: any = await api.getItemData({ type: api.ItemType.block, hash: blockHash })
    //   if (r2.ok) {
    //     tmpData = {
    //       ...tmpData,
    //       expandedBlockData: {
    //         ...r2.data,
    //         hash: blockHash,
    //       },
    //     }
    //   } else {
    //     failed = true
    //     failure(r2.error.message)
    //   }
    // }

    tmpData = {
      ...tmpData,
      expandedData: {
        height: tmpData.Height,
        hash,
        transactionCount: tmpData.totalNodes || (tmpData.Nodes ? tmpData.Nodes.length : 0),
        fee: tmpData.Fees,
        size: tmpData.SizeInBytes,
      },
    }
    if (!failed) {
      result = tmpData
    }
  }
</script>

{#if result}
  <SubtreeDetailsCard data={result} {display} {blockHash} on:display={onDisplay} />
  {#if display === DetailTab.overview}
    <div style="height: 20px" />
    <SubtreeTxsCard subtree={result} {blockHash} />
  {:else if display === DetailTab.merkleproof}
    <div style="height: 20px" />
    <SubtreeMerkleVisualizer subtreeHash={hash} {blockHash} />
  {/if}
{:else if $spinCount === 0}
  <div class="no-data">
    <NoData {hash} />
  </div>
{/if}

<style>
  .no-data {
    padding-top: 80px;
  }
</style>
