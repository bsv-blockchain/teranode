<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import { page } from '$app/stores'
  import SubtreeDetailsCard from './subtree-details-card/index.svelte'
  import SubtreeTxsCard from './subtree-txs-card/index.svelte'

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
  $: display = tab === DetailTab.json ? DetailTab.json : DetailTab.overview

  let result: any = null

  $: {
    if ($assetHTTPAddress && type && hash && hash.length === 64) {
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

    // get subtree data
    const r1: any = await api.getItemData({ type: api.ItemType.subtree, hash: hash })
    if (r1.ok) {
      tmpData = r1.data
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
        transactionCount: tmpData.Nodes.length,
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
    <SubtreeTxsCard subtree={result} />
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
