<svelte:options runes={true} />

<script lang="ts">
  import { page } from '$app/stores'
  import BlockDetailsCard from './block-details-card/index.svelte'
  import BlockCoinbaseCard from './block-coinbase-card/index.svelte'
  import BlockSubtreesCard from './block-subtrees-card/index.svelte'
  import NoData from '../no-data-card/index.svelte'

  import { spinCount } from '$internal/stores/nav'
  import { assetHTTPAddress } from '$internal/stores/nodeStore'
  import { DetailTab, DetailType, setQueryParam } from '$internal/utils/urls'
  import { failure } from '$lib/utils/notifications'
  import * as api from '$internal/api'

  let ready = $state(false)
  $effect(() => {
    ready = true
  })

  const type = DetailType.block

  let { hash = '' }: { hash?: string } = $props()

  let display: DetailTab = $state(DetailTab.overview)

  const tab = $derived(ready ? $page.url.searchParams.get('tab') ?? '' : '')
  $effect(() => {
    display = tab === DetailTab.json ? DetailTab.json : DetailTab.overview
  })

  let result: any = $state(null)

  $effect(() => {
    if ($assetHTTPAddress && type && hash && hash.length === 64) {
      fetchData()
    }
  })

  function onDisplay(e) {
    display = e.value
    setQueryParam('tab', display)
  }

  async function fetchData() {
    let tmpData: any = {}
    let failed = false
    result = null

    // get block data
    const blockResult: any = await api.getItemData({ type: api.ItemType.block, hash: hash })
    if (blockResult.ok) {
      tmpData = blockResult.data
    } else {
      failed = true
      failure(blockResult.error.message)
    }

    // get latest block hash
    const latestBlockData: any = await api.getLastBlocks({ n: 1 })
    if (latestBlockData.ok) {
      tmpData = {
        ...tmpData,
        latestBlockData: latestBlockData.data[0],
      }
    } else {
      failed = true
      failure(latestBlockData.error.message)
    }

    // add extra block header data (needed for block summary display)
    const blockHeaderResult: any = await api.getItemData({
      type: api.ItemType.header,
      hash: hash,
    })
    if (blockHeaderResult.ok) {
      tmpData = {
        ...tmpData,
        expandedHeader: {
          ...blockHeaderResult.data,
        },
      }
    } else {
      failed = true
      failure(blockHeaderResult.error.message)
    }

    if (!failed) {
      result = tmpData
    }
  }
</script>

{#if result}
  <BlockDetailsCard data={result} {display} ondisplay={onDisplay} />
  {#if display === DetailTab.overview}
    <div style="height: 20px"></div>
    <BlockCoinbaseCard data={result} />
    <div style="height: 20px"></div>
    <BlockSubtreesCard block={result} />
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
