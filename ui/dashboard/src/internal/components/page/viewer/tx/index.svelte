<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import { page } from '$app/stores'
  import TxDetailsCard from './tx-details-card/index.svelte'
  import TxIoCard from './tx-io-card/index.svelte'

  import NoData from '../no-data-card/index.svelte'
  import { DetailTab, setQueryParam } from '$internal/utils/urls'
  import { spinCount } from '$internal/stores/nav'
  import { assetHTTPAddress } from '$internal/stores/nodeStore'
  import { failure } from '$lib/utils/notifications'
  import * as api from '$internal/api'

  let ready = false
  beforeUpdate(() => {
    ready = true
  })

  const type = 'tx'

  export let hash = ''

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

    // get block data
    const r1: any = await api.getItemData({ type: api.ItemType.tx, hash: hash })
    if (r1.ok) {
      tmpData = r1.data
      // console.log('r1 data = ', r1.data)
    } else {
      failed = true
      failure(r1.error.message)
    }

    const r2: any = await api.getItemData({ type: api.ItemType.txmeta, hash: hash })
    if (r2.ok) {
      // console.log('r2 data = ', r2.data)
      tmpData = {
        ...tmpData,
        blockHashes: r2.data.blockHashes,
        fee: r2.data.fee,
        parentTxHashes: r2.data.parentTxHashes,
        sizeInBytes: r2.data.sizeInBytes,
      }
    } else {
      failed = true
      failure(r2.error.message)
    }

    if (!failed) {
      result = tmpData
    }
  }
</script>

{#if result}
  <TxDetailsCard data={result} {display} on:display={onDisplay} />
  {#if display === 'overview'}
    <div style="height: 20px" />
    <TxIoCard data={result} />
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
