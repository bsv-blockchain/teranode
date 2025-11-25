<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import { page } from '$app/stores'
  import TxDetailsCard from './tx-details-card/index.svelte'
  import TxIoCard from './tx-io-card/index.svelte'
  import MerkleProofVisualizer from './merkle-proof-visualizer/index.svelte'

  import NoData from '../no-data-card/index.svelte'
  import { DetailTab, DetailType, setQueryParam } from '$internal/utils/urls'
  import { spinCount } from '$internal/stores/nav'
  import { assetHTTPAddress } from '$internal/stores/nodeStore'
  import { failure } from '$lib/utils/notifications'
  import * as api from '$internal/api'
  import type { MerkleProofData } from '$internal/api'

  let ready = false
  beforeUpdate(() => {
    ready = true
  })

  const type = DetailType.tx

  export let hash = ''

  let display: DetailTab

  $: tab = ready ? $page.url.searchParams.get('tab') ?? '' : ''
  $: display = tab === DetailTab.json ? DetailTab.json : 
              tab === DetailTab.merkleproof ? DetailTab.merkleproof : DetailTab.overview

  let result: any = null
  let merkleProof: MerkleProofData | null = null
  let merkleProofLoading: boolean = false
  let merkleProofError: string | null = null

  $: {
    if ($assetHTTPAddress && type && hash && hash.length === 64) {
      fetchData()
    }
  }

  function onDisplay(e) {
    display = e.detail.value
    setQueryParam('tab', display)
    
    // Fetch merkle proof data when switching to merkle proof tab
    if (display === DetailTab.merkleproof && !merkleProof && 
        (result?.blockHashes?.length > 0 || result?.blockIDs?.length > 0)) {
      fetchMerkleProof()
    }
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
        blockHeights: r2.data.blockHeights,
        blockIDs: r2.data.blockIDs,
        subtreeIdxs: r2.data.subtreeIdxs,
        subtreeHashes: r2.data.subtreeHashes,
        fee: r2.data.fee,
        parentTxHashes: r2.data.parentTxHashes,
        sizeInBytes: r2.data.sizeInBytes,
        isCoinbase: r2.data.isCoinbase,
        lockTime: r2.data.lockTime,
      }
    } else {
      failed = true
      failure(r2.error.message)
    }

    if (!failed) {
      result = tmpData
    }
  }

  async function fetchMerkleProof() {
    if (!hash || merkleProofLoading) return
    
    merkleProofLoading = true
    merkleProofError = null
    
    try {
      const response = await api.getMerkleProof(hash)
      if (response.ok) {
        merkleProof = response.data
      } else {
        merkleProofError = response.error?.message || 'Failed to fetch merkle proof'
        console.error('Failed to fetch merkle proof:', response.error)
      }
    } catch (error) {
      merkleProofError = (error as Error)?.message || 'Unknown error occurred'
      console.error('Error fetching merkle proof:', error)
    } finally {
      merkleProofLoading = false
    }
  }
</script>

{#if result}
  <TxDetailsCard data={result} {display} on:display={onDisplay}>
    <svelte:fragment slot="merkle-proof">
      <MerkleProofVisualizer 
        merkleProof={merkleProof} 
        loading={merkleProofLoading} 
        error={merkleProofError} />
    </svelte:fragment>
  </TxDetailsCard>
  {#if display === DetailTab.overview}
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
