<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import { page } from '$app/stores'
  import SubtreeDetailsCard from './subtree-details-card/index.svelte'
  import SubtreeTxsCard from './subtree-txs-card/index.svelte'

  import NoData from '../no-data-card/index.svelte'
  import { spinCount } from '$internal/stores/nav'
  import { assetHTTPAddress } from '$internal/stores/nodeStore'
  import { DetailTab, setQueryParam } from '$internal/utils/urls'
  import { failure } from '$lib/utils/notifications'
  import * as api from '$internal/api'

  let ready = false
  beforeUpdate(() => {
    ready = true
  })

  const type = 'subtree'

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
    if (blockHash) {
      const r2: any = await api.getItemData({ type: api.ItemType.block, hash: blockHash })
      if (r2.ok) {
        tmpData = {
          ...tmpData,
          expandedBlockData: {
            ...r2.data,
            hash: blockHash,
          },
        }
      } else {
        failed = true
        failure(r2.error.message)
      }
    }

    // expand subtree data
    let expandedTransactionData: any[] = []
    if (tmpData && tmpData.Nodes && tmpData.Nodes.length > 0) {
      expandedTransactionData = await Promise.all(
        tmpData.Nodes.map(async (node, i) => {
          if (
            i === 0 &&
            tmpData.Nodes[0].txid ===
              'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'
          ) {
            if (tmpData.expandedBlockData) {
              const coinbase_tx = tmpData.expandedBlockData.coinbase_tx
              return {
                index: i,
                inputsCount: coinbase_tx.inputs.length,
                kbFee: tmpData.Nodes[i].fee,
                outputsCount: coinbase_tx.outputs.length,
                size: tmpData.Nodes[i].size,
                timestamp: 0, // TBD
                txid: coinbase_tx.txid,
              }
            } else {
              return {
                index: i,
                inputsCount: 0,
                kbFee: 0,
                outputsCount: 0,
                size: 0,
                timestamp: 0,
                txid: 'COINBASE',
              }
            }
          } else {
            const txResult: any = await api.getItemData({ type: api.ItemType.tx, hash: node.txid })
            if (txResult.ok) {
              return {
                index: i,
                inputsCount: txResult.data.inputs.length,
                kbFee: tmpData.Nodes[i].fee,
                outputsCount: txResult.data.outputs.length,
                size: tmpData.Nodes[i].size,
                timestamp: 0, // TBD
                txid: txResult.data.txid,
              }
            } else {
              return null
            }
          }
        }),
      )
    }

    expandedTransactionData = expandedTransactionData.filter((item) => item !== null)

    tmpData = {
      ...tmpData,
      expandedData: {
        height: tmpData.Height,
        hash,
        transactionCount: tmpData.Nodes.length,
        fee: tmpData.Fees,
        size: tmpData.SizeInBytes,
      },
      expandedTransactionData,
    }
    if (!failed) {
      result = tmpData
    }
  }
</script>

{#if result}
  <SubtreeDetailsCard data={result} {display} on:display={onDisplay} />
  {#if display === 'overview'}
    <div style="height: 20px" />
    <SubtreeTxsCard subtree={result} data={result.expandedTransactionData} />
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
