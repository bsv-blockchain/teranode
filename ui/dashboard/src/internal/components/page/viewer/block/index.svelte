<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import { page } from '$app/stores'
  import BlockDetailsCard from './block-details-card/index.svelte'
  import BlockSubtreesCard from './block-subtrees-card/index.svelte'
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

  const type = 'block'

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
    const blockResult: any = await api.getItemData({ type: api.ItemType.block, hash: hash })
    if (blockResult.ok) {
      tmpData = blockResult.data
    } else {
      failed = true
      failure(blockResult.error.message)
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

    // expand subtree data (needed for subtree table display)
    let expandedSubtreeData: any[] = []
    if (tmpData && tmpData.subtrees && tmpData.subtrees.length > 0) {
      expandedSubtreeData = await Promise.all(
        tmpData.subtrees.map(async (hash) => {
          const subtreeResult: any = await api.getItemData({ type: api.ItemType.subtree, hash })
          if (subtreeResult.ok) {
            return {
              height: subtreeResult.data.Height,
              hash,
              transactionCount: subtreeResult.data.Nodes.length,
              fee: subtreeResult.data.Fees,
              size: subtreeResult.data.SizeInBytes,
            }
          } else {
            return null
          }
        }),
      )
    }
    expandedSubtreeData = expandedSubtreeData.filter((item) => item !== null)

    tmpData = {
      ...tmpData,
      expandedSubtreeData,
    }

    if (!failed) {
      result = tmpData
    }
  }
</script>

{#if result}
  <BlockDetailsCard data={result} {display} on:display={onDisplay} />
  {#if display === 'overview'}
    <div style="height: 20px" />
    <BlockSubtreesCard block={result} data={result.expandedSubtreeData} />
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
