<script lang="ts">
  import BlocksTableCard from '$internal/components/page/viewer/blocks/blocks-table-card/index.svelte'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import { getHumanReadableTime } from '$internal/utils/humanTime'
  import { assetHTTPAddress } from '$internal/stores/nodeStore'
  import { success, failure } from '$lib/utils/notifications'
  import * as api from '$internal/api'

  let blocks: any[] = []
  let error = ''
  let url = ''

  const numberOfBlocks = 100

  // Fetch data when the selected node changes
  $: $assetHTTPAddress && fetchData()

  const getTps = (transactionCount: number, diff: number) => {
    const timeDiff = diff / 1000 // The time diff between blocks (in seconds)

    if (timeDiff === 0) {
      return ''
    } else {
      const tps = transactionCount / timeDiff

      if (tps < 1) {
        return '<1'
      } else {
        return tps.toLocaleString(undefined, {
          maximumFractionDigits: 0,
          minimumFractionDigits: 0,
        })
      }
    }
  }

  async function fetchData() {
    try {
      if (!$assetHTTPAddress) {
        return
      }

      error = ''

      let b = []

      const result: any = await api.getLastBlocks({ n: numberOfBlocks + 1 }) // Get 1 more block than we need to calculate the delta time
      if (result.ok) {
        b = result.data
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

      blocks = b.slice(0, numberOfBlocks) // Only show the last 10 blocks
    } catch (err: any) {
      error = err.message
      console.error(err)
    }
  }

  function onKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    switch (keyCode) {
      case 'KeyR':
        fetchData()
      default:
    }
  }
</script>

<PageWithMenu>
  <BlocksTableCard data={blocks} pageSize={20} refresh={fetchData} />
</PageWithMenu>

<svelte:window on:keydown|preventDefault={onKeyDown} />

<style>
</style>
