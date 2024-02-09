<script lang="ts">
  import { onMount } from 'svelte'
  import HomeStatsCard from '$internal/components/page/home/home-stats-card/index.svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import * as api from '$internal/api'
  import { failure } from '$lib/utils/notifications'

  let isMounted = false

  // stats
  let statsLoading = true
  let statsData = {}

  async function getStatsData() {
    try {
      const result: any = await api.getBlockStats()
      if (result.ok) {
        statsData = {
          block_count: {
            id: 'block_count',
            icon: 'icon-cube-line',
            value: result.data.block_count,
          },
          tx_count: {
            id: 'tx_count',
            icon: 'icon-arrow-transfer-line',
            value: result.data.tx_count,
          },
          max_height: {
            id: 'max_height',
            icon: 'icon-network-line',
            value: result.data.max_height,
          },
          avg_block_size: {
            id: 'avg_block_size',
            icon: 'icon-scale-line',
            value: Math.round(result.data.avg_block_size),
          },
          avg_tx_count_per_block: {
            id: 'avg_tx_count_per_block',
            icon: 'icon-scale-line',
            value: Math.round(result.data.avg_tx_count_per_block),
          },
          txns_per_second: {
            id: 'txns_per_second',
            icon: 'icon-binoculars-line',
            value: Math.round(result.data.tx_count / 24 / 60 / 60),
          },
        }
      } else {
        failure(result.error.message)
      }
    } catch (err: any) {
      console.error(err)
      failure(err.message)
    } finally {
      statsLoading = false
    }

    return false
  }

  // graph
  const oneDayMillis = 1000 * 60 * 60 * 24

  export let blockGraphData: any = []

  async function getBlockGraphData(periodMillis: number) {
    const res: any = await api.getBlockGraphData({ periodMillis })
    if (res.ok) {
      blockGraphData = res.data.data_points
    } else {
      failure(res.error.message)
    }
  }

  let rangeMillis

  $: if (isMounted && rangeMillis) {
    let from = new Date().getTime() - rangeMillis

    getBlockGraphData(from)
  }

  function onRangeMillis(value: number) {
    rangeMillis = value
  }

  let Graph
  onMount(() => {
    // stats
    getStatsData()

    // graph
    isMounted = true
    rangeMillis = oneDayMillis

    const timeoutId = setTimeout(async () => {
      Graph = (await import('$internal/components/page/home/home-stats-graph/index.svelte')).default
    }, 10)

    return () => clearTimeout(timeoutId)
  })

  function onKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    switch (keyCode) {
      case 'KeyR':
        getStatsData()
      default:
    }
  }
</script>

<PageWithMenu>
  <div class="content">
    <HomeStatsCard loading={statsLoading} data={statsData} onRefresh={getStatsData} />
    {#if Graph}
      <Graph data={blockGraphData} {rangeMillis} {onRangeMillis} />
    {/if}
  </div>
</PageWithMenu>

<svelte:window on:keydown|preventDefault={onKeyDown} />

<style>
  .content {
    width: 100%;

    display: flex;
    flex-direction: column;
    gap: 20px;
  }
</style>
