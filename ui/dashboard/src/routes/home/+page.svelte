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
        let txs_per_second = result.data.tx_count / 24 / 60 / 60

        const time_now = Math.round(+Date.now() / 1000)
        const start_time = result.data.first_block_time
        const last_block_time = result.data.last_block_time
        // if startTime is less than 24 hours, calculate txs_per_second based on the time difference
        if (start_time > time_now - 24 * 60 * 60 + 900) {
          // allow 15 minutes of difference for block mining
          txs_per_second = result.data.tx_count / (last_block_time - start_time)
        }

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
            icon: 'icon-cube-line',
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
            icon: 'icon-arrow-transfer-line',
            value: Math.round(txs_per_second),
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
  export let blockGraphData: any = []

  async function getBlockGraphData(period: string) {
    const res: any = await api.getBlockGraphData({ period })
    if (res.ok) {
      blockGraphData = res.data.data_points
    } else {
      failure(res.error.message)
    }
  }

  let period

  $: if (isMounted && period) {
    getBlockGraphData(period)
  }

  function onChangePeriod(value: string) {
    period = value
  }

  let Graph
  onMount(() => {
    // stats
    getStatsData()

    // graph
    isMounted = true
    period = '24h'

    const timeoutId = setTimeout(async () => {
      Graph = (await import('$internal/components/page/home/home-stats-graph/index.svelte')).default
    }, 10)

    return () => clearTimeout(timeoutId)
  })

  function onKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    if (e.ctrlKey && keyCode === 'KeyR') {
      getStatsData()
    }
  }
</script>

<PageWithMenu>
  <div class="content">
    <HomeStatsCard loading={statsLoading} data={statsData} onRefresh={getStatsData} />
    {#if Graph}
      <Graph data={blockGraphData} {period} {onChangePeriod} />
    {/if}
  </div>
</PageWithMenu>

<svelte:window on:keydown={onKeyDown} />

<style>
  .content {
    width: 100%;

    display: flex;
    flex-direction: column;
    gap: 20px;
  }
</style>
