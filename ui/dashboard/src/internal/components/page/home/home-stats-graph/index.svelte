<script lang="ts">
  import { onMount } from 'svelte'
  import { Chart, ChartContainer } from '$lib/components/chart'
  import Card from '$internal/components/card/index.svelte'
  import RangeToggle from '$internal/components/range-toggle/index.svelte'
  import i18n from '$internal/i18n'
  import { getGraphObj } from './graph'
  import * as api from '$internal/api'
  import { failure } from '$lib/utils/notifications'

  const oneDayMillis = 1000 * 60 * 60 * 24

  let data: any = []

  async function getData(periodMillis: number) {
    const res: any = await api.getBlockGraphData({ periodMillis })
    if (res.ok) {
      data = res.data.data_points
      // console.log('SAO DATA', data)
    } else {
      // console.error(res.error)
      failure(res.error.message)
    }
  }

  const baseKey = 'page.home.txs'

  let renderKey = ''

  $: t = $i18n.t

  let rangeMillis
  let graphObj

  $: rangeMillis = oneDayMillis

  $: if (rangeMillis) {
    let from = new Date().getTime() - rangeMillis

    getData(from)
  }

  $: if (data) {
    graphObj = getGraphObj(t, data)
  }

  onMount(() => {
    let from = new Date().getTime() - rangeMillis

    getData(from)
  })
</script>

<Card title={t(`${baseKey}.title`)} showFooter={true} headerPadding="20px 24px 10px 24px">
  <div slot="header-tools">
    <RangeToggle bind:value={rangeMillis} />
  </div>
  <ChartContainer bind:renderKey height="500px">
    <pre>{JSON.stringify(graphObj?.graphOptions)}</pre>
    <!-- <Chart options={graphObj?.graphOptions} {renderKey} /> -->
  </ChartContainer>
</Card>
