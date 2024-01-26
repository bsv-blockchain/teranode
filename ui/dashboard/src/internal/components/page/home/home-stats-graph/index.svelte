<script lang="ts">
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
</script>

<Card title={t(`${baseKey}.title`)} showFooter={true} headerPadding="20px 24px 10px 24px">
  <svelte:fragment slot="header-tools">
    <RangeToggle bind:value={rangeMillis} />
  </svelte:fragment>
  {#if graphObj?.graphOptions}
    <ChartContainer bind:renderKey height="530px">
      <Chart options={graphObj?.graphOptions} {renderKey} />
    </ChartContainer>
  {/if}
</Card>
