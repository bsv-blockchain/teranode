<script lang="ts">
  import { Chart, ChartContainer } from '$lib/components/chart'
  import Card from '$internal/components/card/index.svelte'
  import RangeToggle from '$internal/components/range-toggle/index.svelte'
  import i18n from '$internal/i18n'
  import { getData, oneDayMillis } from './data'
  import { getGraphObj } from './graph'

  const baseKey = 'page.home.txs'

  let renderKey = ''

  $: t = $i18n.t

  let rangeMillis = oneDayMillis

  let now = new Date().getTime()
  let from = now - rangeMillis

  let data = getData(new Date(from).toISOString(), new Date(now).toISOString())

  $: {
    now = new Date().getTime()
    from = now - rangeMillis

    data = getData(new Date(from).toISOString(), new Date(now).toISOString())
  }

  $: graphObj = getGraphObj(t, data)
</script>

<Card title={t(`${baseKey}.title`)} showFooter={true} headerPadding="20px 24px 10px 24px">
  <div slot="header-tools">
    <RangeToggle bind:value={rangeMillis} />
  </div>
  <ChartContainer bind:renderKey height="500px">
    <Chart options={graphObj?.grapOptions} {renderKey} />
  </ChartContainer>
</Card>
