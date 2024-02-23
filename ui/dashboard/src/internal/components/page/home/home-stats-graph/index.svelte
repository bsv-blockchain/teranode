<script lang="ts">
  import { onDestroy } from 'svelte'
  import { Chart, ChartContainer } from '$lib/components/chart'
  import Card from '$internal/components/card/index.svelte'
  import RangeToggle from '$internal/components/range-toggle/index.svelte'
  import i18n from '$internal/i18n'
  import { getGraphObj } from './graph'

  $: t = $i18n.t

  const baseKey = 'page.home.txs'

  export let data: any = []
  export let period
  export let onChangePeriod

  let renderKey = ''
  let graphObj
  let tmpGraphObj
  let delayId

  function doDelay() {
    if (delayId) {
      clearTimeout(delayId)
    }
    delayId = setTimeout(() => {
      graphObj = tmpGraphObj
    }, 20)
  }

  $: if (data) {
    tmpGraphObj = getGraphObj(t, data, period)
    graphObj = null
    doDelay()
  }

  onDestroy(() => {
    if (delayId) {
      clearTimeout(delayId)
    }
  })
</script>

<Card
  title={t(`${baseKey}.title`)}
  showFooter={true}
  headerPadding="20px 24px 10px 24px"
  wrapHeader={true}
>
  <svelte:fragment slot="header-tools">
    <RangeToggle value={period} on:change={(e) => onChangePeriod(e.detail.value)} />
  </svelte:fragment>

  <ChartContainer bind:renderKey height="530px">
    {#if graphObj?.graphOptions}
      <Chart options={graphObj?.graphOptions} renderKey={renderKey + period} />
    {/if}
  </ChartContainer>
</Card>
