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
  let delayedTs = 0
  let refreshTimeoutId

  function updateDelayTs() {
    if (refreshTimeoutId) {
      clearTimeout(refreshTimeoutId)
    }
    refreshTimeoutId = setTimeout(() => {
      delayedTs = new Date().getTime()
    }, 50)
  }

  $: if (data) {
    graphObj = getGraphObj(t, data, period)
    updateDelayTs()
  }

  onDestroy(() => {
    if (refreshTimeoutId) {
      clearTimeout(refreshTimeoutId)
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
  {#if graphObj?.graphOptions}
    <ChartContainer bind:renderKey height="530px">
      <Chart options={graphObj?.graphOptions} renderKey={renderKey + period + delayedTs} />
    </ChartContainer>
  {/if}
</Card>
