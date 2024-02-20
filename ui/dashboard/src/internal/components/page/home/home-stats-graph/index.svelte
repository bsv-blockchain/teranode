<script lang="ts">
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

  $: if (data) {
    graphObj = getGraphObj(t, data, period)
  }
</script>

<Card title={t(`${baseKey}.title`)} showFooter={true} headerPadding="20px 24px 10px 24px">
  <svelte:fragment slot="header-tools">
    <RangeToggle value={period} on:change={(e) => onChangePeriod(e.detail.value)} />
  </svelte:fragment>
  {#if graphObj?.graphOptions}
    <ChartContainer bind:renderKey height="530px">
      <Chart options={graphObj?.graphOptions} {renderKey} />
    </ChartContainer>
  {/if}
</Card>
