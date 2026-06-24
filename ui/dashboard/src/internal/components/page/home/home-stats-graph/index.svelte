<svelte:options runes={true} />

<script lang="ts">
  import { onDestroy } from 'svelte'
  import { Chart, ChartContainer } from '$lib/components/chart'
  import Card from '$internal/components/card/index.svelte'
  import RangeToggle from '$internal/components/range-toggle/index.svelte'
  import i18n from '$internal/i18n'
  import { getGraphObj } from './graph'

  const t = $derived($i18n.t)

  const baseKey = 'page.home.txs'

  let {
    data = [],
    period,
    onChangePeriod,
  }: {
    data?: any
    period?: string
    onChangePeriod?: (value: string) => void
  } = $props()

  let renderKey = $state('')
  let graphObj = $state<any>(null)
  let tmpGraphObj: any
  let delayId

  function doDelay() {
    if (delayId) {
      clearTimeout(delayId)
    }
    delayId = setTimeout(() => {
      graphObj = tmpGraphObj
    }, 20)
  }

  $effect(() => {
    if (data) {
      tmpGraphObj = getGraphObj(t, data, period)
      graphObj = null
      doDelay()
    }
  })

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
  {#snippet headerTools()}
    <RangeToggle value={period} onchange={(e) => onChangePeriod?.(e.value)} />
  {/snippet}

  <ChartContainer bind:renderKey height="530px">
    {#if graphObj?.graphOptions}
      <Chart options={graphObj?.graphOptions} renderKey={renderKey + period} />
    {/if}
  </ChartContainer>
</Card>
