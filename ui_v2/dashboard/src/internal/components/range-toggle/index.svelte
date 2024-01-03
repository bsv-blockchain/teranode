<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import Toggle from '$lib/components/toggle/index.svelte'
  import i18n from '../../i18n'

  const baseKey = 'comp.range-toggle'

  $: t = $i18n.t

  const oneDayMillis = 1000 * 60 * 60 * 24

  $: items = t
    ? [
        {
          label: t(`${baseKey}.24h.label`),
          tooltip: t(`${baseKey}.24h.tooltip`),
          value: oneDayMillis,
        },
        {
          label: t(`${baseKey}.1w.label`),
          tooltip: t(`${baseKey}.1w.tooltip`),
          value: 7 * oneDayMillis,
        },
        {
          label: t(`${baseKey}.1m.label`),
          tooltip: t(`${baseKey}.1m.tooltip`),
          value: 30 * oneDayMillis,
        },
        {
          label: t(`${baseKey}.3m.label`),
          tooltip: t(`${baseKey}.3m.tooltip`),
          value: 90 * oneDayMillis,
        },
      ]
    : []

  export let value = oneDayMillis

  const dispatch = createEventDispatcher()

  function onSelect(e) {
    value = e.detail.value
    dispatch('change', e.detail)
  }
</script>

<Toggle name="range-toggle" size="medium" {items} bind:value on:change={onSelect} />
