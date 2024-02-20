<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import Toggle from '$lib/components/toggle/index.svelte'
  import i18n from '../../i18n'

  const baseKey = 'comp.range-toggle'

  $: t = $i18n.t

  $: items = t
    ? [
        {
          label: t(`${baseKey}.2h.label`),
          tooltip: t(`${baseKey}.2h.tooltip`),
          value: "2h",
        },
        {
          label: t(`${baseKey}.6h.label`),
          tooltip: t(`${baseKey}.6h.tooltip`),
          value: "6h",
        },
        {
          label: t(`${baseKey}.12h.label`),
          tooltip: t(`${baseKey}.12h.tooltip`),
          value: "12h",
        },
        {
          label: t(`${baseKey}.24h.label`),
          tooltip: t(`${baseKey}.24h.tooltip`),
          value: "24h",
        },
        {
          label: t(`${baseKey}.1w.label`),
          tooltip: t(`${baseKey}.1w.tooltip`),
          value: "1w",
        },
        {
          label: t(`${baseKey}.1m.label`),
          tooltip: t(`${baseKey}.1m.tooltip`),
          value: "1m",
        },
        {
          label: t(`${baseKey}.3m.label`),
          tooltip: t(`${baseKey}.3m.tooltip`),
          value: "3m",
        },
      ]
    : []

  export let value = "24h"

  const dispatch = createEventDispatcher()

  function onSelect(e) {
    value = e.detail.value
    dispatch('change', e.detail)
  }
</script>

<Toggle name="range-toggle" size="medium" {items} bind:value on:change={onSelect} />
