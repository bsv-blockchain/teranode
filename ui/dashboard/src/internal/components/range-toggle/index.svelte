<svelte:options runes={true} />

<script lang="ts">
  import Toggle from '$lib/components/toggle/index.svelte'
  import i18n from '../../i18n'

  const baseKey = 'comp.range-toggle'

  let {
    value = $bindable('24h'),
    onchange,
  }: {
    value?: string
    onchange?: (e: { name: string; type: string; value: any }) => void
  } = $props()

  let t = $derived($i18n.t)

  let items = $derived(
    t
      ? [
          {
            label: t(`${baseKey}.2h.label`),
            tooltip: t(`${baseKey}.2h.tooltip`),
            value: '2h',
          },
          {
            label: t(`${baseKey}.6h.label`),
            tooltip: t(`${baseKey}.6h.tooltip`),
            value: '6h',
          },
          {
            label: t(`${baseKey}.12h.label`),
            tooltip: t(`${baseKey}.12h.tooltip`),
            value: '12h',
          },
          {
            label: t(`${baseKey}.24h.label`),
            tooltip: t(`${baseKey}.24h.tooltip`),
            value: '24h',
          },
          {
            label: t(`${baseKey}.1w.label`),
            tooltip: t(`${baseKey}.1w.tooltip`),
            value: '1w',
          },
          {
            label: t(`${baseKey}.1m.label`),
            tooltip: t(`${baseKey}.1m.tooltip`),
            value: '1m',
          },
          {
            label: t(`${baseKey}.3m.label`),
            tooltip: t(`${baseKey}.3m.tooltip`),
            value: '3m',
          },
          {
            label: t(`${baseKey}.all.label`),
            tooltip: t(`${baseKey}.all.tooltip`),
            value: 'all',
          },
        ]
      : [],
  )

  function onSelect(e: { name: string; type: string; value: any }) {
    value = e.value
    onchange?.(e)
  }
</script>

<Toggle name="range-toggle" size="medium" {items} bind:value onchange={onSelect} />
