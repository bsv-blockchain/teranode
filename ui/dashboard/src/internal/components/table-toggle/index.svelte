<svelte:options runes={true} />

<script lang="ts">
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import Toggle from '$lib/components/toggle/index.svelte'
  import i18n from '$internal/i18n'

  const baseKey = 'comp.table-toggle'

  const t = $derived($i18n.t)

  let {
    value = $bindable(),
    onchange,
  }: {
    value?: any
    onchange?: (e: { name: string; type: string; value: any }) => void
  } = $props()

  const useValue = $derived.by(() => {
    if (value === 'dynamic') {
      return $mediaSize <= MediaSize.sm ? 'div' : 'standard'
    }
    return value
  })

  function onSelect(e: { name: string; type: string; value: any }) {
    value = e.value
    onchange?.(e)
  }
</script>

<Toggle
  name="table-toggle"
  size="small"
  items={[
    { icon: 'Icon-menu-line', value: 'standard', tooltip: t(`${baseKey}.tooltip.standard`) },
    { icon: 'icon-card-line', value: 'div', tooltip: t(`${baseKey}.tooltip.div`) },
  ]}
  value={useValue}
  onchange={onSelect}
/>
