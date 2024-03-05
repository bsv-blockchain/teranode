<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import Toggle from '$lib/components/toggle/index.svelte'
  import i18n from '$internal/i18n'

  const baseKey = 'comp.table-toggle'

  $: t = $i18n.t

  export let value

  let useValue
  $: {
    useValue = value
    if (value === 'dynamic') {
      useValue = $mediaSize <= MediaSize.sm ? 'div' : 'standard'
    }
  }

  const dispatch = createEventDispatcher()

  function onSelect(e) {
    value = e.detail.value
    dispatch('change', e.detail)
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
  on:change={onSelect}
/>
