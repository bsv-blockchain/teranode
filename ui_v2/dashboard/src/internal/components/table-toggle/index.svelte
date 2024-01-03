<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import Toggle from '$lib/components/toggle/index.svelte'
  import i18n from '$internal/i18n'

  const baseKey = 'comp.table-toggle'

  $: t = $i18n.t

  export let value = 'div'

  const dispatch = createEventDispatcher()

  function onSelect(e) {
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
  bind:value
  on:change={onSelect}
/>
