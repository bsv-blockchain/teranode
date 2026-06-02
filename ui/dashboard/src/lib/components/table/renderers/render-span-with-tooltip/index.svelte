<svelte:options runes={true} />

<script lang="ts">
  import { tippy } from '$lib/stores/media'

  let {
    value = '',
    className = '',
    tooltip = '',
  }: { value?: any; className?: string; tooltip?: string } = $props()

  const spanProps = $derived.by(() => {
    const p: any = {}
    if (className) {
      p.class = className
    }
    return p
  })
</script>

{#if value || value === 0}
  {#if tooltip && $tippy}
    <span {...spanProps} use:$tippy={{ content: tooltip }}>{value}</span>
  {:else}
    <span {...spanProps}>{value}</span>
  {/if}
{/if}
