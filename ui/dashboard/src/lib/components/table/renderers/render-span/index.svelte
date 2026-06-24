<svelte:options runes={true} />

<script lang="ts">
  let { value = '', className = '' }: { value?: any; className?: string } = $props()

  const spanProps = $derived.by(() => {
    const p: any = {}
    if (className) {
      p.class = className
    }
    return p
  })
</script>

{#if value || value === 0}
  <span {...spanProps}>{value}</span>
{/if}
