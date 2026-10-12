<svelte:options runes={true} />

<script lang="ts">
  import type { Snippet } from 'svelte'
  import { onMount } from 'svelte'

  let {
    renderKey = $bindable(''),
    width = '100%',
    height = '500px',
    children,
  }: {
    renderKey?: string
    width?: string
    height?: string
    children?: Snippet
  } = $props()

  let containerRef = $state<HTMLDivElement>()

  onMount(() => {
    const resizeObserver = new ResizeObserver((entries) => {
      const entry: any = entries.at(0)
      if (entry) {
        const contentRect = entry.contentRect
        renderKey = `${contentRect.width}_${contentRect.height}`
      }
    })
    if (containerRef) {
      resizeObserver.observe(containerRef)
    }
    return () => {
      if (containerRef) {
        resizeObserver.unobserve(containerRef)
      }
    }
  })
</script>

<div
  class="tui-chart-container"
  bind:this={containerRef}
  style:--height={height}
  style:--width={width}
>
  {@render children?.()}
</div>

<style>
  .tui-chart-container {
    box-sizing: var(--box-sizing);
    font-family: var(--font-family);

    width: var(--width);
    height: var(--height);
  }
</style>
