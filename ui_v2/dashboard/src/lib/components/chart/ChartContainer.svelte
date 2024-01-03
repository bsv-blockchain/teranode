<script lang="ts">
  import { onMount } from 'svelte'

  export let renderKey = ''

  export let width = '100%'
  export let height = '500px'

  let containerRef

  onMount(() => {
    const resizeObserver = new ResizeObserver((entries) => {
      const entry: any = entries.at(0)
      if (entry) {
        const contentRect = entry.contentRect
        renderKey = `${contentRect.width}_${contentRect.height}`
      }
    })
    resizeObserver.observe(containerRef)
    return () => resizeObserver.unobserve(containerRef)
  })
</script>

<div
  class="tui-chart-container"
  bind:this={containerRef}
  style:--height={height}
  style:--width={width}
>
  <slot />
</div>

<style>
  .tui-chart-container {
    box-sizing: var(--box-sizing);
    font-family: var(--font-family);

    width: var(--width);
    height: var(--height);
  }
</style>
