<svelte:options runes={true} />

<script lang="ts">
  import type { Snippet } from 'svelte'
  import { fade, fly } from 'svelte/transition'

  let {
    coverCol = 'rgba(40, 41, 51, 0.7)',
    maxContentW = '900px',
    fadeCoverDuration = 200,
    flyContent = true,
    children,
  }: {
    coverCol?: string
    maxContentW?: string
    fadeCoverDuration?: number
    flyContent?: boolean
    children?: Snippet
  } = $props()

  let w = $state<number>()
  let h = $state<number>()
</script>

<div
  class="cover"
  transition:fade={{ duration: fadeCoverDuration }}
  style:--cover-col-local={coverCol}></div>

<div
  class="tui-modal"
  in:fly={flyContent ? { y: -200, opacity: 0, duration: 200 } : {}}
  out:fade
  bind:clientWidth={w}
  bind:clientHeight={h}
  style:--width-local="{w}px"
  style:--height-local="{h}px"
  style:--max-content-width={maxContentW}
>
  {@render children?.()}
</div>

<style>
  .cover {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    background: var(--cover-col-local);
    z-index: 999;
  }

  .tui-modal {
    position: absolute;
    left: calc(50% - var(--width-local) / 2);
    top: calc(50% - var(--height-local) / 2);

    display: flex;
    flex-direction: column;
    align-items: center;

    width: calc(100% - 40px);
    max-width: var(--max-content-width);
  }
</style>
