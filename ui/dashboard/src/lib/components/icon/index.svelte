<script lang="ts">
  import icons from './svg'
  import { injectedIcons } from '$lib/stores/media'
  import { toUnit } from '$lib/styles/utils/css'

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let name: string | null | undefined = null
  export let size = 24
  export let opacity = 1
  export let color = 'currentColor'
  export let iconSvg = null

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--width:${toUnit(size)}`,
      `--height:${toUnit(size)}`,
      `--opacity:${opacity}`,
      `--color:${color}`,
      `--margin:0`,
    ]
  }
</script>

<!-- svelte-ignore a11y-click-events-have-key-events -->
<div
  data-test-id={testId}
  class={`tui-icon${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  on:click
>
  {#if iconSvg}
    {@html iconSvg}
  {:else if $injectedIcons[name]}
    {@html $injectedIcons[name]}
  {:else if icons[name]}
    {@html icons[name]}
  {/if}
</div>

<style>
  .tui-icon {
    display: flex;
    opacity: var(--icon-opacity, var(--opacity));
    color: var(--icon-color, var(--color));
    width: var(--icon-size, var(--width));
    height: var(--icon-size, var(--height));
    margin: var(--icon-margin, var(--margin));
  }
</style>
