<script lang="ts">
  import logos from './svg'
  import { injectedLogos } from '$lib/stores/media'
  import { toUnit } from '$lib/styles/utils/css'

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let name: string | undefined | null = null
  export let width = -1
  export let height = -1
  export let opacity = 1
  export let logoSvg = null

  let hasNoSize = true

  $: {
    hasNoSize = width === -1 && height === -1
  }

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--width:${toUnit(width)}`,
      `--height:${hasNoSize ? toUnit(64) : toUnit(height)}`,
      `--opacity:${opacity}`,
      `--margin:0`,
    ]
  }
</script>

<!-- svelte-ignore a11y-click-events-have-key-events -->
<div
  role="button"
  tabindex="0"
  data-test-id={testId}
  class={`tui-logo${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  class:o={opacity !== 1}
  class:w={width !== -1}
  class:h={height !== -1 || hasNoSize}
  on:click
  on:keydown={(e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      e.currentTarget.click()
    }
  }}
>
  {#if logoSvg}
    {@html logoSvg}
  {:else if $injectedLogos[name]}
    {@html $injectedLogos[name]}
  {:else if logos[name]}
    {@html logos[name]}
  {/if}
</div>

<style>
  .tui-logo {
    display: flex;
    flex: 0 0 auto;
  }

  .tui-logo.o {
    opacity: var(--logo-opacity, var(--opacity));
  }

  .tui-logo.w {
    width: var(--logo-width, var(--width));
  }

  .tui-logo.h {
    height: var(--logo-height, var(--height));
  }
</style>
