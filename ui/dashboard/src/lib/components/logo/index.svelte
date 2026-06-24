<svelte:options runes={true} />

<script lang="ts">
  import logos from './svg'
  import { injectedLogos } from '$lib/stores/media'
  import { toUnit } from '$lib/styles/utils/css'

  let {
    testId = null,
    class: clazz = null,
    style = '',
    name = null,
    width = -1,
    height = -1,
    opacity = 1,
    logoSvg = null,
    onclick,
  }: {
    testId?: string | undefined | null
    class?: string | undefined | null
    style?: string
    name?: string | undefined | null
    width?: number
    height?: number
    opacity?: number
    logoSvg?: string | null
    onclick?: (e: MouseEvent) => void
  } = $props()

  let hasNoSize = $derived(width === -1 && height === -1)

  let cssVars: string[] = $derived([
    `--width:${toUnit(width)}`,
    `--height:${hasNoSize ? toUnit(64) : toUnit(height)}`,
    `--opacity:${opacity}`,
    `--margin:0`,
  ])
</script>

<!-- svelte-ignore a11y_click_events_have_key_events -->
<div
  role="button"
  tabindex="0"
  data-test-id={testId}
  class={`tui-logo${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  class:o={opacity !== 1}
  class:w={width !== -1}
  class:h={height !== -1 || hasNoSize}
  {onclick}
  onkeydown={(e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      e.currentTarget.click()
    }
  }}
>
  {#if logoSvg}
    {@html logoSvg}
  {:else if name && $injectedLogos[name]}
    {@html $injectedLogos[name]}
  {:else if name && logos[name]}
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
