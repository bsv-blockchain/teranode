<script lang="ts">
  import { toUnit } from '$lib/styles/utils/css'
  import { iconNameOverrides, useLibIcons } from '$lib/stores/media'

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let name: string | null | undefined = null
  export let size = 24
  export let opacity = 1
  export let color = 'currentColor'
  export let iconSvg = null

  let finalName = name
  $: {
    finalName = name

    if (name && $iconNameOverrides[name]) {
      finalName = $iconNameOverrides[name]
    }
  }

  let SvgIcon

  async function loadSvgComp(name) {
    SvgIcon = null

    let result
    try {
      result = await import(`../../../internal/assets/icons/${name}.svg?component`)
    } catch (e) {
      result = null
    }

    if ($useLibIcons && !result) {
      try {
        result = await import(`../../../lib/assets/icons/${name}.svg?component`)
      } catch (e) {
        result = null
      }
    }

    if (result) {
      SvgIcon = result.default
    }
  }

  $: {
    if (finalName) {
      loadSvgComp(finalName)
    }
  }

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
  {#if finalName && SvgIcon}
    {#key finalName}
      <SvgIcon />
    {/key}
  {:else if iconSvg}
    {@html iconSvg}
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
