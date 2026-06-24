<svelte:options runes={true} />

<script lang="ts">
  import type { Component } from 'svelte'
  import { toUnit } from '$lib/styles/utils/css'
  import { iconNameOverrides, useLibIcons } from '$lib/stores/media'

  let {
    testId = null,
    class: clazz = null,
    style = '',
    name = null,
    size = 24,
    opacity = 1,
    color = 'currentColor',
    iconSvg = null,
    onclick,
  }: {
    testId?: string | undefined | null
    class?: string | undefined | null
    style?: string
    name?: string | null | undefined
    size?: number
    opacity?: number
    color?: string
    iconSvg?: string | null
    onclick?: (e: MouseEvent) => void
  } = $props()

  const finalName = $derived.by(() => {
    if (name && $iconNameOverrides[name]) {
      return $iconNameOverrides[name]
    }
    return name
  })

  let SvgIcon = $state<Component | null>(null)

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

  $effect(() => {
    if (finalName) {
      loadSvgComp(finalName)
    }
  })

  const cssVars = $derived([
    `--width:${toUnit(size)}`,
    `--height:${toUnit(size)}`,
    `--opacity:${opacity}`,
    `--color:${color}`,
    `--margin:0`,
  ])
</script>

<!-- svelte-ignore a11y_click_events_have_key_events -->
<div
  role="button"
  tabindex="0"
  data-test-id={testId}
  class={`tui-icon${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  {onclick}
  onkeydown={(e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      e.currentTarget.click()
    }
  }}
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
