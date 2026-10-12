<svelte:options runes={true} />

<script lang="ts">
  import { TypoVariant } from './types'
  import type { TypoVariantType } from './types'

  let {
    testId = null,
    class: clazz = null,
    style = '',
    variant = TypoVariant.heading,
    size = 1,
    color = null,
    html = false,
    value = '',
    wrap = true,
  }: {
    testId?: string | undefined | null
    class?: string | undefined | null
    style?: string
    variant?: TypoVariantType
    size?: number
    color?: string | null
    html?: boolean
    value?: any
    wrap?: boolean
  } = $props()

  const cssVars: string[] = $derived.by(() => {
    const varStr = `--typo-${variant}`
    const sizeStr = `${varStr}-${size}`
    return [
      `--color:${color ? color : `var(${varStr}-color)`}`,
      `--font-family:var(${varStr}-font-family)`,
      `--font-weight:var(${varStr}-font-weight)`,
      `--font-size:var(${sizeStr}-font-size)`,
      `--line-height:var(${sizeStr}-line-height)`,
      `--wrap:${wrap ? 'normal' : 'nowrap'}`,
      `--margin:0`,
    ]
  })
</script>

<span
  data-test-id={testId}
  class={`tui-typo${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
>
  {#if html}
    {@html value}
  {:else}
    {value}
  {/if}
</span>

<style>
  .tui-typo {
    color: var(--color);
    font-family: var(--font-family);
    font-weight: var(--font-weight);
    font-size: var(--font-size);
    line-height: var(--line-height);
    margin: var(--margin);

    white-space: var(--wrap);
  }
</style>
