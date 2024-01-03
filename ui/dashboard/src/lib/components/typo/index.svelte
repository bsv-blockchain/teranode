<script lang="ts">
  import { TypoVariant } from './types'
  import type { TypoVariantType } from './types'

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let variant: TypoVariantType = TypoVariant.heading
  export let size = 1
  export let color: string | null = null
  export let html = false
  export let value: any = ''
  export let wrap = true

  let cssVars: string[] = []
  $: {
    let varStr = `--typo-${variant}`
    let sizeStr = `${varStr}-${size}`
    cssVars = [
      `--color:${color ? color : `var(${varStr}-color)`}`,
      `--font-family:var(${varStr}-font-family)`,
      `--font-weight:var(${varStr}-font-weight)`,
      `--font-size:var(${sizeStr}-font-size)`,
      `--line-height:var(${sizeStr}-line-height)`,
      `--wrap:${wrap ? 'normal' : 'nowrap'}`,
      `--margin:0`,
    ]
  }
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
