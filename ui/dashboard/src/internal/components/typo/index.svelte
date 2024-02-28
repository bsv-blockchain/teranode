<script lang="ts">
  import { TypoVariant } from './types'
  import type { TypoVariantType } from './types'
  import { title, text, getTypoProps } from '$internal/styles/themes/dark/constants/typography'

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let variant: TypoVariantType = TypoVariant.text
  export let size = 'sm'
  export let color: string | null = null
  export let hoverColor: string | null = null
  export let html = false
  export let value: any = ''
  export let wrap = true
  export let overflow = false

  $: typoObj = variant === TypoVariant.text ? text : title

  $: typoProps = getTypoProps(typoObj, size)

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--color:${color ? color : typoProps.color}`,
      `--color-hover:${hoverColor ? hoverColor : color ? color : typoProps.color}`,
      `--font-family:${typoProps.font.family}`,
      `--font-weight:${typoProps.font.weight}`,
      `--font-size:${typoProps.font.size}`,
      `--line-height:${typoProps.line.height}`,
      `--wrap:${wrap ? 'normal' : 'nowrap'}`,
      `--margin:0`,
    ]
  }
</script>

<span
  data-test-id={testId}
  class={`tui-typo${clazz ? ' ' + clazz : ''}`}
  class:single-line={!wrap && !overflow}
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
  .tui-typo:hover {
    color: var(--color-hover);
  }
  .tui-typo.single-line {
    overflow: hidden;
    text-overflow: ellipsis;
  }
</style>
