<svelte:options runes={true} />

<script lang="ts">
  import { TypoVariant } from './types'
  import type { TypoVariantType } from './types'
  import { title, text, getTypoProps } from '$internal/styles/themes/dark/constants/typography'

  let {
    testId = null,
    class: clazz = null,
    style = '',
    variant = TypoVariant.text,
    size = 'sm',
    color = null,
    hoverColor = null,
    html = false,
    value = '',
    wrap = true,
    overflow = false,
  }: {
    testId?: string | undefined | null
    class?: string | undefined | null
    style?: string
    variant?: TypoVariantType
    size?: string
    color?: string | null
    hoverColor?: string | null
    html?: boolean
    value?: any
    wrap?: boolean
    overflow?: boolean
  } = $props()

  const typoObj = $derived(variant === TypoVariant.text ? text : title)

  const typoProps = $derived(getTypoProps(typoObj, size))

  const cssVars: string[] = $derived([
    `--color:${color ? color : typoProps.color}`,
    `--color-hover:${hoverColor ? hoverColor : color ? color : typoProps.color}`,
    `--font-family:${typoProps.font.family}`,
    `--font-weight:${typoProps.font.weight}`,
    `--font-size:${typoProps.font.size}`,
    `--line-height:${typoProps.line.height}`,
    `--wrap:${wrap ? 'normal' : 'nowrap'}`,
    `--margin:0`,
  ])
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
