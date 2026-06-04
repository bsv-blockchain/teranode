<svelte:options runes={true} />

<script lang="ts">
  import type { Snippet } from 'svelte'
  import { Typo } from '$lib/components'
  import { LabelAlignment } from '$lib/styles/types'
  import type { LabelAlignmentType } from '$lib/styles/types'

  let {
    testId = null,
    class: clazz = null,
    style = '',
    disabled = false,
    size = -1,
    footnote = '',
    error = '',
    stretch = true,
    html = false,
    footnoteAlignment = LabelAlignment.start,
    children,
  }: {
    testId?: string | undefined | null
    class?: string | undefined | null
    style?: string
    disabled?: boolean
    size?: number
    footnote?: any
    error?: any
    stretch?: boolean
    html?: boolean
    footnoteAlignment?: LabelAlignmentType
    children?: Snippet
  } = $props()

  const direction = 'column'
  const justify = 'flex-start'

  const footnoteAlign = $derived.by(() => {
    switch (footnoteAlignment) {
      case 'start':
        return 'flex-start'
      case 'center':
        return 'center'
      case 'end':
        return 'flex-end'
      default:
        return 'center'
    }
  })

  const bodyTextSize = $derived(size !== -1 ? size : 4)

  const cssVars = $derived([
    `--direction:${direction}`,
    `--justify:${justify}`,
    `--footnote-align:${footnoteAlign}`,
    `--gap:var(--comp-footnote-gap, 8px)`,
    `--flex:${stretch ? 1 : 0}`,
    `--content-with:${stretch ? '100%' : 'auto'}`,
  ])
</script>

<div
  data-test-id={testId}
  class={`tui-footnote-container${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  tabindex="-1"
>
  <div class="content">
    {@render children?.()}
  </div>
  {#if footnote}
    <Typo
      variant="body"
      size={bodyTextSize}
      value={footnote}
      {html}
      style={`--color:var(${
        disabled ? '--comp-footnote-disabled-color' : '--comp-footnote-color'
      })`}
    />
  {/if}
  {#if error}
    <Typo
      variant="body"
      size={bodyTextSize}
      value={error}
      style={`--color:var(--comp-footnote-error-color)`}
    />
  {/if}
</div>

<style>
  .tui-footnote-container {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    display: flex;
    flex-direction: var(--direction);
    align-items: var(--footnote-align);
    justify-content: var(--justify);
    gap: var(--gap);
  }

  .content {
    flex: var(--flex);
    width: var(--content-with);
  }
</style>
