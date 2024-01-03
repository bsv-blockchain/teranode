<script lang="ts">
  import { Typo } from '$lib/components'
  import { LabelAlignment } from '$lib/styles/types'
  import type { LabelAlignmentType } from '$lib/styles/types'

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let disabled = false
  export let size = -1
  export let footnote: any = ''
  export let error: any = ''
  export let stretch = true
  export let html = false

  export let footnoteAlignment: LabelAlignmentType = LabelAlignment.start

  let direction = 'column'
  let justify = 'flex-start'

  let footnoteAlign = 'center'

  $: {
    switch (footnoteAlignment) {
      case 'start':
        footnoteAlign = 'flex-start'
        break
      case 'center':
        footnoteAlign = 'center'
        break
      case 'end':
        footnoteAlign = 'flex-end'
        break
    }
  }

  $: bodyTextSize = size !== -1 ? size : 4

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--direction:${direction}`,
      `--justify:${justify}`,
      `--footnote-align:${footnoteAlign}`,
      `--gap:var(--comp-footnote-gap, 8px)`,
      `--flex:${stretch ? 1 : 0}`,
      `--content-with:${stretch ? '100%' : 'auto'}`,
    ]
  }
</script>

<div
  data-test-id={testId}
  class={`tui-footnote-container${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  tabindex="-1"
>
  <div class="content">
    <slot />
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
