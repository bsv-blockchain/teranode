<script lang="ts">
  // import { Typo } from '$lib/components'
  import {
    ComponentSize,
    LabelAlignment,
    LabelPlacement,
    getStyleSizeFromComponentSize,
  } from '$lib/styles/types'
  import type { ComponentSizeType, LabelAlignmentType, LabelPlacementType } from '$lib/styles/types'
  import type { TypoVariantType } from '../typo/types'

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let name = ''
  export let disabled = false
  export let required = false
  export let label: any = ''
  // export let variant: TypoVariantType = 'heading'
  export let interactive = false
  export let margin = '0'
  export let stretch = true

  export let labelPlacement: LabelPlacementType = LabelPlacement.top
  export let labelAlignment: LabelAlignmentType = LabelAlignment.start

  export let size: ComponentSizeType = ComponentSize.medium
  $: styleSize = getStyleSizeFromComponentSize(size)

  let direction = 'row'
  let justify = 'flex-start'

  $: {
    switch (labelPlacement) {
      case 'top':
        direction = 'column'
        break
      case 'bottom':
        direction = 'column-reverse'
        justify = 'flex-end'
        break
      case 'left':
        direction = 'row'
        break
      case 'right':
        direction = 'row-reverse'
        justify = 'flex-end'
        break
    }
  }

  let labelAlign = 'center'

  $: {
    switch (labelAlignment) {
      case 'start':
        labelAlign = 'flex-start'
        break
      case 'center':
        labelAlign = 'center'
        break
      case 'end':
        labelAlign = 'flex-end'
        break
    }
  }

  // TODO: handle sizes differently
  // let typoSize = 1
  // $: {
  //   switch (variant) {
  //     case 'heading':
  //       typoSize = size === ComponentSize.small ? 7 : 6
  //       break
  //     case 'body':
  //       switch (size) {
  //         case ComponentSize.small:
  //           typoSize = 4
  //           break
  //         case ComponentSize.medium:
  //           typoSize = 3
  //           break
  //         case ComponentSize.large:
  //           typoSize = 2
  //           break
  //       }
  //       break
  //   }
  // }

  $: compSizeStr = `--comp-size-${styleSize}`
  // $: labelSizeStr = `--label-size-${styleSize}`

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--direction:${direction}`,
      `--justify:${justify}`,
      `--label-align:${labelAlign}`,
      `--gap:var(--comp-label-gap, 8px)`,
      `--flex:${stretch ? 1 : 0}`,
      `--content-with:${stretch ? '100%' : 'auto'}`,
      `--font-family:var(--label-font-family, var(--comp-font-family))`,
      `--font-size:var(${compSizeStr}-font-size)`,
      `--color:${disabled ? 'var(--comp-label-disabled-color)' : 'var(--comp-label-color)'}`,
      `--margin:${margin}`,
    ]
  }
</script>

<!-- svelte-ignore a11y-click-events-have-key-events -->
<div
  data-test-id={testId}
  class={`tui-label-container${clazz ? ' ' + clazz : ''}`}
  class:interactive={interactive && !disabled}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  on:click
  tabindex="-1"
>
  {#if label}
    <!-- <Typo
      {variant}
      size={typoSize}
      value={`${label}${required ? ' *' : ''}`}
      style={disabled
        ? `--color:var(--comp-label-disabled-color);--margin:${margin}`
        : `--margin:${margin}`}
    /> -->
    <label class="label" aria-label={label} id={`${name}_label`}>
      {label}
      {required ? ' *' : ''}
    </label>
  {/if}
  <div class="content">
    <slot />
  </div>
</div>

<style>
  .tui-label-container {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    display: flex;
    flex-direction: var(--direction);
    align-items: var(--label-align);
    justify-content: var(--justify);
    gap: var(--gap);
  }
  .tui-label-container.interactive {
    cursor: pointer;
  }
  .tui-label-container .label {
    font-family: var(--font-family);
    font-size: var(--font-size);
    color: var(--color);
    margin: var(--margin);
  }

  .content {
    flex: var(--flex);
    width: var(--content-with);
  }
</style>
