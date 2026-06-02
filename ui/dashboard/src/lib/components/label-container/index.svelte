<svelte:options runes={true} />

<script lang="ts">
  import type { Snippet } from 'svelte'
  // import { Typo } from '$lib/components'
  import {
    ComponentSize,
    LabelAlignment,
    LabelPlacement,
    getStyleSizeFromComponentSize,
  } from '$lib/styles/types'
  import type { ComponentSizeType, LabelAlignmentType, LabelPlacementType } from '$lib/styles/types'
  // import type { TypoVariantType } from '../typo/types'

  let {
    testId = null,
    class: clazz = null,
    style = '',
    name = '',
    disabled = false,
    required = false,
    label = '',
    // variant = 'heading',
    interactive = false,
    margin = '0',
    stretch = true,
    labelPlacement = LabelPlacement.top,
    labelAlignment = LabelAlignment.start,
    size = ComponentSize.medium,
    onclick,
    children,
  }: {
    testId?: string | undefined | null
    class?: string | undefined | null
    style?: string
    name?: string
    disabled?: boolean
    required?: boolean
    label?: any
    interactive?: boolean
    margin?: string
    stretch?: boolean
    labelPlacement?: LabelPlacementType
    labelAlignment?: LabelAlignmentType
    size?: ComponentSizeType
    onclick?: ((e: MouseEvent) => void) | null
    children?: Snippet
  } = $props()

  const styleSize = $derived(getStyleSizeFromComponentSize(size))

  const direction = $derived.by(() => {
    switch (labelPlacement) {
      case 'top':
        return 'column'
      case 'bottom':
        return 'column-reverse'
      case 'left':
        return 'row'
      case 'right':
        return 'row-reverse'
      default:
        return 'row'
    }
  })

  const justify = $derived(
    labelPlacement === 'bottom' || labelPlacement === 'right' ? 'flex-end' : 'flex-start',
  )

  const labelAlign = $derived.by(() => {
    switch (labelAlignment) {
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

  const compSizeStr = $derived(`--comp-size-${styleSize}`)

  const cssVars = $derived([
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
  ])
</script>

<!-- svelte-ignore a11y_click_events_have_key_events -->
<div
  role={interactive && !disabled ? 'button' : null}
  data-test-id={testId}
  class={`tui-label-container${clazz ? ' ' + clazz : ''}`}
  class:interactive={interactive && !disabled}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  {onclick}
  onkeydown={(e) => {
    if (interactive && !disabled && (e.key === 'Enter' || e.key === ' ')) {
      e.preventDefault()
      e.currentTarget.click()
    }
  }}
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
    <label class="label" aria-label={label} id={`${name}_label`} for={name}>
      {label}
      {required ? ' *' : ''}
    </label>
  {/if}
  <div class="content">
    {@render children?.()}
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
