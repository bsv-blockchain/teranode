<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { FocusRect, FootnoteContainer, LabelContainer } from '$lib/components'
  import {
    ComponentSize,
    LabelAlignment,
    LabelPlacement,
    getStyleSizeFromComponentSize,
  } from '$lib/styles/types'
  import type { LabelAlignmentType, LabelPlacementType } from '$lib/styles/types'
  import type { InputSizeType } from '$lib/styles/types/input'

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let label: any = 'Lalala'
  export let footnote: any = ''
  export let required = false
  export let name = ''
  export let group = ''
  export let checked = false
  export let disabled = false
  export let valid = true
  export let error = ''
  export let allowToggle = false

  let type = 'radio'

  export let labelPlacement: LabelPlacementType = LabelPlacement.right
  export let labelAlignment: LabelAlignmentType = LabelAlignment.center

  export let size: InputSizeType = ComponentSize.large
  $: styleSize = getStyleSizeFromComponentSize(size)

  $: cbVarStr = `--checkbox-default`
  $: cbSizeStr = `--checkbox-size-${styleSize}`
  $: radioSizeStr = `--radio-size-${styleSize}`

  let cssVars: string[] = []
  $: {
    let states = ['enabled', 'hover', 'focused', 'checked', 'disabled']
    cssVars = [
      ...states.reduce(
        (acc, state) => [
          ...acc,
          `--${state}-color:var(${cbVarStr}-${state}-color)`,
          `--${state}-bg-color:var(${cbVarStr}-${state}-bg-color)`,
          `--${state}-border-color:var(${cbVarStr}-${state}-border-color)`,
        ],
        [] as string[],
      ),
      `--invalid-border-color:var(--checkbox-default-invalid-border-color)`,
      `--size:var(${radioSizeStr}-size)`,
      `--border-width:var(--comp-border-width)`,
      `--border-radius:var(--checkbox-border-radius)`,
      `--icon-size:var(${radioSizeStr}-icon-size)`,
      `--focus-rect-size:calc(var(${cbSizeStr}-size) / 2)`,
    ]
  }

  $: blockInteraction = disabled || (checked && !allowToggle)

  let focused = false

  let inputRef

  function onInputParentClick() {
    if (blockInteraction) {
      return
    }
    inputRef.focus()
    checked = !checked
    dispatch('change', { name, group, type, checked })
  }

  function onFocusAction(eventName) {
    switch (eventName) {
      case 'blur':
        focused = false
        break
      case 'focus':
        focused = true
        break
    }
    dispatch(eventName)
  }
</script>

<FootnoteContainer {footnote} {error} {disabled}>
  <LabelContainer
    {name}
    {size}
    {disabled}
    {label}
    {labelAlignment}
    {labelPlacement}
    {required}
    margin="-2px 0 0 0"
    interactive={!disabled && !blockInteraction}
    on:click={disabled || blockInteraction ? null : onInputParentClick}
  >
    <!-- svelte-ignore a11y-click-events-have-key-events -->
    <div
      data-test-id={testId}
      class={`tui-radio${clazz ? ' ' + clazz : ''}`}
      style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
    >
      <FocusRect
        {disabled}
        style={`--focus-rect-width:1px;--focus-rect-bg-color:#FFFFFF;--focus-rect-padding:0px;--focus-rect-border-radius:calc((var(${radioSizeStr}-size) + 2px) / 2)`}
      >
        <div class="input" class:disabled class:error={error || !valid} class:focused class:checked>
          <input
            bind:this={inputRef}
            {type}
            {name}
            {checked}
            on:focus={() => onFocusAction('focus')}
            on:blur={() => onFocusAction('blur')}
            aria-labelledby={`${name}_label`}
          />
          <div
            class="icon"
            style={`width:var(${radioSizeStr}-icon-size); height:var(${radioSizeStr}-icon-size); border-radius:calc(var(${radioSizeStr}-icon-size) / 2)`}
          />
        </div>
      </FocusRect>
    </div>
  </LabelContainer>
</FootnoteContainer>

<style>
  .tui-radio {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);
  }

  input {
    box-sizing: var(--box-sizing);

    outline: none;
    border: none;
    position: absolute;
    opacity: 0;
    pointer-events: none;
  }

  .input {
    box-sizing: var(--box-sizing);

    display: flex;
    align-items: center;
    justify-content: center;
    width: var(--size);
    height: var(--size);

    border-width: var(--border-width);
    border-style: solid;
    border-radius: calc(var(--size) / 2);

    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);
    transition:
      color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out),
      background-color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);

    cursor: var(--cursor-local);
  }

  .input:hover {
    border-color: var(--hover-border-color);
  }
  .input:hover .icon {
    background-color: var(--hover-bg-color);
  }

  .input.focused {
    border-color: var(--focused-border-color);
  }
  .input.focused .icon {
    background-color: var(--focused-bg-color);
  }

  .input.checked {
    border-color: var(--checked-border-color);
  }
  .input.checked .icon {
    background-color: var(--checked-bg-color);
  }

  .disabled,
  .disabled:active,
  .input.disabled {
    background-color: var(--enabled-bg-color);
    border-color: var(--disabled-border-color);
  }

  .input.checked.disabled .icon {
    background-color: var(--disabled-bg-color);
  }

  .input.error,
  .input.error.focused {
    border-color: var(--invalid-border-color);
  }
  .input.error .icon,
  .input.error.focused .icon,
  .input.error.disabled .icon {
    background-color: var(--invalid-border-color);
  }
</style>
