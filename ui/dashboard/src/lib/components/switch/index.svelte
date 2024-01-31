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
  export let checked = false
  export let disabled = false
  export let valid = true
  export let error = ''

  let type = 'checkbox'

  export let labelPlacement: LabelPlacementType = LabelPlacement.right
  export let labelAlignment: LabelAlignmentType = LabelAlignment.center

  export let size: InputSizeType = ComponentSize.large
  $: styleSize = getStyleSizeFromComponentSize(size)

  $: cbVarStr = `--switch-default`
  $: switchSizeStr = `--switch-size-${styleSize}`

  let cssVars: string[] = []
  $: {
    let states = [
      'enabled',
      'enabled-checked',
      'hover',
      'hover-checked',
      'focused',
      'focused-checked',
      'disabled',
      'disabled-checked',
    ]
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
      `--width:var(${switchSizeStr}-width)`,
      `--height:var(${switchSizeStr}-height)`,
      `--padding:var(${switchSizeStr}-padding)`,
      `--icon-size:var(${switchSizeStr}-icon-size)`,
      `--border-width:var(${switchSizeStr}-border-width, var(--comp-border-width))`,
      `--focus-rect-width:var(${switchSizeStr}-focus-rect-width, var(--comp-focus-rect-width))`,
      `--focus-rect-padding:var(${switchSizeStr}-focus-rect-padding, var(--comp-focus-rect-padding))`,
    ]
  }

  let focused = false

  let inputRef

  function onInputParentClick() {
    inputRef.focus()
    checked = !checked
    dispatch('change', { name, type, checked })
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
    margin="-4px 0 0 0"
    interactive
    on:click={disabled ? null : onInputParentClick}
  >
    <!-- svelte-ignore a11y-click-events-have-key-events -->
    <div
      data-test-id={testId}
      class={`tui-switch${clazz ? ' ' + clazz : ''}`}
      style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
    >
      <FocusRect
        {disabled}
        style={`--focus-rect-padding:var(${switchSizeStr}-focus-rect-padding, var(--comp-focus-rect-padding));--focus-rect-width:var(${switchSizeStr}-focus-rect-width, var(--comp-focus-rect-width));--focus-rect-border-radius:calc(var(${switchSizeStr}-height) / 2)`}
      >
        <div class="input" class:disabled class:error={!valid} class:focused class:checked>
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
            style={`width:var(${switchSizeStr}-icon-size); height:var(${switchSizeStr}-icon-size); border-radius:calc(var(${switchSizeStr}-icon-size) / 2)`}
          />
        </div>
      </FocusRect>
    </div>
  </LabelContainer>
</FootnoteContainer>

<style>
  .tui-switch {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);
  }

  .icon {
    width: var(--icon-size);
    height: var(--icon-size);
    background-color: var(--enabled-color);

    margin-left: var(--padding);
    transition: margin-left var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
  }
  .input.checked .icon {
    margin-left: calc(var(--padding) + var(--icon-size));
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
    justify-content: flex-start;
    width: var(--width);
    height: var(--height);

    border-width: var(--border-width);
    border-style: solid;
    border-radius: calc(var(--height) / 2);

    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);
    transition:
      color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out),
      background-color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);

    cursor: var(--cursor-local);
  }
  .input .icon {
    background-color: currentColor;
  }
  .input.checked {
    color: var(--enabled-checked-color);
    background-color: var(--enabled-checked-bg-color);
    border-color: var(--enabled-checked-border-color);
  }

  .input:hover {
    color: var(--hover-color);
    background-color: var(--hover-bg-color);
    border-color: var(--hover-border-color);
  }
  .input.checked:hover {
    color: var(--hover-checked-color);
    background-color: var(--hover-checked-bg-color);
    border-color: var(--hover-checked-border-color);
  }

  .input.focused {
    color: var(--focused-color);
    background-color: var(--focused-bg-color);
    border-color: var(--focused-border-color);
  }
  .input.focused.checked {
    color: var(--focused-checked-color);
    background-color: var(--focused-checked-bg-color);
    border-color: var(--focused-checked-border-color);
  }

  .input.error,
  .input.error.focused {
    color: var(--enabled-color);
    border-color: var(--invalid-border-color);
  }

  .disabled,
  .disabled:active,
  .input.disabled {
    color: var(--disabled-color);
    background-color: var(--disabled-bg-color);
    border-color: var(--disabled-border-color);
  }

  .input.checked.disabled {
    color: var(--disabled-checked-color);
    background-color: var(--disabled-checked-bg-color);
    border-color: var(--disabled-checked-border-color);
  }
</style>
