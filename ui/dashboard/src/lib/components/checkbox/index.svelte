<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { FocusRect, FootnoteContainer, Icon, LabelContainer } from '$lib/components'
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

  export let label: any = ''
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

  export let size: InputSizeType = ComponentSize.medium
  $: styleSize = getStyleSizeFromComponentSize(size)

  $: cbVarStr = `--checkbox-default`
  $: cbSizeStr = `--checkbox-size-${styleSize}`

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
      `--size:var(${cbSizeStr}-size)`,
      `--border-width:var(--comp-border-width)`,
      `--border-radius:var(--checkbox-border-radius)`,
      `--icon-size:var(${cbSizeStr}-icon-size)`,
    ]
  }

  // TODO: fix check icon, this is a tmp workaround
  let iconMarginTop = '0'
  $: {
    switch (size) {
      case ComponentSize.small:
        iconMarginTop = '-7px'
        break
      case ComponentSize.medium:
        iconMarginTop = '-3px'
        break
      case ComponentSize.large:
        iconMarginTop = '0'
        break
    }
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

<FootnoteContainer {footnote} {error} {disabled} stretch={false}>
  <LabelContainer
    variant="body"
    {name}
    {size}
    {disabled}
    {label}
    {labelAlignment}
    {labelPlacement}
    {required}
    stretch={false}
    margin="-2px 0 0 0"
    interactive
    on:click={disabled ? null : onInputParentClick}
  >
    <!-- svelte-ignore a11y-click-events-have-key-events -->
    <div
      data-test-id={testId}
      class={`tui-checkbox${clazz ? ' ' + clazz : ''}`}
      style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
    >
      <FocusRect
        {disabled}
        style={`--focus-rect-width:1px;--focus-rect-bg-color:#FFFFFF;--focus-rect-padding:1px`}
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
          <Icon
            class="icon"
            name="check"
            style={`--width:var(${cbSizeStr}-size);--height:var(${cbSizeStr}-size);--margin:${iconMarginTop} 0 0 0`}
          />
        </div>
      </FocusRect>
    </div>
  </LabelContainer>
</FootnoteContainer>

<style>
  .tui-checkbox {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);
  }

  .icon {
    width: var(--icon-size);
    height: var(--icon-size);
    margin-top: -14px;
    color: red;
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
    border-radius: var(--border-radius);

    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);
    transition:
      color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out),
      background-color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);

    cursor: var(--cursor-local);
  }

  .input:hover {
    background-color: var(--hover-bg-color);
    border-color: var(--hover-border-color);
  }

  .input.focused {
    background-color: var(--focused-bg-color);
    border-color: var(--focused-border-color);
  }

  .input.error,
  .input.error.focused {
    border-color: var(--invalid-border-color);
  }

  .input.checked {
    background-color: var(--checked-bg-color);
    border-color: var(--checked-border-color);
  }

  .disabled,
  .disabled:active,
  .input.disabled {
    background-color: var(--enabled-bg-color);
    border-color: var(--disabled-border-color);
  }

  .input.checked.disabled {
    background-color: var(--disabled-bg-color);
  }
</style>
