<svelte:options runes={true} />

<script lang="ts">
  import { FocusRect, FootnoteContainer, Icon, LabelContainer } from '$lib/components'
  import {
    ComponentSize,
    LabelAlignment,
    LabelPlacement,
    getStyleSizeFromComponentSize,
  } from '$lib/styles/types'
  import type { LabelAlignmentType, LabelPlacementType } from '$lib/styles/types'
  import type { InputSizeType } from '$lib/styles/types/input'

  let {
    testId = null,
    class: clazz = null,
    style = '',
    label = '',
    footnote = '',
    required = false,
    name = '',
    checked = $bindable(false),
    disabled = false,
    valid = true,
    error = '',
    labelPlacement = LabelPlacement.right,
    labelAlignment = LabelAlignment.center,
    size = ComponentSize.medium,
    onchange,
    onfocus,
    onblur,
  }: {
    testId?: string | undefined | null
    class?: string | undefined | null
    style?: string
    label?: any
    footnote?: any
    required?: boolean
    name?: string
    checked?: boolean
    disabled?: boolean
    valid?: boolean
    error?: string
    labelPlacement?: LabelPlacementType
    labelAlignment?: LabelAlignmentType
    size?: InputSizeType
    onchange?: (e: { name: string; type: string; checked: boolean }) => void
    onfocus?: () => void
    onblur?: () => void
  } = $props()

  const type = 'checkbox'

  const styleSize = $derived(getStyleSizeFromComponentSize(size))

  const cbVarStr = $derived(`--checkbox-default`)
  const cbSizeStr = $derived(`--checkbox-size-${styleSize}`)

  const cssVars = $derived.by(() => {
    const states = ['enabled', 'hover', 'focused', 'checked', 'disabled']
    return [
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
  })

  // TODO: fix check icon, this is a tmp workaround
  const iconMarginTop = $derived.by(() => {
    switch (size) {
      case ComponentSize.small:
        return '-7px'
      case ComponentSize.medium:
        return '-3px'
      case ComponentSize.large:
        return '0'
      default:
        return '0'
    }
  })

  let focused = $state(false)

  let inputRef = $state<HTMLInputElement>()

  function onInputParentClick() {
    inputRef?.focus()
    checked = !checked
    onchange?.({ name, type, checked })
  }

  function onFocusAction(eventName: 'focus' | 'blur') {
    switch (eventName) {
      case 'blur':
        focused = false
        break
      case 'focus':
        focused = true
        break
    }
    if (eventName === 'focus') onfocus?.()
    else if (eventName === 'blur') onblur?.()
  }
</script>

<FootnoteContainer {footnote} {error} {disabled} stretch={false}>
  <LabelContainer
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
    onclick={disabled ? null : onInputParentClick}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events -->
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
            onfocus={() => onFocusAction('focus')}
            onblur={() => onFocusAction('blur')}
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
