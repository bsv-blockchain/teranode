<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte'
  import { FootnoteContainer, Icon, LabelContainer, FocusRect } from '$lib/components'
  import {
    ComponentSize,
    LabelAlignment,
    LabelPlacement,
    getStyleSizeFromComponentSize,
  } from '$lib/styles/types'
  import type { LabelAlignmentType, LabelPlacementType } from '$lib/styles/types'
  import type { InputSizeType } from '$lib/styles/types/input'
  import { valueSet } from '$lib/utils'

  let {
    testId = null,
    class: clazz = null,
    style = '',
    type = 'text',
    min = null,
    max = null,
    label = '',
    footnote = '',
    footnoteHtml = true,
    required = false,
    name = '',
    value = $bindable(''),
    placeholder = null,
    disabled = false,
    valid = true,
    error = '',
    autocomplete = 'off',
    stretch = false,
    width = -1,
    focusWidth = -1,
    icon = null,
    iconAfter = null,
    labelPlacement = LabelPlacement.top,
    labelAlignment = LabelAlignment.start,
    size = ComponentSize.medium,
    confirm = false,
    onchange,
    onmount,
    onconfirm,
    onkeydown,
    onfocus,
    onblur,
  }: {
    testId?: string | undefined | null
    class?: string | undefined | null
    style?: string
    type?: 'text' | 'number'
    min?: number | null
    max?: number | null
    label?: any
    footnote?: any
    footnoteHtml?: boolean
    required?: boolean
    name?: string
    value?: string
    placeholder?: any
    disabled?: boolean
    valid?: boolean
    error?: string
    autocomplete?: string | undefined | null
    stretch?: boolean
    width?: number
    focusWidth?: number
    icon?: string | undefined | null
    iconAfter?: string | undefined | null
    labelPlacement?: LabelPlacementType
    labelAlignment?: LabelAlignmentType
    size?: InputSizeType
    confirm?: boolean
    onchange?: (e: { name: string; type: string; value: string }) => void
    onmount?: (e: { inputRef: HTMLInputElement | undefined }) => void
    onconfirm?: (e: { name: string; type: string; value: string }) => void
    onkeydown?: (e: KeyboardEvent) => void
    onfocus?: () => void
    onblur?: () => void
  } = $props()

  const styleSize = $derived(getStyleSizeFromComponentSize(size))

  const inputVarStr = `--input-default`
  const inputSizeStr = $derived(`--input-size-${styleSize}`)

  // in confirm mode, changes are local until confirm is clicked,
  // or alternatively changes can be reset to previous non-local value
  let localValue = $state(value)

  $effect(() => {
    localValue = value
  })

  const placeholderActive = $derived(placeholder && !localValue)

  const cssVars = $derived.by(() => {
    const states = ['enabled', 'hover', 'active', 'focus', 'disabled']
    return [
      ...states.reduce(
        (acc, state) => [
          ...acc,
          `--${state}-color:${
            placeholderActive
              ? 'var(--input-placeholder-color)'
              : `var(${inputVarStr}-${state}-color)`
          }`,
          `--${state}-bg-color:${`var(${inputVarStr}-${state}-bg-color)`}`,
          `--${state}-border-color:var(${inputVarStr}-${state}-border-color)`,
        ],
        [] as string[],
      ),
      `--invalid-border-color:var(--input-default-invalid-border-color)`,
      `--height:var(${inputSizeStr}-height)`,
      `--padding:var(${inputSizeStr}-padding)`,
      `--border-radius:var(${inputSizeStr}-border-radius)`,
      `--icon-size:var(${inputSizeStr}-icon-size)`,
      `--font-size:var(${inputSizeStr}-font-size)`,
      `--line-height:var(${inputSizeStr}-line-height)`,
      `--letter-spacing:var(${inputSizeStr}-letter-spacing)`,
      `--font-weight:var(--input-font-weight)`,
      `--border-width:var(--comp-border-width)`,
      `--gap:var(--button-icon-gap, 6px)`,
      `--width:${width}px`,
      `--focus-width:${focusWidth}px`,
    ]
  })

  const inputOpts = $derived.by(() => {
    const opts: any = {}

    if (autocomplete) {
      opts.autocomplete = autocomplete
    }
    if (placeholder) {
      opts.placeholder = placeholder
    }
    if (type === 'number') {
      if (valueSet(min)) {
        opts.min = min
      }
      if (valueSet(max)) {
        opts.max = max
      }
    }

    return opts
  })

  let focused = $state(false)

  let inputRef = $state<HTMLInputElement>()

  function onInputParentClick() {
    inputRef?.focus()
  }

  function onInputChange(e: Event) {
    const target = e.target as HTMLInputElement
    if (confirm) {
      localValue = target.value
    } else {
      value = target.value
    }
    // we always pass through the raw value here to aid validators, etc
    onchange?.({ name, type, value: target.value })
  }

  onMount(() => {
    onmount?.({ inputRef })
  })

  function doConfirm() {
    value = localValue
    onconfirm?.({ name, type, value })
  }

  function doReset() {
    localValue = value
  }

  function onInputKeyDown(e: KeyboardEvent) {
    if (!e) e = window.event as KeyboardEvent
    const keyCode = e.code || e.key
    if (confirm) {
      if (keyCode === 'Enter') {
        doConfirm()
        return false
      } else if (keyCode === 'Escape') {
        doReset()
        return false
      }
    }
    onkeydown?.(e)
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

<LabelContainer
  {testId}
  class={`tui-textinput${clazz ? ' ' + clazz : ''}`}
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  {name}
  {size}
  {disabled}
  {label}
  {labelAlignment}
  {labelPlacement}
  {required}
  {stretch}
>
  <FootnoteContainer {footnote} {error} {disabled} html={footnoteHtml}>
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <FocusRect {disabled} style={`--comp-focus-rect-border-radius:var(--border-radius)`}>
      <div
        role="button"
        tabindex="-1"
        class="input"
        class:disabled
        class:error={!valid || error !== ''}
        class:focused
        class:focusWidth={focusWidth !== -1}
        class:width={width !== -1}
        class:placeholder={placeholder && !localValue}
        onclick={onInputParentClick}
        onkeydown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault()
            onInputParentClick()
          }
        }}
      >
        {#if icon}
          <Icon
            name={icon}
            style="--icon-size:${inputSizeStr}-icon-size);--icon-margin:0 5px 0 -5px"
          />
        {/if}
        <input
          bind:this={inputRef}
          {type}
          {name}
          value={confirm ? localValue : value}
          {disabled}
          {...inputOpts}
          oninput={onInputChange}
          onfocus={() => onFocusAction('focus')}
          onblur={() => onFocusAction('blur')}
          onkeydown={onInputKeyDown}
          aria-labelledby={`${name}_label`}
        />
        {#if iconAfter}
          <Icon name={iconAfter} style="--icon-size:${inputSizeStr}-icon-size)" />
        {/if}
        {#if confirm && localValue !== value}
          <div class="confirm-row">
            <button
              class="confirm-icon"
              style="color: #6EC492; width: 20px; height: 20px;"
              onclick={doConfirm}
              type="button"
              aria-label="Confirm"
            >
              <Icon name="check" size={20} />
            </button>
            <button
              class="confirm-icon"
              style="color: #FF344C; width: 17px; height: 17px;"
              onclick={doReset}
              type="button"
              aria-label="Cancel"
            >
              <Icon name="close" size={17} />
            </button>
          </div>
        {/if}
      </div>
    </FocusRect>
  </FootnoteContainer>
</LabelContainer>

<style>
  .tui-textinput {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);
  }

  input {
    box-sizing: var(--box-sizing);

    outline: none;
    border: none;
    width: 100%;

    background-color: inherit;

    color: inherit;

    font-family: var(--font-family);
    font-size: var(--font-size);
    font-weight: var(--font-weight);
    line-height: var(--line-height);
    letter-spacing: var(--letter-spacing);
  }

  .input {
    box-sizing: var(--box-sizing);

    display: flex;
    align-items: center;
    padding: var(--padding);
    height: var(--height);

    border-width: var(--border-width);
    border-style: solid;
    border-radius: var(--border-radius);

    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);

    transition: width var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
  }
  .input.width {
    width: var(--width);
  }
  .input.focusWidth.focused {
    width: var(--focus-width);
  }

  .input.focused {
    color: var(--focus-color);
    background-color: var(--focus-bg-color);
    border-color: var(--focus-border-color);
  }

  .input:focus {
    opacity: var(--focus-opacity);
    color: var(--focus-color);
    background-color: var(--focus-bg-color);
    border-color: var(--focus-border-color);
  }
  .input:focus .icon {
    color: var(--focus-color);
  }

  .input:hover {
    opacity: var(--hover-opacity);
    color: var(--hover-color);
    background-color: var(--hover-bg-color);
    border-color: var(--hover-border-color);
  }
  .input:hover .icon {
    color: var(--hover-color);
  }
  .input:hover:focus {
    border-color: var(--focus-border-color);
  }
  .input:hover:focus .icon {
    color: var(--focus-color);
  }

  .input:active,
  .input.selected {
    opacity: var(--active-opacity);
    color: var(--active-color);
    background-color: var(--active-bg-color);
    border-color: var(--active-border-color);
  }
  .input:active .icon {
    color: var(--active-color);
  }
  .input:active:focus,
  .input.selected:focus {
    border-color: var(--focus-border-color);
  }
  .input:active:focus .icon {
    color: var(--focus-color);
  }

  .input.error,
  .input.error.focused {
    border-color: var(--invalid-border-color);
  }

  .confirm-row {
    display: flex;
    align-items: center;
    height: var(--height-local);
    gap: 4px;

    background-color: var(--comp-bg-color);
    z-index: 2;
    margin-left: -40px;
  }
  .confirm-icon {
    width: 18px;
    height: 18px;
    background: none;
    border: none;
    padding: 0;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .confirm-icon:hover {
    cursor: pointer;
  }

  .disabled,
  .disabled:active {
    background-color: var(--disabled-bg-color);
    border-color: var(--disabled-border-color);
  }
</style>
