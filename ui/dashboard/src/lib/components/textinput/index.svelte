<script lang="ts">
  import { createEventDispatcher, onMount } from 'svelte'
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

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let type: 'text' | 'number' = 'text'
  export let min: number | null = null
  export let max: number | null = null

  export let label: any = ''
  export let footnote: any = ''
  export let footnoteHtml = true
  export let required = false
  export let name = ''
  export let value = ''
  export let placeholder: any = null
  export let disabled = false
  export let valid = true
  export let error = ''
  export let autocomplete: string | undefined | null = 'off'
  export let stretch = false
  export let width = -1
  export let focusWidth = -1
  export let icon: string | undefined | null = null
  export let iconAfter: string | undefined | null = null

  export let labelPlacement: LabelPlacementType = LabelPlacement.top
  export let labelAlignment: LabelAlignmentType = LabelAlignment.start

  export let size: InputSizeType = ComponentSize.medium
  $: styleSize = getStyleSizeFromComponentSize(size)

  $: inputVarStr = `--input-default`
  $: inputSizeStr = `--input-size-${styleSize}`

  $: placeholderActive = placeholder && !localValue

  let cssVars: string[] = []
  $: {
    let states = ['enabled', 'hover', 'active', 'focus', 'disabled']
    cssVars = [
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
  }

  // in confirm mode, changes are local until confirm is clicked,
  // or alternatively changes can be reset to previous non-local value
  export let confirm = false
  let localValue = value

  $: {
    localValue = value
  }

  let inputOpts: any = {}

  $: {
    inputOpts = {}

    if (autocomplete) {
      inputOpts.autocomplete = autocomplete
    }
    if (placeholder) {
      inputOpts.placeholder = placeholder
    }
    if (type === 'number') {
      if (valueSet(min)) {
        inputOpts.min = min
      }
      if (valueSet(max)) {
        inputOpts.max = max
      }
    }
  }

  let focused = false

  let inputRef

  function onInputParentClick() {
    inputRef.focus()
  }

  function onInputChange(e) {
    if (confirm) {
      localValue = e.srcElement.value
    } else {
      value = e.srcElement.value
    }
    // we always pass through the raw value here to aid validators, etc
    dispatch('change', { name, type, value: e.srcElement.value })
  }

  onMount(() => {
    dispatch('mount', { inputRef })
  })

  function doConfirm() {
    value = localValue
    dispatch('confirm', { name, type, value })
  }

  function doReset() {
    localValue = value
  }

  function onKeyDown(e) {
    if (!e) e = window.event
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
    dispatch('keydown', e)
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
    <!-- svelte-ignore a11y-click-events-have-key-events -->
    <FocusRect {disabled} style={`--comp-focus-rect-border-radius:var(--border-radius)`}>
      <div
        class="input"
        class:disabled
        class:error={!valid || error !== ''}
        class:focused
        class:focusWidth={focusWidth !== -1}
        class:width={width !== -1}
        class:placeholder={placeholder && !localValue}
        on:click={onInputParentClick}
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
          on:input={onInputChange}
          on:focus={() => onFocusAction('focus')}
          on:blur={() => onFocusAction('blur')}
          on:keydown={onKeyDown}
          aria-labelledby={`${name}_label`}
        />
        {#if iconAfter}
          <Icon name={iconAfter} style="--icon-size:${inputSizeStr}-icon-size)" />
        {/if}
        {#if confirm && localValue !== value}
          <div class="confirm-row">
            <div
              class="confirm-icon"
              style="color: #6EC492; width: 20px; height: 20px;"
              on:click={doConfirm}
            >
              <Icon name="check" size={20} />
            </div>
            <div
              class="confirm-icon"
              style="color: #FF344C; width: 17px; height: 17px;"
              on:click={doReset}
            >
              <Icon name="close" size={17} />
            </div>
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

    transition: width var(--easing-duration, 0.2s) var(--easing-function, var);
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

    background-color: rgba(255, 255, 255, 0.8);
    z-index: 2;
    margin-left: -40px;
  }
  .confirm-icon {
    width: 18px;
    height: 18px;
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
