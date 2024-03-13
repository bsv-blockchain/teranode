<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { fade, slide } from 'svelte/transition'
  import { clickOutside } from '../../actions'
  import { FootnoteContainer, Icon, LabelContainer, FocusRect } from '$lib/components'
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
  export let value: any = undefined
  export let multiple = false
  export let items: { value: any; label: string }[] = []
  export let disabled = false
  export let valid = true
  export let error = ''
  export let maxVisibleListItems = -1
  export let expandUp = false
  export let stretch = false

  let type = 'select'

  let open = false

  export let labelPlacement: LabelPlacementType = LabelPlacement.top
  export let labelAlignment: LabelAlignmentType = LabelAlignment.start

  export let size: InputSizeType = ComponentSize.medium
  $: styleSize = getStyleSizeFromComponentSize(size)

  $: inputVarStr = `--input-default`
  $: inputSizeStr = `--input-size-${styleSize}`

  let cssVars: string[] = []
  $: {
    let states = ['enabled', 'hover', 'active', 'focus', 'disabled']
    cssVars = [
      ...states.reduce(
        (acc, state) => [
          ...acc,
          `--${state}-color:var(${inputVarStr}-${state}-color)`,
          `--${state}-bg-color:var(${inputVarStr}-${state}-bg-color)`,
          `--${state}-border-color:var(${inputVarStr}-${state}-border-color)`,
        ],
        [] as string[],
      ),
      `--invalid-border-color:var(--input-default-invalid-border-color)`,
      `--height:var(${inputSizeStr}-height)`,
      `--padding:var(${inputSizeStr}-padding)`,
      `--border-radius:var(${inputSizeStr}-border-radius)`,
      `--border-color:var(--dropdown-list-border-color)`,
      `--icon-size:var(${inputSizeStr}-icon-size)`,
      `--font-size:var(${inputSizeStr}-font-size)`,
      `--line-height:var(${inputSizeStr}-line-height)`,
      `--letter-spacing:var(${inputSizeStr}-letter-spacing)`,
      `--font-weight:var(--input-font-weight)`,
      `--border-width:var(--comp-border-width)`,
      `--gap:var(--button-icon-gap, 6px)`,
      `--list-padding:var(--dropdown-list-padding)`,
      `--list-bg-color:var(--dropdown-list-bg-color)`,
      `--list-border-radius:var(--dropdown-list-border-radius)`,
      `--list-box-shadow:var(--dropdown-list-box-shadow)`,
      `--list-item-padding:var(--dropdown-size-${styleSize}-list-item-padding)`,
      `--list-item-enabled-bg-color:var(--dropdown-list-item-enabled-bg-color)`,
      `--list-item-hover-bg-color:var(--dropdown-list-item-hover-bg-color)`,
      `--list-item-selected-bg-color:var(--dropdown-list-item-selected-bg-color)`,
      `--list-bottom:${expandUp ? `var(${inputSizeStr}-height)` : 'auto'}`,
      `--list-top:${expandUp ? `auto` : '0'}`,
    ]
    if (maxVisibleListItems === -1) {
      cssVars.push(`--list-height:calc(${items.length} * var(${inputSizeStr}-height))`)
    } else {
      cssVars.push(
        `--list-height:calc(min(calc(${items.length} * var(${inputSizeStr}-height)), calc(${maxVisibleListItems} * var(${inputSizeStr}-height))))`,
      )
    }
  }

  let focused = false

  let selectRef

  function onSelectParentClick() {
    selectRef.focus()
    open = !open
    if (open) {
      const result = items.filter((item) => item.value === value)
      if (result && result.length > 0) {
        arrowFocusIndex = items.indexOf(result[0])
      } else {
        arrowFocusIndex = 0
      }
    }
  }

  function onSelectChange(e) {
    arrowFocusIndex = -1
    value = items[e.srcElement.selectedIndex].value
    dispatch('change', { name, type, value })
    selectRef.focus()
  }

  function onClose() {
    open = false
  }

  function onItemSelect(val) {
    value = val
    open = false
    arrowFocusIndex = -1
    dispatch('change', { name, type, value })
    selectRef.focus()
  }

  let arrowFocusIndex = -1

  function onKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    switch (keyCode) {
      case 'Space':
        e.preventDefault()
        onSelectParentClick()
        return false
      case 'ArrowDown':
      case 'ArrowUp':
        e.preventDefault()
        if (open) {
          arrowFocusIndex =
            keyCode === 'ArrowDown'
              ? (arrowFocusIndex + 1) % items.length
              : arrowFocusIndex === 0
                ? items.length - 1
                : (arrowFocusIndex - 1) % items.length
        }
        return false
      case 'Enter':
        e.preventDefault()
        if (open && arrowFocusIndex !== -1) {
          ;(document.querySelectorAll('.list-item')[arrowFocusIndex] as any).click()
        }
        return false
    }
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
  {name}
  {size}
  {disabled}
  {label}
  {labelAlignment}
  {labelPlacement}
  {required}
  {stretch}
>
  <FootnoteContainer {footnote} {error} {disabled}>
    <div
      data-test-id={testId}
      class={`tui-dropdown${clazz ? ' ' + clazz : ''}`}
      style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
      class:open
      class:expandUp
      class:stretch
      use:clickOutside
      on:outclick={onClose}
      on:focus={(e) => (focused = true)}
    >
      <FocusRect {disabled} style={`--comp-focus-rect-border-radius:var(--border-radius)`}>
        <!-- svelte-ignore a11y-click-events-have-key-events -->
        <div
          class="select"
          class:disabled
          class:error={!valid || error !== ''}
          class:focused
          on:click={disabled ? null : onSelectParentClick}
        >
          <select
            bind:this={selectRef}
            {multiple}
            {disabled}
            {name}
            {value}
            on:change={onSelectChange}
            on:focus={() => onFocusAction('focus')}
            on:blur={() => onFocusAction('blur')}
            on:keydown={onKeyDown}
            aria-labelledby={`${name}_label`}
          >
            {#each items as item (item.value)}
              <option value={item.value}>
                {item.label}
              </option>
            {/each}
          </select>
          <div class="icon">
            <Icon
              class="icon"
              name="chevron-down"
              style={`--width:var(${inputSizeStr}-icon-size);--height:var(${inputSizeStr}-icon-size)`}
            />
          </div>
        </div>
        {#if open}
          <div class="list-container">
            <div
              in:slide
              out:fade
              class="list"
              style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
            >
              <div class="options-container">
                {#each items as item, i (item.value)}
                  <!-- svelte-ignore a11y-click-events-have-key-events -->
                  <div
                    class="list-item"
                    class:selected={item.value === value}
                    class:arrowFocused={arrowFocusIndex === i}
                    on:click={() => onItemSelect(item.value)}
                  >
                    <div>{item.label}</div>
                  </div>
                {/each}
              </div>
            </div>
          </div>
        {/if}
      </FocusRect>
    </div>
  </FootnoteContainer>
</LabelContainer>

<style>
  .tui-dropdown {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    display: flex;
    flex-direction: column;
  }

  .icon {
    width: var(--icon-size);
    height: var(--icon-size);
    transition: transform var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
  }
  .tui-dropdown .icon {
    transform: rotate(0deg);
  }
  .tui-dropdown.expandUp .icon {
    transform: rotate(180deg);
  }
  .tui-dropdown.open .icon {
    transform: rotate(180deg);
  }
  .tui-dropdown.expandUp.open .icon {
    transform: rotate(0deg);
  }

  .list-container {
    position: relative;
    z-index: 200;
  }
  .list {
    position: absolute;
    width: 100%;
    overflow: hidden;
    top: var(--list-top);
    bottom: var(--list-bottom);

    box-sizing: var(--box-sizing);
    /* padding: var(--list-padding); */

    background-color: var(--list-bg-color);
    border-style: solid;
    border-width: var(--border-width);
    border-color: var(--border-color);
    border-radius: var(--list-border-radius);
    box-shadow: var(--dropdown-list-box-shadow);
  }
  .options-container {
    height: var(--list-height);
    overflow-x: hidden;
    overflow-y: auto;
  }
  .list-item {
    display: flex;
    align-items: center;
    width: calc(100% - 2px);
    height: var(--height);
    padding: var(--list-item-padding);

    font-family: var(--font-family);
    font-size: var(--font-size);
    font-weight: var(--font-weight);
    line-height: var(--line-height);
    letter-spacing: var(--letter-spacing);

    border-radius: var(--border-radius);

    color: var(--enabled-color);
    background-color: var(--list-item-enabled-bg-color);

    cursor: pointer;
  }
  .list-item > div {
    margin: var(--list-item-padding);
  }
  .list-item:hover,
  .list-item:focus,
  .list-item.arrowFocused {
    background-color: var(--list-item-hover-bg-color);
    outline: none;
  }
  .list-item.selected {
    background-color: var(--list-item-selected-bg-color);
  }

  select {
    box-sizing: var(--box-sizing);

    outline: none;
    border: none;
    width: calc(100% - var(--icon-size));

    background-color: inherit;
    appearance: none;
    pointer-events: none;

    color: inherit;
    font-family: var(--font-family);
    font-size: var(--font-size);
    font-weight: var(--font-weight);
    line-height: var(--line-height);
    letter-spacing: var(--letter-spacing);
  }

  .select {
    box-sizing: var(--box-sizing);

    display: flex;
    align-items: center;
    padding: var(--padding);
    height: var(--height);

    font-family: var(--font-family);
    font-size: var(--font-size);
    font-weight: var(--font-weight);
    line-height: var(--line-height);
    letter-spacing: var(--letter-spacing);

    border-width: var(--border-width);
    border-style: solid;
    border-radius: var(--border-radius);

    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);

    cursor: pointer;
  }
  .select.disabled {
    cursor: auto;
  }

  .select.focused,
  .select:focus {
    color: var(--focus-color);
    background-color: var(--focus-bg-color);
    border-color: var(--focus-border-color);
  }
  .tui-dropdown.open .select.focused,
  .tui-dropdown.open .select:focus {
    border-color: var(--enabled-border-color);
  }

  .select:focus .icon {
    color: var(--focus-color);
  }

  .select:hover {
    opacity: var(--hover-opacity);
    color: var(--hover-color);
    background-color: var(--hover-bg-color);
    border-color: var(--hover-border-color);
  }
  .select:hover .icon {
    color: var(--hover-color);
  }
  .select:hover:focus {
    border-color: var(--focus-border-color);
  }
  .select:hover:focus .icon {
    color: var(--focus-color);
  }

  .select:active,
  .select.selected {
    opacity: var(--active-opacity);
    color: var(--active-color);
    background-color: var(--active-bg-color);
    border-color: var(--active-border-color);
  }
  .select:active .icon {
    color: var(--active-color);
  }
  .select:active:focus,
  .select.selected:focus {
    border-color: var(--focus-border-color);
  }
  .select:active:focus .icon {
    color: var(--focus-color);
  }

  .select.error,
  .select.error.focused {
    border-color: var(--invalid-border-color);
  }

  .disabled,
  .disabled:active {
    background-color: var(--disabled-bg-color);
    border-color: var(--disabled-border-color);
  }
  .disabled select {
    background-color: var(--disabled-bg-color);
  }
</style>
