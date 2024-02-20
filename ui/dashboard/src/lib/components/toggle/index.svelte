<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { tippy } from '$lib/stores/media'

  import {
    ComponentSize,
    type ComponentSizeType,
    ComponentVariant,
    type ComponentVariantType,
    FlexDirection,
    type FlexDirectionType,
    getStyleSizeFromComponentSize,
    getComponentSizeDown,
  } from '$lib/styles/types'

  import { FocusRect, Button } from '$lib/components'

  export let name = ''
  export let items: any = []
  export let value: any = null
  export let tabindex = -99
  export let disabled = false
  export let hasFocusRect = true
  export let round = false
  export let stretch = false

  let variant: ComponentVariantType = ComponentVariant.tool

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  const type = 'toggle'

  export let size: ComponentSizeType = ComponentSize.medium
  $: styleSize = getStyleSizeFromComponentSize(size)

  $: buttonComponentSize = getComponentSizeDown(size as ComponentSize)
  $: buttonStyleSize = getStyleSizeFromComponentSize(buttonComponentSize)

  $: compVarStr = `--comp-${variant}`
  $: toggleVarStr = `--toggle-${variant}`
  $: compSizeStr = `--comp-size-${styleSize}`
  $: toggleSizeStr = `--toggle-size-${styleSize}`

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--border-radius:${round ? '9999px' : `var(--toggle-border-radius, 6px)`}`,
      `--gap:var(--toggle-gap, 4px)`,
      `--padding-x:var(--toggle-padding-x, 3px)`,
      `--padding-y:var(--toggle-padding-y, 2px)`,
      `--icon-tab-border-radius:${round ? '9999px' : `var(--toggle-tab-border-radius, 4px)`}`,
      `--icon-size:var(--toggle-icon-size, 16px)`,
      `--height:var(${toggleSizeStr}-height, var(${compSizeStr}-height))`,
    ]
  }

  let focused = false

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

  let toggleRef

  let arrowFocusIndex = -1

  $: {
    for (let i = 0; i < items.length; i++) {
      if (items[i].value === value) {
        arrowFocusIndex = i
        break
      }
    }
  }

  function onSelect(val, index) {
    value = val
    arrowFocusIndex = index
    dispatch('change', { name, type, value })
    toggleRef.focus()
  }

  function onKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    switch (keyCode) {
      case 'ArrowLeft':
      case 'ArrowRight':
        e.preventDefault()

        arrowFocusIndex =
          keyCode === 'ArrowRight'
            ? (arrowFocusIndex + 1) % items.length
            : arrowFocusIndex === 0
              ? items.length - 1
              : (arrowFocusIndex - 1) % items.length

        return false
      case 'Enter':
      case 'Space':
        e.preventDefault()
        if (arrowFocusIndex !== -1) {
          ;(toggleRef.querySelectorAll('.tab')[arrowFocusIndex] as any).click()
        }
        return false
    }
  }
</script>

<FocusRect
  {disabled}
  show={hasFocusRect}
  borderRadius={round ? '9999px' : `var(--toggle-border-radius, 6px)`}
  {stretch}
>
  <div
    data-test-id={testId}
    class={`tui-toggle${clazz ? ' ' + clazz : ''}`}
    style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
    bind:this={toggleRef}
    on:focus={() => onFocusAction('focus')}
    on:blur={() => onFocusAction('blur')}
    on:keydown={onKeyDown}
    role="listbox"
    tabindex={tabindex === -99 ? 0 : tabindex}
    aria-label={name}
  >
    {#each items as item, i (item.value)}
      <div
        class="tab"
        class:selected={item.value === value}
        class:arrowFocused={arrowFocusIndex === i}
        on:click={() => onSelect(item.value, i)}
        on:keydown={() => {}}
        use:$tippy={{ content: item.tooltip }}
        role="option"
        aria-selected={item.value === value}
        aria-label={item.label}
        tabindex={-1}
      >
        <!-- <Icon name={item.icon} style="--icon-size:var(--toggle-icon-size, 16px)" /> -->
        {#if item.label}
          <Button
            tabindex={-1}
            icon={item.icon}
            size={buttonComponentSize}
            {variant}
            style={`--button-size-${buttonStyleSize}-icon-size:var(--toggle-icon-size, 16px)`}
            selected={item.value === value}
            emulateHover={arrowFocusIndex === i}>{item.label}</Button
          >
        {:else}
          <Button
            tabindex={-1}
            icon={item.icon}
            size={buttonComponentSize}
            {variant}
            style={`--button-size-${buttonStyleSize}-icon-size:var(--toggle-icon-size, 16px)`}
            ico={true}
            selected={item.value === value}
            emulateHover={arrowFocusIndex === i}
          />
        {/if}
      </div>
    {/each}
  </div>
</FocusRect>

<style>
  .tui-toggle {
    box-sizing: var(--box-sizing);

    display: flex;
    align-items: center;
    justify-content: center;
    gap: var(--gap);

    padding: var(--padding-y) var(--padding-y) !important;
    border-radius: var(--border-radius);
    height: var(--height);

    background: #33373c;
    outline: none;
  }

  .tui-toggle .tab {
    outline: none;
  }
</style>
