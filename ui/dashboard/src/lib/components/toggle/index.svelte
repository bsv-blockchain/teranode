<svelte:options runes={true} />

<script lang="ts">
  import { tippy } from '$lib/stores/media'

  import {
    ComponentSize,
    type ComponentSizeType,
    ComponentVariant,
    type ComponentVariantType,
    getStyleSizeFromComponentSize,
    getComponentSizeDown,
  } from '$lib/styles/types'

  import { FocusRect, Button } from '$lib/components'

  let {
    name = '',
    items = [],
    value = $bindable(null),
    tabindex = -99,
    disabled = false,
    hasFocusRect = true,
    round = false,
    stretch = false,
    testId = null,
    class: clazz = null,
    style = '',
    size = ComponentSize.medium,
    onchange,
    onfocus,
    onblur,
  }: {
    name?: string
    items?: any
    value?: any
    tabindex?: number
    disabled?: boolean
    hasFocusRect?: boolean
    round?: boolean
    stretch?: boolean
    testId?: string | undefined | null
    class?: string | undefined | null
    style?: string
    size?: ComponentSizeType
    onchange?: (e: { name: string; type: string; value: any }) => void
    onfocus?: () => void
    onblur?: () => void
  } = $props()

  const variant: ComponentVariantType = ComponentVariant.tool

  const type = 'toggle'

  const styleSize = $derived(getStyleSizeFromComponentSize(size))

  const buttonComponentSize = $derived(getComponentSizeDown(size as ComponentSize))
  const buttonStyleSize = $derived(getStyleSizeFromComponentSize(buttonComponentSize))

  const compVarStr = $derived(`--comp-${variant}`)
  const toggleVarStr = $derived(`--toggle-${variant}`)
  const compSizeStr = $derived(`--comp-size-${styleSize}`)
  const toggleSizeStr = $derived(`--toggle-size-${styleSize}`)

  const cssVars = $derived.by(() => [
    `--border-radius:${round ? '9999px' : `var(--toggle-border-radius, 6px)`}`,
    `--gap:var(--toggle-gap, 4px)`,
    `--padding-x:var(--toggle-padding-x, 3px)`,
    `--padding-y:var(--toggle-padding-y, 2px)`,
    `--icon-tab-border-radius:${round ? '9999px' : `var(--toggle-tab-border-radius, 4px)`}`,
    `--icon-size:var(--toggle-icon-size, 16px)`,
    `--height:var(${toggleSizeStr}-height, var(${compSizeStr}-height))`,
  ])

  let focused = false

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

  let toggleRef = $state<HTMLElement>()

  let arrowFocusIndex = $state(-1)

  $effect(() => {
    for (let i = 0; i < items.length; i++) {
      if (items[i].value === value) {
        arrowFocusIndex = i
        break
      }
    }
  })

  function onSelect(val: any, index: number) {
    value = val
    arrowFocusIndex = index
    onchange?.({ name, type, value })
    toggleRef?.focus()
  }

  function onKeyDown(e: KeyboardEvent) {
    if (!e) e = window.event as KeyboardEvent
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
          ;(toggleRef?.querySelectorAll('.tab')[arrowFocusIndex] as any).click()
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
    onfocus={() => onFocusAction('focus')}
    onblur={() => onFocusAction('blur')}
    onkeydown={onKeyDown}
    role="listbox"
    tabindex={tabindex === -99 ? 0 : tabindex}
    aria-label={name}
  >
    {#each items as item, i (item.value)}
      <div
        class="tab"
        class:selected={item.value === value}
        class:arrowFocused={arrowFocusIndex === i}
        onclick={() => onSelect(item.value, i)}
        onkeydown={() => {}}
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

    background: var(--toggle-bg-color, #33373c);
    outline: none;
  }

  .tui-toggle .tab {
    outline: none;
  }
</style>
