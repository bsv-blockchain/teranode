<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { FocusRect, Icon } from '$lib/components'
  import {
    ComponentSize,
    ComponentVariant,
    FlexDirection,
    getStyleSizeFromComponentSize,
  } from '$lib/styles/types'
  import type {
    ComponentSizeType,
    ComponentVariantType,
    FlexDirectionType,
  } from '$lib/styles/types'

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let variant: ComponentVariantType = ComponentVariant.primary
  export let icon: string | undefined | null = null
  export let iconAfter: string | undefined | null = null
  export let border = true

  let hasIcon = false
  let direction: FlexDirectionType = FlexDirection.row

  $: {
    hasIcon = Boolean(icon) || Boolean(iconAfter)

    if (hasIcon) {
      direction = icon ? FlexDirection.row : FlexDirection.rowReverse
    }
  }

  export let disabled = false
  export let selected = false
  export let toggle = false
  export let width = -1
  export let round = false
  export let hasFocusRect = true
  export let stretch = false

  export let size: ComponentSizeType = ComponentSize.medium
  $: styleSize = getStyleSizeFromComponentSize(size)

  $: compVarStr = `--comp-${variant}`
  $: tabVarStr = `--tab-default`
  $: compSizeStr = `--comp-size-${styleSize}`
  $: tabSizeStr = `--tab-size-${styleSize}`

  let cssVars: string[] = []
  $: {
    let states = ['enabled', 'hover', 'selected', 'focus', 'disabled']
    cssVars = [
      ...states.reduce(
        (acc, state) => [
          ...acc,
          `--${state}-color:var(${tabVarStr}-${state}-color, var(${compVarStr}-${state}-color))`,
          `--${state}-bg-color:var(${tabVarStr}-${state}-bg-color, var(${compVarStr}-${state}-bg-color))`,
          `--${state}-border-color:var(${tabVarStr}-${state}-border-color, var(${compVarStr}-${state}-border-color))`,
        ],
        [] as string[],
      ),
      `--height:var(${tabSizeStr}-height, var(${compSizeStr}-height))`,
      `--padding:var(${tabSizeStr}-padding, var(${compSizeStr}-padding))`,
      `--border-radius:var(${tabSizeStr}-border-radius, var(${compSizeStr}-border-radius))`,
      `--border-radius:${
        round ? '9999px' : `var(${tabSizeStr}-border-radius, var(${compSizeStr}-border-radius))`
      }`,
      `--icon-size:var(${tabSizeStr}-icon-size, var(${compSizeStr}-icon-size))`,
      `--font-size:var(${tabSizeStr}-font-size, var(${compSizeStr}-font-size))`,
      `--line-height:var(${tabSizeStr}-line-height, var(${compSizeStr}-line-height))`,
      `--letter-spacing:var(${tabSizeStr}-letter-spacing, var(${compSizeStr}-letter-spacing))`,
      `--font-weight:var(--button-font-weight, var(--comp-font-weight))`,
      `--border-width:var(--button-border-width, var(--comp-border-width))`,
      `--gap:var(--button-icon-gap, 6px)`,
      `--min-width:var(${tabSizeStr}-height, var(${compSizeStr}-height))`,
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

  function onKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    if (keyCode === 'Enter') {
      dispatch('click', e)
      return false
    }
    dispatch('keydown', e)
  }
</script>

<FocusRect
  {disabled}
  show={hasFocusRect && !selected}
  borderRadius={round
    ? '9999px'
    : `var(${tabSizeStr}-border-radius, var(${compSizeStr}-border-radius))`}
  {stretch}
  style="--focus-rect-bg-color:transparent;"
>
  <!-- svelte-ignore a11y-click-events-have-key-events -->
  <!-- svelte-ignore a11y-no-noninteractive-tabindex -->
  <div
    data-test-id={testId}
    class={`tui-tab${clazz ? ' ' + clazz : ''}`}
    style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
    class:disabled
    class:selected
    class:noborder={!border}
    class:toggle
    style:--direction={direction}
    style:--width={width === -1 ? 'auto' : `${width}px`}
    tabindex="0"
    on:click
    on:focus={() => onFocusAction('focus')}
    on:blur={() => onFocusAction('blur')}
    on:keydown={onKeyDown}
  >
    {#if hasIcon}
      <Icon
        name={icon || iconAfter}
        style={`--width:var(${tabSizeStr}-icon-size, var(${compSizeStr}-icon-size));--height:var(${tabSizeStr}-icon-size, var(${compSizeStr}-icon-size))`}
      />
    {/if}
    {#if $$slots.default}
      <div class="label"><slot /></div>
    {/if}
  </div>
</FocusRect>

<style>
  .tui-tab {
    display: flex;
    flex-direction: var(--direction);
    align-items: center;
    justify-content: center;
    gap: var(--gap);

    outline: none;
    box-sizing: var(--box-sizing);

    width: var(--width);
    min-width: var(--min-width);
    min-height: var(--height);
    max-height: var(--height);
    padding: var(--padding) !important;

    font-family: var(--font-family);
    font-size: var(--font-size);
    font-weight: var(--font-weight);
    line-height: var(--line-height);
    letter-spacing: var(--letter-spacing);

    border-width: var(--border-width);
    border-style: solid;
    border-radius: var(--border-radius);
    cursor: pointer;
    user-select: none;

    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);

    transition:
      color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out),
      background-color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
  }
  .tui-tab.noborder {
    /* border-color: transparent; */
  }

  .tui-tab:focus {
    color: var(--focus-color);
    background-color: var(--focus-bg-color);
    border-color: var(--focus-border-color);
  }
  .tui-tab.noborder:focus {
    /* border-color: transparent; */
  }

  .tui-tab:hover {
    color: var(--hover-color);
    background-color: var(--hover-bg-color);
    border-color: var(--hover-border-color);
  }
  .tui-tab:hover:focus {
    border-color: var(--focus-border-color);
  }
  .tui-tab.noborder:hover,
  .tui-tab.noborder:hover:focus {
    /* border-color: transparent; */
  }

  .tui-tab:active {
    color: var(--active-color);
    background-color: var(--active-bg-color);
    border-color: var(--active-border-color);
  }
  .tui-tab:active:focus {
    border-color: var(--focus-border-color);
  }
  .tui-tab.noborder:active,
  .tui-tab.noborder:active:focus {
    border-color: transparent;
  }

  .tui-tab.selected,
  .tui-tab:hover.selected {
    color: var(--selected-color);
    background-color: var(--selected-bg-color);
    border-color: var(--selected-border-color);
  }
  .tui-tab.noborder.selected {
    /* border-color: transparent; */
  }

  .tui-tab:disabled,
  .tui-tab.disabled {
    color: var(--disabled-color);
    background-color: var(--disabled-bg-color);
    border-color: var(--disabled-border-color);
    cursor: auto;
    pointer-events: none;
  }
  .tui-tab.noborder:disabled,
  .tui-tab.noborder.disabled {
    border-color: transparent;
  }

  .tui-tab .label {
    display: flex;
    align-items: center;
    white-space: nowrap;
  }
</style>
