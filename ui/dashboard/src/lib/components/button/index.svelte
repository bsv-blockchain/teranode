<svelte:options runes={true} />

<script lang="ts">
  import type { Snippet } from 'svelte'
  import { FocusRect, Icon } from '$lib/components'
  import { tippy } from '$lib/stores/media'
  import {
    ComponentSize,
    type ComponentSizeType,
    ComponentVariant,
    type ComponentVariantType,
    FlexDirection,
    getStyleSizeFromComponentSize,
  } from '$lib/styles/types'

  let {
    testId = null,
    class: clazz = null,
    style = '',
    variant = ComponentVariant.primary,
    icon = null,
    iconColor = 'currentColor',
    iconAfter = null,
    tabindex = -99,
    hasFocusRect = true,
    emulateHover = false,
    tooltip = '',
    disabled = false,
    selected = false,
    toggle = false,
    width = -1,
    round = false,
    ico = false,
    stretch = false,
    uppercase = false,
    size = ComponentSize.medium,
    onclick,
    onfocus,
    onblur,
    onkeydown,
    children,
  }: {
    testId?: string | null
    class?: string | null
    style?: string
    variant?: ComponentVariantType
    icon?: string | null
    iconColor?: string
    iconAfter?: string | null
    tabindex?: number
    hasFocusRect?: boolean
    emulateHover?: boolean
    tooltip?: string
    disabled?: boolean
    selected?: boolean
    toggle?: boolean
    width?: number
    round?: boolean
    ico?: boolean
    stretch?: boolean
    uppercase?: boolean
    size?: ComponentSizeType
    onclick?: (e: MouseEvent) => void
    onfocus?: () => void
    onblur?: () => void
    onkeydown?: (e: KeyboardEvent) => void
    children?: Snippet
  } = $props()

  const hasIcon = $derived(Boolean(icon) || Boolean(iconAfter))
  const direction = $derived(
    hasIcon ? (icon ? FlexDirection.row : FlexDirection.rowReverse) : FlexDirection.row,
  )

  const styleSize = $derived(getStyleSizeFromComponentSize(size))

  const compVarStr = $derived(`--comp-${variant}`)
  const buttonVarStr = $derived(`--button-${variant}`)
  const compSizeStr = $derived(`--comp-size-${styleSize}`)
  const buttonSizeStr = $derived(`--button-size-${styleSize}`)

  const cssVars = $derived.by(() => {
    const states = ['enabled', 'hover', 'active', 'focus', 'disabled']
    return [
      ...states.reduce(
        (acc, state) => [
          ...acc,
          `--${state}-opacity:var(${buttonVarStr}-${state}-opacity, var(${compVarStr}-${state}-opacity, 1))`,
          `--${state}-color:var(${buttonVarStr}-${state}-color, var(${compVarStr}-${state}-color))`,
          `--${state}-bg-color:var(${buttonVarStr}-${state}-bg-color, var(${compVarStr}-${state}-bg-color))`,
          `--${state}-border-color:var(${buttonVarStr}-${state}-border-color, var(${compVarStr}-${state}-border-color))`,
        ],
        [] as string[],
      ),
      `--height:var(${buttonSizeStr}-height, var(${compSizeStr}-height))`,
      `--padding:${
        ico
          ? `var(${compSizeStr}-ico-padding)`
          : `var(${buttonSizeStr}-padding, var(${compSizeStr}-padding))`
      }`,
      // `--ico-margin:${ico ? `var(${compSizeStr}-ico-margin)` : `0`}`,
      `--border-radius:${
        round ? '9999px' : `var(${buttonSizeStr}-border-radius, var(${compSizeStr}-border-radius))`
      }`,
      `--icon-size:var(${buttonSizeStr}-icon-size, var(${compSizeStr}-icon-size))`,
      `--font-size:var(${buttonSizeStr}-font-size, var(${compSizeStr}-font-size))`,
      `--line-height:var(${buttonSizeStr}-line-height, var(${compSizeStr}-line-height))`,
      `--letter-spacing:var(${buttonSizeStr}-letter-spacing, var(${compSizeStr}-letter-spacing))`,
      `--font-weight:var(--button-font-weight, var(--comp-font-weight))`,
      `--border-width:var(--button-border-width, var(--comp-border-width))`,
      `--gap:var(--button-icon-gap, 6px)`,
    ]
  })

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

  function onKeyDown(e: KeyboardEvent) {
    if (!e) e = window.event as KeyboardEvent
    const keyCode = e.code || e.key
    if (keyCode === 'Enter') {
      onclick?.(e as unknown as MouseEvent)
      return false
    }
    onkeydown?.(e)
  }
</script>

<FocusRect
  {disabled}
  show={hasFocusRect}
  borderRadius={round
    ? '9999px'
    : `var(${buttonSizeStr}-border-radius, var(${compSizeStr}-border-radius))`}
  {stretch}
>
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
  <div
    role="button"
    data-test-id={testId}
    class={`tui-button${clazz ? ' ' + clazz : ''}`}
    style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
    class:disabled
    class:selected
    class:emulateHover
    class:toggle
    class:round
    class:uppercase
    style:--direction={direction}
    style:--width={width === -1 ? 'auto' : `${width}px`}
    tabindex={tabindex === -99 ? 0 : tabindex}
    onclick={onclick}
    onfocus={() => onFocusAction('focus')}
    onblur={() => onFocusAction('blur')}
    onkeydown={onKeyDown}
    use:$tippy={{ content: tooltip }}
  >
    {#if hasIcon}
      <div class="icon">
        <Icon
          name={icon || iconAfter}
          color={iconColor}
          style={`--width:var(${buttonSizeStr}-icon-size, var(${compSizeStr}-icon-size));--height:var(${buttonSizeStr}-icon-size, var(${compSizeStr}-icon-size))`}
        />
      </div>
    {/if}
    {#if children}
      <div class="label">{@render children()}</div>
    {/if}
  </div>
</FocusRect>

<style>
  .tui-button {
    display: flex;
    flex-direction: var(--direction);
    align-items: center;
    justify-content: center;
    gap: var(--gap);

    outline: none;
    box-sizing: var(--box-sizing);

    width: var(--width);
    min-height: var(--height);
    max-height: var(--height);
    padding: var(--padding);
    margin: 0;

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

    opacity: var(--enabled-opacity);
    color: var(--enabled-color);
    background-color: var(--enabled-bg-color);
    border-color: var(--enabled-border-color);

    transition:
      color 0.1s linear,
      background-color 0.1s linear;
  }
  .tui-button.round {
    border-radius: 9999px;
  }
  .tui-button.uppercase {
    text-transform: uppercase;
  }
  .tui-button .icon {
    color: var(--enabled-color);
    margin: var(--ico-margin) !important;
  }

  .tui-button:focus {
    opacity: var(--focus-opacity);
    color: var(--focus-color);
    background-color: var(--focus-bg-color);
    border-color: var(--focus-border-color);
  }
  .tui-button:focus .icon {
    color: var(--focus-color);
  }

  .tui-button:hover,
  .tui-button.emulateHover {
    opacity: var(--hover-opacity);
    color: var(--hover-color);
    background-color: var(--hover-bg-color);
    border-color: var(--hover-border-color);
  }
  .tui-button:hover .icon,
  .tui-button.emulateHover .icon {
    color: var(--hover-color);
  }
  .tui-button:hover:focus,
  .tui-button.emulateHover:focus {
    border-color: var(--focus-border-color);
  }
  .tui-button:hover:focus .icon,
  .tui-button.emulateHover:focus .icon {
    color: var(--focus-color);
  }

  .tui-button:active,
  .tui-button.selected {
    opacity: var(--active-opacity);
    color: var(--active-color);
    background-color: var(--active-bg-color);
    border-color: var(--active-border-color);
  }
  .tui-button:active .icon,
  .tui-button.selected .icon {
    color: var(--active-color);
  }
  .tui-button:active:focus,
  .tui-button.selected:focus {
    border-color: var(--focus-border-color);
  }
  .tui-button:active:focus .icon,
  .tui-button.selected:focus .icon {
    color: var(--focus-color);
  }

  .tui-button:disabled,
  .tui-button.disabled {
    opacity: var(--disabled-opacity);
    color: var(--disabled-color);
    background-color: var(--disabled-bg-color);
    border-color: var(--disabled-border-color);
    cursor: auto;
    pointer-events: none;
  }
  .tui-button:disabled .icon,
  .tui-button.disabled .icon {
    color: var(--disabled-color);
  }

  .tui-button .label {
    display: flex;
    align-items: center;
    white-space: nowrap;
  }
</style>
