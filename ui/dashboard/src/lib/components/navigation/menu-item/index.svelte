<svelte:options runes={true} />

<script lang="ts">
  import { tippy } from '$lib/stores/media'
  import Icon from '../../icon/index.svelte'

  let {
    selected = false,
    collapsed = false,
    icon,
    iconSelected,
    label,
    onclick,
    onfocus,
    onblur,
  }: {
    selected?: boolean
    collapsed?: boolean
    icon?: any
    iconSelected?: any
    label?: any
    onclick?: (detail?: any) => void
    onfocus?: () => void
    onblur?: () => void
  } = $props()

  let focused = $state(false)

  function onFocusAction(eventName: 'focus' | 'blur') {
    switch (eventName) {
      case 'blur':
        focused = false
        onblur?.()
        break
      case 'focus':
        focused = true
        onfocus?.()
        break
    }
  }

  function dispatchClick(e: any) {
    onclick?.(e?.detail)
  }

  function onKeyDown(e: any) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    switch (keyCode) {
      case 'Space':
        e.preventDefault()
        if (focused) {
          dispatchClick(e)
        }
        return false
    }
  }

  let active = $state(false)

  const currentIcon = $derived(selected || active || focused ? iconSelected : icon)
</script>

{#key collapsed}
  <div
    role="menuitem"
    class={`tui-menu-item${selected ? ' selected' : ''}${collapsed ? ' collapsed' : ''}`}
    tabindex="0"
    onclick={dispatchClick}
    onkeydown={onKeyDown}
    onfocus={() => onFocusAction('focus')}
    onblur={() => onFocusAction('blur')}
    onmouseenter={() => (active = true)}
    onmouseleave={() => (active = false)}
    use:$tippy={{ content: collapsed ? label : null, offset: [0, 0] }}
  >
    {#if icon}
      <div class="icon">
        <Icon name={currentIcon} size={18} />
      </div>
    {/if}
    {#if !collapsed}
      <div class="label">
        {label}
      </div>
    {/if}
  </div>
{/key}

<style>
  .tui-menu-item {
    display: flex;
    align-items: center;
    justify-content: flex-start;
    gap: 8px;
    padding: 0px 10px;
    height: 40px;

    color: var(--comp-color);
    margin: 0;
    border: none;
    border-radius: 8px;

    font-family: var(--font-family);
    font-weight: 400;
    font-size: 15px;
    font-style: normal;
    outline: none;
  }
  .icon {
    width: 18px;
    height: 18px;
  }
  .label {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .tui-menu-item:hover {
    cursor: pointer;
  }

  .tui-menu-item.collapsed {
    /* justify-content: center; */
    padding: 0 10px;
  }

  .tui-menu-item .icon,
  .tui-menu-item .label {
    opacity: 0.66;
  }

  .tui-menu-item:hover {
    font-weight: 700;
  }
  .tui-menu-item:hover .icon,
  .tui-menu-item:hover .label {
    opacity: 0.88;
  }

  .tui-menu-item:focus {
    font-weight: 700;
    margin: -1px;
    border: 1px solid var(--comp-label-color);
  }
  .tui-menu-item:focus .icon,
  .tui-menu-item:focus .label {
    opacity: 0.88;
  }

  .tui-menu-item.selected {
    font-weight: 700;
    margin: 0;
    border: none;
  }
  .tui-menu-item.selected .icon,
  .tui-menu-item.selected .label {
    opacity: 0.88;
  }
</style>
