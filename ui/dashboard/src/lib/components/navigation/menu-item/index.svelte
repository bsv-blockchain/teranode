<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { tippy } from '$lib/stores/media'
  import Icon from '../../icon/index.svelte'

  export let selected = false
  export let collapsed = false
  export let icon
  export let iconSelected
  export let label

  const dispatch = createEventDispatcher()

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

  function dispatchClick(e) {
    dispatch('click', e.detail)
  }

  function onKeyDown(e) {
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

  let active = false

  $: currentIcon = selected || active || focused ? iconSelected : icon
</script>

{#key collapsed}
  <div
    class={`tui-menu-item${selected ? ' selected' : ''}${collapsed ? ' collapsed' : ''}`}
    tabindex="0"
    on:click={dispatchClick}
    on:keydown={onKeyDown}
    on:focus={() => onFocusAction('focus')}
    on:blur={() => onFocusAction('blur')}
    on:mouseenter={() => (active = true)}
    on:mouseleave={() => (active = false)}
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
    border: 1px solid rgba(255, 255, 255, 0.66);
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
