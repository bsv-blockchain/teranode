<script lang="ts">
  import { onDestroy } from 'svelte'
  import { createEventDispatcher } from 'svelte'
  import { Icon } from '$lib/components'

  export let size = 18
  export let icon = ''
  export let iconSuccess = 'icon-check-line'
  export let iconFailure = 'icon-close-line'
  export let statusIndicationPeriod = 2000 // millis
  export let action: (data: any) => Promise<any>
  export let actionData: any

  export let status: 'success' | 'failure' | null = null
  let showingStatus = false
  let statusTimeoutId: any = null

  const dispatch = createEventDispatcher()

  function doSetStatus(value) {
    status = value
    showingStatus = true

    if (statusTimeoutId) {
      clearTimeout(statusTimeoutId)
    }
    statusTimeoutId = setTimeout(() => {
      showingStatus = false
    }, statusIndicationPeriod)
  }

  function onStatus(value) {
    doSetStatus(value)
    dispatch('status', { value })
  }

  async function onClick() {
    const result = await action(actionData)
    if (result.ok) {
      onStatus('success')
    } else {
      onStatus('failure')
    }
  }

  let showIcon = icon

  $: {
    showIcon = icon

    if (showingStatus) {
      switch (status) {
        case 'success':
          showIcon = iconSuccess
          break
        case 'failure':
          showIcon = iconFailure
          break
        default:
      }
    }
  }

  onDestroy(() => {
    if (statusTimeoutId) {
      clearTimeout(statusTimeoutId)
    }
  })
</script>

<button class="action-status-icon" on:click={onClick} style:--size={size} type="button">
  <Icon name={showIcon} {size} />
</button>

<style>
  .action-status-icon {
    width: var(--size);
    height: var(--size);
    cursor: pointer;
    background: none;
    border: none;
    padding: 0;
    color: inherit;
    font: inherit;
    display: flex;
    align-items: center;
    justify-content: center;
  }
</style>
