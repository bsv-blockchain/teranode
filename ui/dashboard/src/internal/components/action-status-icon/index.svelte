<svelte:options runes={true} />

<script lang="ts">
  import { onDestroy } from 'svelte'
  import { Icon } from '$lib/components'

  let {
    size = 18,
    icon = '',
    iconSuccess = 'icon-check-line',
    iconFailure = 'icon-close-line',
    statusIndicationPeriod = 2000, // millis
    action,
    actionData,
    status = $bindable(null),
    onstatus,
  }: {
    size?: number
    icon?: string
    iconSuccess?: string
    iconFailure?: string
    statusIndicationPeriod?: number
    action: (data: any) => Promise<any>
    actionData?: any
    status?: 'success' | 'failure' | null
    onstatus?: (e: { value: 'success' | 'failure' }) => void
  } = $props()

  let showingStatus = $state(false)
  let statusTimeoutId: any = null

  function doSetStatus(value: 'success' | 'failure') {
    status = value
    showingStatus = true

    if (statusTimeoutId) {
      clearTimeout(statusTimeoutId)
    }
    statusTimeoutId = setTimeout(() => {
      showingStatus = false
    }, statusIndicationPeriod)
  }

  function onStatus(value: 'success' | 'failure') {
    doSetStatus(value)
    onstatus?.({ value })
  }

  async function onClick() {
    const result = await action(actionData)
    if (result.ok) {
      onStatus('success')
    } else {
      onStatus('failure')
    }
  }

  const showIcon = $derived.by(() => {
    if (showingStatus) {
      if (status === 'success') return iconSuccess
      if (status === 'failure') return iconFailure
    }
    return icon
  })

  onDestroy(() => {
    if (statusTimeoutId) {
      clearTimeout(statusTimeoutId)
    }
  })
</script>

<button class="action-status-icon" onclick={onClick} style:--size={size} type="button">
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
