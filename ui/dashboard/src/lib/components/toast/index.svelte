<svelte:options runes={true} />

<script lang="ts">
  import { Icon, Typo } from '$lib/components'
  import { ToastStatus } from './types'
  import type { ToastStatusType } from './types'

  let {
    testId = null,
    status = ToastStatus.success,
    title = '',
    message = '',
  }: {
    testId?: string | undefined | null
    status?: ToastStatusType
    title?: string
    message?: string
  } = $props()

  const icon = $derived.by(() => {
    switch (status) {
      case ToastStatus.success:
        return 'check-circle'
      case ToastStatus.failure:
        return 'exclamation-circle'
      case ToastStatus.warn:
        return 'exclamation'
      case ToastStatus.info:
        return 'information-circle'
    }
    return ''
  })

  const toastVarStr = `--toast`

  const cssVars = $derived([
    `--width:var(${toastVarStr}-width)`,
    `--padding:var(${toastVarStr}-padding)`,
    `--border-radius:var(${toastVarStr}-border-radius)`,
    `--border-width:var(${toastVarStr}-border-width)`,
    `--border-style:var(${toastVarStr}-border-style)`,
    `--bg-color:var(${toastVarStr}-${status}-bg-color)`,
    `--border-color:var(${toastVarStr}-${status}-border-color)`,
  ])
</script>

<div class="tui-toast" data-test-id={testId} style={`${cssVars.join(';')}`}>
  <div class="tab"><Icon name={icon} size={20} /></div>
  <div class="body">
    {#if title}
      <Typo variant="heading" size={6} value={title} />
    {/if}
    {#if message}
      <Typo variant="body" size={3} value={message} />
    {/if}
  </div>
</div>

<style>
  .tui-toast {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    width: var(--width);

    display: flex;
    align-items: flex-start;
    gap: 10px;

    background-color: var(--bg-color);
    border-color: var(--border-color);

    border-style: var(--border-style);
    border-width: var(--border-width);
    border-radius: var(--border-radius);

    padding: var(--padding);
  }

  .tab {
    color: var(--border-color);
  }

  .body {
    flex: 1;

    display: flex;
    flex-direction: column;
    align-items: flex-start;
    justify-content: flex-start;
    gap: 10px;

    min-height: 40px;
    word-break: break-all;
  }
</style>
