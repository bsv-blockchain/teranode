<script lang="ts">
  import { Icon, Typo } from '$lib/components'
  import { ToastStatus } from './types'
  import type { ToastStatusType } from './types'

  export let testId: string | undefined | null = null

  export let status: ToastStatusType = ToastStatus.success
  export let title = ''
  export let message = ''

  let icon = ''

  $: {
    switch (status) {
      case ToastStatus.success:
        icon = 'check-circle'
        break
      case ToastStatus.failure:
        icon = 'exclamation-circle'
        break
      case ToastStatus.warn:
        icon = 'exclamation'
        break
      case ToastStatus.info:
        icon = 'information-circle'
        break
    }
  }

  $: toastVarStr = `--toast`

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--width:var(${toastVarStr}-width)`,
      `--padding:var(${toastVarStr}-padding)`,
      `--border-radius:var(${toastVarStr}-border-radius)`,
      `--border-width:var(${toastVarStr}-border-width)`,
      `--border-style:var(${toastVarStr}-border-style)`,
      `--bg-color:var(${toastVarStr}-${status}-bg-color)`,
      `--border-color:var(${toastVarStr}-${status}-border-color)`,
    ]
  }
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
