<script lang="ts">
  import { Icon, Progress } from '$lib/components'
  import { copyTextToClipboard } from '../../utils/clipboard'
  import type { I18n } from '../../types'

  export let testId: string | undefined | null = null

  export let label: any = ''
  export let value: number | string = ''
  export let format = null
  export let layout = 'column'
  export let copy = false
  export let wrapLabel = false
  export let i18n: I18n

  let textWidth = '100%'

  $: {
    textWidth = copy ? 'calc(100% - 24px)' : '100%'
  }

  function onCopy(value) {
    copyTextToClipboard(i18n, value)
  }
</script>

<div
  class="tui-field"
  data-test-id={testId}
  class:row={layout === 'row'}
  style:--text-width={textWidth}
  style:--wrap-label={wrapLabel ? 'normal' : 'nowrap'}
>
  <div class="label">{label}</div>
  <div class="value">
    {#if format === 'progress'}
      <Progress ratio={parseInt(value.toString())} />
    {:else}
      <div class="text">
        {value}
      </div>
    {/if}
    {#if copy}
      <div class="ico" on:click={() => onCopy(value)}>
        <Icon name="document-duplicate" size={18} />
      </div>
    {/if}
  </div>
</div>

<style>
  .tui-field {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    width: 100%;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
  }
  .tui-field.row {
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
    width: 100%;
  }

  .label {
    font-weight: 600;
    font-size: 13px;
    line-height: 16px;

    text-transform: uppercase;
    white-space: var(--wrap-label);

    color: #232d7c;
  }

  .value {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 8px;

    font-weight: 400;
    font-size: 14px;
    line-height: 24px;
    letter-spacing: -0.01em;

    color: #282933;
  }

  .text {
    width: var(--text-width);
    word-wrap: break-word;
  }

  .ico {
    width: 18px;
    height: 18px;
    cursor: pointer;
    color: #232d7c;
  }
</style>
