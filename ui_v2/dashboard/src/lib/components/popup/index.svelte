<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { Heading, Icon } from '$lib/components'

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  export let title: any = ''
  export let maxW = -1

  function onClose() {
    dispatch('close')
  }
</script>

<div
  class="tui-popup"
  data-test-id={testId}
  style:--max-width-local={maxW !== -1 ? maxW + 'px' : 'auto'}
>
  <div class="header">
    <div class="title"><Heading value={title} size={5} /></div>
    <!-- svelte-ignore a11y-click-events-have-key-events -->
    <div class="ico" on:click={onClose}><Icon name="close" size={24} /></div>
  </div>
  {#if $$slots.body}
    <div class="body">
      <slot name="body" />
    </div>
  {/if}
  {#if $$slots.footer}
    <div class="footer">
      <slot name="footer" />
    </div>
  {/if}
</div>

<style>
  .tui-popup {
    display: flex;
    flex-direction: column;
    width: 100%;
    max-width: var(--max-width-local);

    background: #ffffff;
    border: 1px solid #ffffff;
    box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.08);
    border-radius: 6px;
  }

  .header {
    display: flex;
    align-items: center;

    height: 52px;
    padding: 0 16px;
    border-bottom: 1px solid #efefef;
  }
  .title {
    width: 100%;
    margin-right: 48px;
  }
  .ico {
    right: 16px;
    width: 24px;
    height: 24px;
    cursor: pointer;
  }

  .body {
    display: flex;
    flex-direction: column;
    align-self: flex-start;
    width: calc(100% - 32px);
    padding: 16px;
  }

  .footer {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    width: calc(100% - 32px);
    padding: 16px;
  }
  .footer > :global(div) {
    flex: 0;
  }
</style>
