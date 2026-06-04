<svelte:options runes={true} />

<script lang="ts">
  import type { Snippet } from 'svelte'

  let {
    title = '',
    headerPadding = '20px 24px 32px 24px',
    contentPadding = '0 24px',
    footerPadding,
    stretch = true,
    showFooter = true,
    wrapHeader = false,
    subtitle,
    headerTools,
    footer,
    children,
  }: {
    title?: string | Snippet
    headerPadding?: string
    contentPadding?: string
    footerPadding?: string
    stretch?: boolean
    showFooter?: boolean
    wrapHeader?: boolean
    subtitle?: Snippet
    headerTools?: Snippet
    footer?: Snippet
    children?: Snippet
  } = $props()

  const effectiveFooterPadding = $derived(
    footerPadding ?? (footer ? '20px 24px' : '0 24px 20px 24px'),
  )
</script>

<div
  class="tui-card"
  class:stretch
  style:--header-padding={headerPadding}
  style:--content-padding={contentPadding}
  style:--footer-padding={effectiveFooterPadding}
>
  <div class="header" class:wrapHeader>
    <div class="title-container">
      <div class="title">
        {#if typeof title === 'function'}
          {@render title()}
        {:else}
          {title}
        {/if}
      </div>
      <div class="subtitle">
        {@render subtitle?.()}
      </div>
    </div>
    <div class="header-tools">
      {@render headerTools?.()}
    </div>
  </div>
  <div class="content">
    {@render children?.()}
  </div>
  {#if showFooter}
    <div class="footer">
      {@render footer?.()}
    </div>
  {/if}
</div>

<style>
  .tui-card {
    box-sizing: var(--box-sizing);
    background: var(--comp-bg-color);
    border-radius: 12px;
  }
  .tui-card.stretch {
    width: 100%;
  }

  .tui-card .header {
    padding: var(--header-padding);
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
  }
  .tui-card .header.wrapHeader {
    gap: 5px;
    flex-wrap: wrap;
  }

  .tui-card .header .title-container {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
  }

  .tui-card .header .title {
    color: var(--app-color);

    font-family: Satoshi;
    font-size: 22px;
    font-style: normal;
    font-weight: 700;
    line-height: 28px;
    letter-spacing: 0.44px;
  }

  .tui-card .header .subtitle {
    color: var(--comp-label-color);

    font-family: Satoshi;
    font-size: 15px;
    font-style: normal;
    font-weight: 400;
    line-height: 24px;
    letter-spacing: 0.3px;
  }

  .tui-card .header-tools {
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: flex-end;
    flex-wrap: wrap;
    gap: 8px;
    flex: 1 0;
  }

  .tui-card .content {
    padding: var(--content-padding);
  }

  .tui-card .footer {
    padding: var(--footer-padding);
  }
</style>
