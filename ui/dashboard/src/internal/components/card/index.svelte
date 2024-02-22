<script lang="ts">
  export let title = ''
  export let headerPadding = '20px 24px 32px 24px'
  export let contentPadding = '0 24px'
  export let footerPadding = $$slots.footer ? '20px 24px' : '0 24px 20px 24px'
  export let stretch = true
  export let showFooter = true
  export let wrapHeader = false
</script>

<div
  class="tui-card"
  class:stretch
  style:--header-padding={headerPadding}
  style:--content-padding={contentPadding}
  style:--footer-padding={footerPadding}
>
  <div class="header" class:wrapHeader>
    <div class="title-container">
      <div class="title">
        {#if $$slots.title}
          <slot name="title" />
        {:else}
          {title}
        {/if}
      </div>
      <div class="subtitle">
        <slot name="subtitle" />
      </div>
    </div>
    <div class="header-tools">
      <slot name="header-tools" />
    </div>
  </div>
  <div class="content">
    <slot />
  </div>
  {#if showFooter}
    <div class="footer">
      <slot name="footer" />
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
    color: rgba(255, 255, 255, 0.88);

    font-family: Satoshi;
    font-size: 22px;
    font-style: normal;
    font-weight: 700;
    line-height: 28px;
    letter-spacing: 0.44px;
  }

  .tui-card .header .subtitle {
    color: rgba(255, 255, 255, 0.66);

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
