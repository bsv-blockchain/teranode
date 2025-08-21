<script lang="ts">
  import { tippy } from '$lib/stores/media'
  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'
  
  export let hash = ''
  export let hashUrl = ''
  export let shortHash = ''
  export let miner = ''
  export let className = ''
  export let tooltip = ''
  export let showCopyButton = false
  export let copyTooltip = 'Copy hash'
</script>

<div class="hash-miner-container {className}">
  <div class="hash-wrapper">
    <div class="hash-row">
      {#if hash && hashUrl}
        {#if tooltip && $tippy}
          <div class="hash-link-wrapper" use:$tippy={{ content: tooltip, interactive: false, trigger: 'mouseenter' }}>
            <a href={hashUrl} class="hash-link">
              {shortHash || hash}
            </a>
          </div>
        {:else}
          <a href={hashUrl} class="hash-link">
            {shortHash || hash}
          </a>
        {/if}
      {:else if hash}
        <span class="hash-text">{shortHash || hash}</span>
      {:else}
        <span>-</span>
      {/if}
      {#if showCopyButton && hash}
        <div class="copy-icon" use:$tippy={{ content: copyTooltip }}>
          <ActionStatusIcon
            icon="icon-duplicate-line"
            action={copyTextToClipboardVanilla}
            actionData={hash}
            size={15}
          />
        </div>
      {/if}
    </div>
    {#if miner}
      <div class="miner-text">{miner}</div>
    {/if}
  </div>
</div>

<style>
  .hash-miner-container {
    display: flex;
    flex-direction: column;
  }
  
  .hash-wrapper {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  
  .hash-row {
    display: flex;
    align-items: center;
    gap: 4px;
  }
  
  .hash-link-wrapper {
    display: inline-flex;
  }
  
  .hash-link {
    color: #4a9eff;
    text-decoration: none;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
  }
  
  .hash-link:hover {
    text-decoration: underline;
  }
  
  .hash-text {
    color: #4a9eff;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
  }
  
  .miner-text {
    color: rgba(255, 255, 255, 0.5);
    font-size: 11px;
    word-break: break-word;
    max-width: 100%;
  }
  
  .copy-icon {
    cursor: pointer;
    flex-shrink: 0;
  }
</style>
