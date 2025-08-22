<script lang="ts">
  import { Modal } from '$lib/components'
  import { blockAssemblyModalStore } from '../../stores/blockAssemblyModalStore'
  import { formatNum } from '$lib/utils/format'
  import { onMount, onDestroy } from 'svelte'

  let isOpen = false
  let nodeId = ''
  let nodeUrl = ''
  let blockAssembly: any = null
  
  // Subtree pagination state
  let subtreesExpanded = false
  let subtreesPage = 1
  let subtreesPerPage = 10
  
  $: totalSubtrees = blockAssembly?.subtrees?.length || 0
  $: totalPages = Math.ceil(totalSubtrees / subtreesPerPage)
  $: paginatedSubtrees = blockAssembly?.subtrees?.slice(
    (subtreesPage - 1) * subtreesPerPage,
    subtreesPage * subtreesPerPage
  ) || []
  $: startIndex = (subtreesPage - 1) * subtreesPerPage + 1
  $: endIndex = Math.min(subtreesPage * subtreesPerPage, totalSubtrees)

  const unsubscribe = blockAssemblyModalStore.subscribe((state) => {
    isOpen = state.isOpen
    nodeId = state.nodeId || ''
    nodeUrl = state.nodeUrl || ''
    blockAssembly = state.blockAssembly
    // Reset pagination when modal opens
    if (state.isOpen) {
      subtreesExpanded = false
      subtreesPage = 1
    }
  })

  function handleClose() {
    blockAssemblyModalStore.hide()
  }

  function handleKeydown(event: KeyboardEvent) {
    if (event.key === 'Escape') {
      handleClose()
    }
  }
  
  function toggleSubtrees() {
    subtreesExpanded = !subtreesExpanded
  }
  
  function goToPage(page: number) {
    if (page >= 1 && page <= totalPages) {
      subtreesPage = page
    }
  }

  onMount(() => {
    window.addEventListener('keydown', handleKeydown)
  })

  onDestroy(() => {
    window.removeEventListener('keydown', handleKeydown)
    unsubscribe()
  })

  function formatHash(hash: string | undefined, truncate: boolean = true): string {
    if (!hash) return '-'
    if (truncate && hash.length > 16) {
      return hash.substring(0, 8) + '...' + hash.substring(hash.length - 8)
    }
    return hash
  }
</script>

{#if isOpen && blockAssembly}
  <Modal maxContentW="700px">
    <div class="modal-content">
      <div class="modal-header">
        <h2>Block Assembly Details</h2>
        <button class="close-btn" on:click={handleClose}>×</button>
      </div>
      
      <div class="node-info">
        <span class="node-label">Node:</span>
        <span class="node-value">{nodeUrl}</span>
      </div>

      <div class="details-grid">
        <div class="detail-item">
          <span class="label">Transaction Count:</span>
          <span class="value">{blockAssembly.txCount !== undefined ? formatNum(blockAssembly.txCount) : '-'}</span>
        </div>

        <div class="detail-item">
          <span class="label">State:</span>
          <span class="value state-{(blockAssembly.blockAssemblyState || '').toLowerCase()}">{blockAssembly.blockAssemblyState || '-'}</span>
        </div>

        <div class="detail-item">
          <span class="label">Subtree Count:</span>
          <span class="value">{blockAssembly.subtreeCount !== undefined ? formatNum(blockAssembly.subtreeCount) : '-'}</span>
        </div>

        <div class="detail-item">
          <span class="label">Current Height:</span>
          <span class="value">{blockAssembly.currentHeight !== undefined ? formatNum(blockAssembly.currentHeight) : '-'}</span>
        </div>

        <div class="detail-item full-width">
          <span class="label">Current Hash:</span>
          {#if blockAssembly.currentHash}
            <a 
              href="/viewer/block?hash={blockAssembly.currentHash}" 
              class="value hash-link"
              target="_blank"
              rel="noopener noreferrer"
            >
              {formatHash(blockAssembly.currentHash, false)}
            </a>
          {:else}
            <span class="value hash">-</span>
          {/if}
        </div>

        {#if blockAssembly.subtrees && blockAssembly.subtrees.length > 0}
          <div class="detail-item full-width">
            <div class="subtrees-header">
              <button class="expand-btn" on:click={toggleSubtrees}>
                <span class="expand-icon">{subtreesExpanded ? '▼' : '▶'}</span>
                <span class="label">Subtrees ({formatNum(blockAssembly.subtrees.length)})</span>
              </button>
            </div>
            
            {#if subtreesExpanded}
              <div class="subtrees-container">
                <div class="subtrees-list">
                  {#each paginatedSubtrees as subtree, index}
                    <div class="subtree-item">
                      <span class="subtree-index">{startIndex + index}.</span>
                      <a 
                        href="/viewer/subtree/?hash={subtree}" 
                        class="subtree-hash-link"
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        {formatHash(subtree, false)}
                      </a>
                    </div>
                  {/each}
                </div>
                
                {#if totalPages > 1}
                  <div class="pagination">
                    <button 
                      class="page-btn" 
                      on:click={() => goToPage(subtreesPage - 1)}
                      disabled={subtreesPage === 1}
                    >
                      ←
                    </button>
                    
                    <span class="page-info">
                      Page {subtreesPage} of {totalPages} 
                      <span class="page-range">({startIndex}-{endIndex} of {totalSubtrees})</span>
                    </span>
                    
                    <button 
                      class="page-btn" 
                      on:click={() => goToPage(subtreesPage + 1)}
                      disabled={subtreesPage === totalPages}
                    >
                      →
                    </button>
                  </div>
                {/if}
              </div>
            {/if}
          </div>
        {/if}
      </div>
    </div>
  </Modal>
{/if}

<style>
  .modal-content {
    background: #2a2b36;
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 12px;
    padding: 24px;
    min-width: 400px;
    max-height: 80vh;
    overflow-y: auto;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
    z-index: 1000;
  }

  .modal-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
    padding-bottom: 12px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
  }

  .modal-header h2 {
    margin: 0;
    font-size: 20px;
    font-weight: 600;
    color: #ffffff;
  }

  .close-btn {
    background: none;
    border: none;
    font-size: 28px;
    line-height: 1;
    color: #999;
    cursor: pointer;
    padding: 0;
    width: 32px;
    height: 32px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 4px;
    transition: all 0.2s;
  }

  .close-btn:hover {
    background: rgba(255, 255, 255, 0.1);
    color: #ffffff;
  }

  .node-info {
    display: flex;
    gap: 8px;
    margin-bottom: 20px;
    padding: 8px 12px;
    background: rgba(0, 0, 0, 0.3);
    border-radius: 6px;
  }

  .node-label {
    font-weight: 600;
    color: #999;
  }

  .node-value {
    color: #ffffff;
  }

  .details-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 16px;
  }

  .detail-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .detail-item.full-width {
    grid-column: span 2;
  }

  .label {
    font-size: 12px;
    font-weight: 600;
    color: #999;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .value {
    font-size: 16px;
    color: #ffffff;
    font-weight: 500;
  }

  .value.hash {
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 13px;
    word-break: break-all;
    line-height: 1.4;
  }

  .value.hash-link {
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 13px;
    word-break: break-all;
    line-height: 1.4;
    color: #4a9eff;
    text-decoration: none;
    display: inline-block;
    transition: all 0.2s;
  }

  .value.hash-link:hover {
    color: #6fb4ff;
    text-decoration: underline;
  }

  .value.hash-link:active {
    color: #3a8eef;
  }

  .value.state-active {
    color: #15b241;
  }

  .value.state-inactive {
    color: #ffa500;
  }

  .value.state-stopped {
    color: #ff4444;
  }

  .subtrees-header {
    margin-top: 8px;
  }

  .expand-btn {
    display: flex;
    align-items: center;
    gap: 8px;
    background: none;
    border: none;
    padding: 8px 12px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.2s;
    width: 100%;
    text-align: left;
  }

  .expand-btn:hover {
    background: rgba(255, 255, 255, 0.05);
  }

  .expand-icon {
    color: #999;
    font-size: 12px;
    transition: transform 0.2s;
  }

  .subtrees-container {
    margin-top: 12px;
    animation: slideDown 0.2s ease-out;
  }

  @keyframes slideDown {
    from {
      opacity: 0;
      transform: translateY(-10px);
    }
    to {
      opacity: 1;
      transform: translateY(0);
    }
  }

  .subtrees-list {
    display: flex;
    flex-direction: column;
    gap: 6px;
    max-height: 300px;
    overflow-y: auto;
    padding: 12px;
    background: rgba(0, 0, 0, 0.3);
    border: 1px solid rgba(255, 255, 255, 0.05);
    border-radius: 4px;
  }

  .subtree-item {
    display: flex;
    gap: 8px;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 12px;
    padding: 4px 0;
  }

  .subtree-index {
    color: #777;
    min-width: 40px;
    text-align: right;
  }

  .subtree-hash {
    color: #ddd;
    word-break: break-all;
  }

  .subtree-hash-link {
    color: #4a9eff;
    text-decoration: none;
    word-break: break-all;
    transition: all 0.2s;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 12px;
  }

  .subtree-hash-link:hover {
    color: #6fb4ff;
    text-decoration: underline;
  }

  .subtree-hash-link:active {
    color: #3a8eef;
  }

  .pagination {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-top: 12px;
    padding: 8px 12px;
    background: rgba(0, 0, 0, 0.2);
    border-radius: 6px;
  }

  .page-btn {
    background: rgba(255, 255, 255, 0.1);
    border: 1px solid rgba(255, 255, 255, 0.1);
    color: #fff;
    padding: 4px 12px;
    border-radius: 4px;
    cursor: pointer;
    font-size: 14px;
    transition: all 0.2s;
  }

  .page-btn:hover:not(:disabled) {
    background: rgba(255, 255, 255, 0.2);
    border-color: rgba(255, 255, 255, 0.2);
  }

  .page-btn:disabled {
    opacity: 0.3;
    cursor: not-allowed;
  }

  .page-info {
    color: #fff;
    font-size: 13px;
    text-align: center;
  }

  .page-range {
    color: #999;
    font-size: 12px;
  }
</style>