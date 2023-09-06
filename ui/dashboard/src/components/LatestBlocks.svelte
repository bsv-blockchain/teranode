<script>
  import { blocks, error } from '@stores/blocksStore.js'
  import LatestBlocksNode from '@components/LatestBlocksNode.svelte'
</script>

<div>
  {#if $error}
    <p>Error fetching hashes: {$error}</p>
  {:else}
    <div class="box">
      {#each Object.entries($blocks) as [key, item]}
        <div class="card">
          <header class="card-header">
            <p>Chaintip: {key}</p>
          </header>
          {#each item as block}
            <LatestBlocksNode data={block} />
          {/each}
        </div>
      {/each}
    </div>
  {/if}
</div>
