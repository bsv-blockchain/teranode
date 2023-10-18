<script>
  import BlocksTable from '@components/BlocksTable.svelte'
  import { blocks, error, url, wsUrl } from '@stores/blocksStore2.js'
</script>

<div>
  <div class="url">{$url} / {$wsUrl}</div>
  {#if $error}
    <div class="error">{$error}</div>
  {/if}
</div>

<section class="section">
  <BlocksTable {$blocks} />
</section>

<style>
  .url {
    display: inline-block;
    margin-left: 25px;
    padding: 10px;
    font-size: 0.7rem;
  }

  .error {
    display: inline-block;
    font-size: 0.7rem;
    color: red;
  }
</style>
