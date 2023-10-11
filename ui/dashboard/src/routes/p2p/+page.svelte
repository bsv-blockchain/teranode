<script>
  import MessageBox from './MessageBox.svelte'
  import { messages, wsUrl } from '@stores/p2pStore.js'
</script>

<div>
  <div class="url">{$wsUrl}</div>

  {#each $messages as message}
    <MessageBox {message} />
  {/each}
</div>

<style>
  .url {
    display: inline-block;
    margin-left: 25px;
    padding: 10px;
    font-size: 0.7rem;
  }
</style>
