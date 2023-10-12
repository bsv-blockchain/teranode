<script>
  import { afterUpdate } from 'svelte'
  import MessageBox from './MessageBox.svelte'
  import { messages, wsUrl } from '@stores/p2pStore.js'

  function scrollToTop() {
    if (!import.meta.env.SSR && window && window.scrollTo) {
      // Check if the user is near the top of the scroll (e.g., within the top 100 pixels)
      const scrollThreshold = 100 // Adjust this threshold as needed

      if (window.scrollY <= scrollThreshold) {
        window.scrollTo({ top: 0, behavior: 'smooth' })
      }
    }
  }

  afterUpdate(() => {
    scrollToTop()
  })
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
