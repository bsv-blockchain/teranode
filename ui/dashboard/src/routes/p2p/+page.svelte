<script>
  import MessageBox from './MessageBox.svelte'
  import { messages } from '@stores/p2pStore.js'

  $: console.log($messages)
</script>

<div>
  {#each $messages as message}
    <MessageBox {message} />
  {/each}
</div>
