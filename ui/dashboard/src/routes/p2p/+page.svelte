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

  let byPeer = false
  let filter = ''
  let groupedMessages = []
  let filteredMessages = []

  $: {
    filteredMessages = $messages

    if (filter.length > 0) {
      const f = filter.toLowerCase()

      filteredMessages = filteredMessages.filter((message) => {
        const search = JSON.stringify(message).toLowerCase()
        return search.includes(f)
      })
    }

    if (byPeer) {
      let newGroupedMessages = {}

      filteredMessages.forEach((message) => {
        if (message.type !== 'Ping') {
          if (!newGroupedMessages[message.peer_id]) {
            newGroupedMessages[message.peer_id] = []
          }
          newGroupedMessages[message.peer_id].push(message)
        }
      })

      Object.keys(newGroupedMessages).forEach((peer_id) => {
        newGroupedMessages[peer_id].sort((a, b) =>
          a.peer_id.localeCompare(b.peerid)
        )
      })

      groupedMessages = newGroupedMessages
    }
  }
</script>

<div>
  <div class="url">
    {$wsUrl}
    <span class="check">
      <input type="checkbox" bind:checked={byPeer} id="group-by-peer" />
      <label for="group-by-peer" class="checkbox-label">by Peer</label>
      <input
        class="input is-small"
        type="text"
        placeholder="Filter"
        bind:value={filter}
      />
    </span>
  </div>

  {#if byPeer}
    <div class="container">
      {#each Object.keys(groupedMessages) as peer}
        <div class="column">
          <h2>{peer}</h2>
          {#each groupedMessages[peer] as message}
            <MessageBox {message} />
          {/each}
        </div>
      {/each}
    </div>
  {:else}
    {#each filteredMessages as message}
      <MessageBox {message} />
    {/each}
  {/if}
</div>

<style>
  .url {
    display: flex;
    align-items: center;
    margin-left: 25px;
    padding: 10px;
    font-size: 0.7rem;
  }
  .container {
    display: flex;
    justify-content: space-around;
    max-width: 100%;
    overflow-x: hidden;
  }

  .column {
    flex: 1;
  }

  .check {
    margin-left: 10px;
    display: flex;
    align-items: center;
    margin-left: 25px;
    padding: 10px;
    font-size: 0.7rem;
  }

  * {
    box-sizing: border-box;
  }

  .column {
    flex: 1;
    min-width: 0;
  }

  .checkbox-label {
    min-width: 100px; /* Adjust as needed */
  }
</style>
