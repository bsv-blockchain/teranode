<script>
  import { onMount } from 'svelte'
  import BlocksTable from '@components/BlocksTable.svelte'
  import Spinner from '@components/Spinner.svelte'

  import { nodes } from '@stores/nodeStore.js'

  let blocks = []
  let selectedURL = ''
  let urls = []
  let error = ''
  let loading = false

  $: if ($nodes) {
    urls = $nodes.map((node) => node.blobServerHTTPAddress)
    if (!selectedURL && urls.length > 0) {
      selectedURL = urls[0]
    }
  }

  onMount(async () => {
    fetchData(selectedURL)
  })

  async function fetchData(url) {
    try {
      if (!url) return

      error = ''
      loading = true

      const res = await fetch(`${url}/lastblocks?n=11`) // Get 1 more block than we need to calculate the delta time

      const b = await res.json()

      // Calculate delta time which is the time between blocks
      b.forEach((block, i) => {
        if (i === b.length - 1) {
          return
        }

        const prevBlock = b[i + 1]
        const prevBlockTime = new Date(prevBlock.timestamp)
        const blockTime = new Date(block.timestamp)
        const diff = blockTime - prevBlockTime

        const minutes = Math.floor(diff / (1000 * 60))
        const seconds = Math.floor((diff % (1000 * 60)) / 1000)

        if (minutes > 0) {
          block.deltaTime = `${minutes}m${seconds}s`
        } else {
          block.deltaTime = `${seconds}s`
        }
      })

      // Calculate the age of the block
      b.forEach((block) => {
        const blockTime = new Date(block.timestamp)
        const now = new Date()
        const diff = now - blockTime

        const days = Math.floor(diff / (1000 * 60 * 60 * 24))
        const hours = Math.floor(
          (diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60)
        )
        const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60))
        const seconds = Math.floor((diff % (1000 * 60)) / 1000)

        if (days > 0) {
          block.age = `${days}d${hours}h${minutes}m`
        } else if (hours > 0) {
          block.age = `${hours}h${minutes}m`
        } else if (minutes > 0) {
          block.age = `${minutes}m${seconds}s`
        } else {
          block.age = `${seconds}s`
        }
      })

      blocks = b.slice(0, 10) // Only show the last 10 blocks
    } catch (err) {
      error = err.message
      console.error(err)
    } finally {
      loading = false
    }
  }
</script>

{#if error}
  <p>{error}</p>
{:else}
  {#if loading}
    <Spinner />
  {/if}
  <section class="section">
    <!-- Dropdown for URL selection -->
    <div class="select">
      <select bind:value={selectedURL} on:change={() => fetchData(selectedURL)}>
        <option disabled>Select a URL</option>
        {#each urls as url (url)}
          <option value={url}>{url}</option>
        {/each}
      </select>
    </div>

    <button class="button is-info" on:click={() => fetchData(selectedURL)}
      >Refresh</button
    >

    <!-- Blocks table -->
    <BlocksTable {blocks} />
  </section>
{/if}

<style>
  .select {
    margin-bottom: 10px;
  }
</style>
