<script>
  import BlocksTable from '@components/BlocksTable.svelte'
  import Spinner from '@components/Spinner.svelte'

  import { selectedNode } from '@stores/nodeStore.js'

  let blocks = []
  let error = ''
  let loading = false
  let url = ''

  // Fetch data when the selected node changes
  $: $selectedNode && fetchData()

  async function fetchData() {
    try {
      if (!$selectedNode) {
        return
      }

      error = ''
      loading = true

      url = `${$selectedNode}/lastblocks?n=11` // Get 1 more block than we need to calculate the delta time

      const res = await fetch(url)

      if (!res.ok) {
        throw new Error(`HTTP error! Status: ${res.status}`)
      }

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
    <button class="button is-info" on:click={() => fetchData()}>Refresh</button>

    <!-- Blocks table -->
    <BlocksTable {blocks} />
  </section>
{/if}
