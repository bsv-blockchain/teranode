<script>
  import BlocksTable from '@components/BlocksTable.svelte'
  import { blobServerHTTPAddress } from '@stores/nodeStore.js'

  let blocks = []
  let error = ''
  let url = ''

  const numberOfBlocks = 100

  // Fetch data when the selected node changes
  $: $blobServerHTTPAddress && fetchData()

  function getHumanReadableTime(diff) {
    const days = Math.floor(diff / (1000 * 60 * 60 * 24))
    const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60))
    const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60))
    const seconds = Math.floor((diff % (1000 * 60)) / 1000)

    let humanReadableTime = ''
    if (days > 0) {
      return `${days}d${hours}h${minutes}m`
    } else if (hours > 0) {
      return `${hours}h${minutes}m`
    } else if (minutes > 0) {
      return `${minutes}m${seconds}s`
    } else {
      return `${seconds}s`
    }
  }

  async function fetchData() {
    try {
      if (!$blobServerHTTPAddress) {
        return
      }

      error = ''

      url = `${$blobServerHTTPAddress}/lastblocks?n=${numberOfBlocks + 1}` // Get 1 more block than we need to calculate the delta time

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

        block.timeDiff = diff / 1000 // The time diff between blocks (in seconds)
        block.deltaTime = getHumanReadableTime(diff) // The time diff in human readable format
      })

      // Calculate the age of the block
      b.forEach((block) => {
        const blockTime = new Date(block.timestamp)
        const now = new Date()
        const diff = now - blockTime
        block.age = getHumanReadableTime(diff)
      })

      blocks = b.slice(0, numberOfBlocks) // Only show the last 10 blocks
    } catch (err) {
      error = err.message
      console.error(err)
    }
  }
</script>

<div>
  <div class="url">{url}</div>
  {#if error}
    <div class="error">{error}</div>
  {/if}
</div>

<section class="section">
  <button class="button is-info" on:click={() => fetchData()}>Refresh</button>

  <!-- Blocks table -->
  <BlocksTable {blocks} />
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
