<!-- src/components/MessageBox.svelte -->
<script>
  import { onMount } from 'svelte'
  import { humanTime } from '@utils/humanTime.js'

  export let node // This will receive the JSON data as a prop
  let age = ''

  onMount(() => {
    age = humanTime(node.receivedAt)

    const interval = setInterval(() => {
      if (node.receivedAt) {
        age = humanTime(node.receivedAt)
      }
    }, 1000)

    return () => clearInterval(interval)
  })

  function shortHash(hash) {
    if (!hash) return ''

    return hash.substring(0, 4) + '...' + hash.substring(hash.length - 8)
  }
</script>

<tr>
  <td>{node.base_url}</td>
  <td class="right">{node.height}</td>
  <td class="right">{node.tx_count}</td>
  <td class="right">{node.size_in_bytes}</td>
  <td>{node.miner}</td>
  <td title={node.hash}>{shortHash(node.hash)}</td>
  <td title={node.previousblockhash}>{shortHash(node.previousblockhash)}</td>
  <td class="right" title={node.receivedAt}>{age} ago</td>
</tr>

<style>
  td {
    border: 1px solid #e5e5e5; /* Add a light border to table cells */
    padding: 8px 12px; /* Add some padding to table cells */
  }

  .right {
    text-align: right;
  }

  @media (max-width: 600px) {
    tr {
      display: grid;
      grid-template-columns: 1fr 1fr;
      grid-template-rows: repeat(2, auto);
    }

    td {
      grid-column: span 1;
    }
  }
</style>
