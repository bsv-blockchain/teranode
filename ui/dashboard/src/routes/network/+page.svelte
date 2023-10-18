<script>
  import { messages, wsUrl } from '@stores/p2pStore.js'
  import Node from './Node.svelte'

  const uniqueNodes = {}
  let nodes = []

  $: {
    $messages.forEach((m) => {
      if (m.type !== 'block') return

      // Get the record for m.peer_id
      const existing = uniqueNodes[m.peer_id]
      if (!existing) {
        uniqueNodes[m.peer_id] = m
        return
      }

      if (m.height > existing.height) {
        uniqueNodes[m.peer_id] = m
        return
      }
    })

    nodes = Object.values(uniqueNodes)

    console.log('NODES', nodes)
  }
</script>

<div class="url">{$wsUrl}</div>

<div class="card-content">
  <table class="table">
    <thead>
      <tr>
        <th>PeerID</th>
        <th>Address</th>
        <th class="right">Height</th>
        <th>Latest hash</th>
        <th>Previous hash</th>
        <th>Last seen</th>
      </tr>
    </thead>
    <tbody>
      {#each nodes as node}
        <Node {node} />
      {/each}
    </tbody>
  </table>
</div>

<style>
  .right {
    text-align: right;
  }

  /* Custom styles for the table inside card-content */
  .card-content .table {
    width: 100%; /* Make the table take the full width of the card */
    border-collapse: collapse; /* Collapse table borders */
  }

  .card-content .table th,
  .card-content .table th {
    background-color: #f5f5f5; /* Light background for table headers */
    text-align: left; /* Align header text to the left */
  }

  .card-content .table tr:nth-child(even) {
    background-color: #f9f9f9; /* Zebra-striping for even rows */
  }

  @media (max-width: 600px) {
    .table tr {
      display: grid;
      grid-template-columns: 1fr 1fr;
      grid-template-rows: repeat(2, auto);
    }

    .table th {
      grid-column: span 1;
    }
  }

  .url {
    display: inline-block;
    margin-left: 25px;
    padding: 10px;
    font-size: 0.7rem;
  }
</style>
