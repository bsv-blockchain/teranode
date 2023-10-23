<script>
  import { miningNodes, wsUrl } from '@stores/p2pStore.js'
  import Node from './Node.svelte'
</script>

<div class="url">{$wsUrl}</div>

<div class="card-content">
  <table class="table">
    <thead>
      <tr>
        <th>Address</th>
        <th class="right">Height</th>
        <th class="right">TX Count</th>
        <th class="right">Size</th>
        <th>Miner</th>
        <th>Latest hash</th>
        <th>Previous hash</th>
        <th style="width:120px;">Last seen</th>
      </tr>
    </thead>
    <tbody>
      {#each $miningNodes as node (node.base_url)}
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
