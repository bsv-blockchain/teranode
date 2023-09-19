<script>
  import { nodes } from '@stores/bootstrapStore.js'
  import Node from './ChaintipTrackerNode.svelte'
</script>

<div class="card panel">
  <header class="card-header">
    <p class="card-header-title">Chaintip tracker</p>
  </header>
  <div class="card-content">
    <table class="table">
      <thead>
        <tr>
          <th>Source</th>
          <th>Name</th>
          <th>Address</th>
          <th class="right">Height</th>
          <th>Latest hash</th>
          <th>Previous hash</th>
        </tr>
      </thead>
      <tbody>
        {#each $nodes as node (node.name)}
          {#if node.blobServerHTTPAddress}
            <Node {node} />
          {/if}
        {/each}
      </tbody>
    </table>
  </div>
  <!-- <footer class="card-footer">
		<a class="card-footer-item">Action 1</a>
		<a class="card-footer-item">Action 2</a>
	</footer> -->
</div>

<style>
  .right {
    text-align: right;
  }

  .panel {
    margin: 20px; /* Adjust as needed */
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
</style>
