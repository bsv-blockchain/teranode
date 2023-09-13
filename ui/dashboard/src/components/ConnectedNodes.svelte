<script>
  import { nodes } from '@stores/bootstrapStore.js'
  import ConnectedAge from '@components/ConnectedAge.svelte'
</script>

<div class="card panel">
  <header class="card-header">
    <p class="card-header-title">Connected nodes</p>
  </header>
  <div class="card-content">
    {#if $nodes.length === 0}
      <p>No nodes connected</p>
    {:else}
      <table class="table">
        <tbody>
          {#each $nodes as node (node.ip)}
            <tr>
              <td>{node.blobServerHTTPAddress || 'anonymous'}</td>
              <td>{node.source}</td>
              <td>{node.name}</td>
              <td>{node.ip}</td>
              <td>
                <ConnectedAge time={node.connectedAt} />
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
  <!-- <footer class="card-footer">
		<a class="card-footer-item">Action 1</a>
		<a class="card-footer-item">Action 2</a>
	</footer> -->
</div>

<style>
  .table {
    width: 100%; /* Make the table take the full width of the card */
    border-collapse: collapse; /* Collapse table borders */
  }

  .table td {
    vertical-align: middle;
    padding-top: 2px;
    padding-bottom: 2px;
  }

  .table tr:nth-child(even) {
    background-color: #f9f9f9; /* Zebra-striping for even rows */
  }

  @media (max-width: 600px) {
    .table tr {
      display: grid;
      grid-template-columns: 1fr 1fr;
      grid-template-rows: repeat(2, auto);
    }

    .table td {
      grid-column: span 1;
    }
  }
</style>
