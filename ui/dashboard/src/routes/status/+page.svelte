<script>
  import { onMount } from 'svelte'
  import {
    connectToStatusServer,
    messages,
    wsUrl,
    error,
  } from '@stores/statusStore.js'

  onMount(async () => {
    connectToStatusServer()
  })
</script>

<div class="url">
  {$wsUrl}
  {#if $error && $error.message}
    <span class="error-message">{$error.message}</span>
  {/if}
</div>

<div class="card-content">
  <table class="table">
    <thead>
      <tr>
        <th>Timestamp</th>
        <th>Latency</th>
        <th>Service</th>
        <th>Status</th>
      </tr>
    </thead>
    <tbody>
      {#each $messages as message (message.timestamp)}
        <tr>
          <td>{message.timestamp.replace('T', ' ')}</td>
          <td>{new Date(message.receivedAt) - new Date(message.timestamp)}ms</td
          >
          <td>{message.serviceName}</td>
          <td>{message.statusText}</td>
        </tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
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

  .error-message {
    color: red;
  }
</style>
