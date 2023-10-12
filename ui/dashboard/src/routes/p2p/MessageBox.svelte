<!-- src/components/MessageBox.svelte -->
<script>
  export let message // This will receive the JSON data as a prop
</script>

<!-- If message is a "block" -->
{#if message.type === 'block'}
  <div class="message-box blue">
    <div class="message-box-title">
      <span>BLOCK</span>
      <span>{message.receivedAt.toISOString().replace('T', ' ')}</span>
    </div>
    <div class="message-box-content">
      <div>Hash: {message.hash}</div>
      <div>From: {message.base_url}</div>
      <div>Peer: {message.peer_id}</div>
      <div>Height: {message.details.height}</div>
      <div>Size: {message.details.sizeInBytes}</div>
      <div>Txns: {message.details.txCount}</div>
      <div>Miner: {message.details.miner}</div>
    </div>
  </div>
{:else if message.type === 'subtree'}
  <div class="message-box pink">
    <div class="message-box-title">
      <span>SUBTREE</span>
      <span>{message.receivedAt.toISOString().replace('T', ' ')}</span>
    </div>
    <div class="message-box-content">
      <div>Hash: {message.hash}</div>
      <div>From: {message.base_url}</div>
      <div>Peer: {message.peer_id}</div>
    </div>
  </div>
{:else if message.type === 'Ping'}
  <div class="message-box grey">
    <div class="message-box-title">
      <span>PING</span>
      <span>{message.receivedAt.toISOString().replace('T', ' ')}</span>
    </div>
  </div>
{/if}

<style>
  /* Add your CSS styles here to style the message box */
  .message-box {
    border: 1px solid #ccc;
    margin-left: 20px;
    margin-right: 20px;
    padding-left: 10px;
    padding-right: 10px;
    margin: 5px;
    border-radius: 5px;
  }

  .message-box-title {
    font-size: 1em;
    font-weight: bold;
  }

  .message-box-title span:first-child {
    display: inline-block; /* This is necessary to set a fixed width */
    width: 70px;
    padding-left: 10px;
    padding-right: 10px;
  }

  .message-box-title span:nth-child(2) {
    font-size: 0.7em;
    font-weight: normal;
    vertical-align: middle;
  }

  .message-box-content {
    font-size: 0.7em;
    margin-top: 3px;
    margin-left: 70px;
    padding-left: 10px;
    padding-right: 10px;
    padding-bottom: 3px;
  }

  .blue {
    background-color: #89cff0;
  }

  .pink {
    background-color: #ffc0cb;
  }

  .grey {
    background-color: #d3d3d3;
  }
</style>
