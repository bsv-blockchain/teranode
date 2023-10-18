<!-- src/components/MessageBox.svelte -->
<script>
  import { onMount } from 'svelte'
  import { humanTime } from '@utils/humanTime.js'

  export let message // This will receive the JSON data as a prop
  let age = ''

  onMount(() => {
    age = humanTime(message.receivedAt)

    const interval = setInterval(() => {
      if (message.receivedAt) {
        age = humanTime(message.receivedAt)
      }
    }, 1000)

    return () => clearInterval(interval)
  })
</script>

<!-- If message is a "block" -->
{#if message.type === 'block'}
  <div class="message-box blue">
    <div class="message-box-title">
      <span>BLOCK</span>
      <span>
        {message.receivedAt.toISOString().replace('T', ' ')}
        ({age} ago)
      </span>
    </div>
    <div class="message-box-content">
      <div><span>Hash:</span><span>{message.hash}</span></div>
      <div><span>From:</span><span>{message.base_url}</span></div>
      <div><span>Peer:</span><span>{message.peer_id}</span></div>
      <div><span>Height:</span><span>{message.height}</span></div>
      <div><span>Size:</span><span>{message.sizeInBytes}</span></div>
      <div><span>Txns:</span><span>{message.txCount}</span></div>
      <div><span>Miner:</span><span>{message.miner}</span></div>
    </div>
  </div>
{:else if message.type === 'subtree'}
  <div class="message-box pink">
    <div class="message-box-title">
      <span>SUBTREE</span>
      <span>
        {message.receivedAt.toISOString().replace('T', ' ')}
        ({age} ago)
      </span>
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
      <span>
        {message.receivedAt.toISOString().replace('T', ' ')}
        ({age} ago)
      </span>
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
    width: 85px;
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
    margin-left: 85px;
    padding-left: 10px;
    padding-right: 10px;
    padding-bottom: 3px;
  }

  .message-box-content span:first-child {
    display: inline-block; /* This is necessary to set a fixed width */
    width: 50px;
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
