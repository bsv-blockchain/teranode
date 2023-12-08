<script>
  import { onMount } from 'svelte'
  import {
    connectToStatusServer,
    messages,
    wsUrl,
    error,
  } from '@stores/statusStore.js'

  import {
    wsUrl as p2pWsUrl,
    error as p2pError,
    miningNodes,
    messages as p2pMessages,
    connectToP2PServer,
  } from '@stores/p2pStore.js'

  onMount(async () => {
    connectToStatusServer()
    connectToP2PServer()
  })

  // timestamp, base_url,type,hash
  // timestamp, service_name,statusText
  // Combine all messages into one array

  function combineMessages(messages, p2pMessages) {
    const allMessages = {}

    messages.forEach((message) => {
      if (message.type !== 'PING') {
        allMessages[message.clusterName + message.type + message.subtype] = {
          timestamp: message.timestamp || new Date().toISOString(),
          type: message.type,
          subtype: message.subtype,
          value: message.value,
          base_url: message.clusterName,
          latency: new Date() - new Date(message.timestamp),
        }
      }
    })

    p2pMessages.forEach((message) => {
      if (message.type !== 'PING') {
        allMessages[message.base_url + message.type] = {
          timestamp: message.timestamp || new Date().toISOString(),
          type: message.type,
          subtype: '',
          value: '',
          base_url: message.base_url,
          latency: new Date() - new Date(message.timestamp),
        }
      }
    })

    return allMessages
  }

  $: allMessages = combineMessages($messages, $p2pMessages)
</script>

<div class="url">
  {$wsUrl}
  {#if $error && $error.message}
    <span class="error-message">{$error.message}</span>
  {/if}
  {$p2pWsUrl}
  {#if $p2pError && $p2pError.message}
    <span class="error-message">{$p2pError.message}</span>
  {/if}
</div>

<div class="box-container">
  {#each $miningNodes as node (node.timestamp)}
    <div class="boxs">
      <span>{node.base_url}</span>
      <span>{node.hash}</span>
    </div>
  {/each}
</div>

<div class="box-container">
  <!-- Get each message from the allMessages map -->
  {#each Object.entries(allMessages) as message (message[0])}
    <div class="box">
      <span>{message[1].base_url}</span>
      {message[1].timestamp.replace('T', ' ')} ({message[1].latency}ms)
      {message[1].type}
      {message[1].subtype}
      {message[1].value || message[1].hash}
      {message[1].base_url}
    </div>
  {/each}
</div>

<style>
  .box-container {
    display: flex;
    flex-wrap: wrap;
  }

  .box {
    width: 400px; /* Fixed width */
    height: 100px; /* Fixed height */
    margin: 10px; /* Spacing between boxes */
    background-color: #00d1b2; /* Example background color */
    border: 1px solid #ccc; /* Example border */
    display: flex;
    justify-content: center;
    align-items: center;
    color: white;
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
