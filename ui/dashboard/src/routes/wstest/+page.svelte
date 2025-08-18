<script lang="ts">
  import { onMount } from 'svelte'
  import { Button, TextInput } from '$lib/components'
  import Card from '$internal/components/card/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  
  let wsUrl = 'ws://localhost:9906/p2p-ws'
  let socket: WebSocket | null = null
  let connected = false
  let messages: any[] = []
  let firstNodeStatus: any = null
  let connectionLog: string[] = []
  
  function addLog(msg: string) {
    connectionLog = [...connectionLog, `[${new Date().toISOString()}] ${msg}`]
  }
  
  function connect() {
    if (socket) {
      socket.close()
    }
    
    messages = []
    firstNodeStatus = null
    connectionLog = []
    
    addLog(`Connecting to ${wsUrl}...`)
    
    try {
      socket = new WebSocket(wsUrl)
      
      socket.onopen = () => {
        connected = true
        addLog('WebSocket connected')
        addLog('Sending initial connect message...')
        socket!.send(JSON.stringify({}))
      }
      
      socket.onmessage = (event) => {
        const data = JSON.parse(event.data)
        
        // Check if this is the connect response
        if (data.connect) {
          addLog(`Received connect response: client=${data.connect.client}`)
          if (data.connect.subs) {
            addLog(`Subscriptions: ${Object.keys(data.connect.subs).join(', ')}`)
          }
          return
        }
        
        // Check if this is a node_status message
        if (data.type === 'node_status' || data?.pub?.data?.type === 'node_status') {
          const nodeData = data?.pub?.data || data
          
          // Capture the first node_status
          if (!firstNodeStatus) {
            firstNodeStatus = nodeData
            addLog(`FIRST NODE_STATUS RECEIVED:`)
            addLog(`  peer_id: ${nodeData.peer_id}`)
            addLog(`  base_url: ${nodeData.base_url}`)
            addLog(`  client_name: ${nodeData.client_name || '(not set)'}`)
            addLog(`  fsm_state: ${nodeData.fsm_state}`)
            addLog(`  is wrapped: ${!!data?.pub?.data}`)
          }
          
          messages = [...messages, {
            time: new Date().toISOString(),
            type: nodeData.type,
            peer_id: nodeData.peer_id,
            client_name: nodeData.client_name,
            wrapped: !!data?.pub?.data,
            raw: nodeData
          }]
        } else {
          // Other message types
          const msgType = data?.pub?.data?.type || data.type || 'unknown'
          messages = [...messages, {
            time: new Date().toISOString(),
            type: msgType,
            wrapped: !!data?.pub?.data,
            raw: data?.pub?.data || data
          }]
        }
      }
      
      socket.onerror = (error) => {
        addLog(`WebSocket error: ${error}`)
      }
      
      socket.onclose = () => {
        connected = false
        addLog('WebSocket disconnected')
        socket = null
      }
    } catch (error) {
      addLog(`Failed to connect: ${error}`)
    }
  }
  
  function disconnect() {
    if (socket) {
      socket.close()
      socket = null
    }
  }
  
  onMount(() => {
    return () => {
      if (socket) {
        socket.close()
      }
    }
  })
</script>

<div class="container">
  <Card>
    <div slot="title">
      <Typo variant="title" size="h4" value="WebSocket Test Tool" />
    </div>
    
    <div class="controls">
      <TextInput
        name="wsUrl"
        label="WebSocket URL"
        bind:value={wsUrl}
        disabled={connected}
      />
      
      {#if !connected}
        <Button on:click={connect} variant="primary">Connect</Button>
      {:else}
        <Button on:click={disconnect} variant="danger">Disconnect</Button>
      {/if}
      
      <div class="status">
        Status: <span class:connected>{connected ? 'Connected' : 'Disconnected'}</span>
      </div>
    </div>
    
    {#if firstNodeStatus}
      <div class="first-node-panel">
        <h3>First Node Status Received</h3>
        <div class="first-node-info">
          <div class="info-row">
            <span class="label">Peer ID:</span>
            <span class="value">{firstNodeStatus.peer_id}</span>
          </div>
          <div class="info-row">
            <span class="label">Client Name:</span>
            <span class="value {firstNodeStatus.client_name ? '' : 'missing'}">{firstNodeStatus.client_name || '(not set)'}</span>
          </div>
          <div class="info-row">
            <span class="label">Base URL:</span>
            <span class="value">{firstNodeStatus.base_url}</span>
          </div>
          <div class="info-row">
            <span class="label">FSM State:</span>
            <span class="value">{firstNodeStatus.fsm_state}</span>
          </div>
        </div>
      </div>
    {/if}
    
    <div class="log-section">
      <h3>Connection Log</h3>
      <div class="log">
        {#each connectionLog as log}
          <div class="log-entry">{log}</div>
        {/each}
      </div>
    </div>
    
    <div class="messages-section">
      <h3>Messages ({messages.length})</h3>
      <div class="messages">
        {#each messages as msg (msg.time)}
          <div class="message">
            <div class="message-header">
              <span class="time">{msg.time}</span>
              <span class="type">{msg.type}</span>
              {#if msg.wrapped}
                <span class="wrapped">wrapped</span>
              {/if}
            </div>
            {#if msg.peer_id}
              <div class="message-info">
                peer_id: {msg.peer_id}
                {#if msg.client_name}
                  | client: {msg.client_name}
                {/if}
              </div>
            {/if}
            <details>
              <summary>Raw Data</summary>
              <pre>{JSON.stringify(msg.raw, null, 2)}</pre>
            </details>
          </div>
        {/each}
      </div>
    </div>
  </Card>
</div>

<style>
  .container {
    padding: 20px;
    max-width: 1200px;
    margin: 0 auto;
  }
  
  .controls {
    display: flex;
    gap: 20px;
    align-items: flex-end;
    margin-bottom: 20px;
  }
  
  .status {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
  }
  
  .status span {
    font-weight: bold;
    color: #ff4444;
  }
  
  .status span.connected {
    color: #44ff44;
  }
  
  .first-node-panel {
    background: rgba(74, 158, 255, 0.1);
    border: 1px solid #4a9eff;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 20px;
  }
  
  .first-node-panel h3 {
    margin: 0 0 12px 0;
    color: #4a9eff;
  }
  
  .first-node-info {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  
  .info-row {
    display: flex;
    gap: 12px;
  }
  
  .info-row .label {
    font-weight: bold;
    min-width: 120px;
    color: rgba(255, 255, 255, 0.7);
  }
  
  .info-row .value {
    color: rgba(255, 255, 255, 0.9);
    font-family: 'JetBrains Mono', monospace;
  }
  
  .info-row .value.missing {
    color: #ff9999;
    font-style: italic;
  }
  
  .log-section, .messages-section {
    margin-top: 20px;
  }
  
  .log-section h3, .messages-section h3 {
    margin-bottom: 10px;
    color: rgba(255, 255, 255, 0.9);
  }
  
  .log {
    background: rgba(0, 0, 0, 0.3);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 4px;
    padding: 10px;
    max-height: 200px;
    overflow-y: auto;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
  }
  
  .log-entry {
    color: rgba(255, 255, 255, 0.8);
    margin-bottom: 4px;
  }
  
  .messages {
    background: rgba(0, 0, 0, 0.3);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 4px;
    padding: 10px;
    max-height: 400px;
    overflow-y: auto;
  }
  
  .message {
    background: rgba(255, 255, 255, 0.05);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 4px;
    padding: 10px;
    margin-bottom: 10px;
  }
  
  .message-header {
    display: flex;
    gap: 12px;
    align-items: center;
    margin-bottom: 8px;
  }
  
  .time {
    font-size: 11px;
    color: rgba(255, 255, 255, 0.5);
    font-family: 'JetBrains Mono', monospace;
  }
  
  .type {
    font-weight: bold;
    color: #4a9eff;
  }
  
  .wrapped {
    background: rgba(255, 193, 7, 0.2);
    color: #ffc107;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 11px;
  }
  
  .message-info {
    font-size: 12px;
    color: rgba(255, 255, 255, 0.7);
    margin-bottom: 8px;
    font-family: 'JetBrains Mono', monospace;
  }
  
  details {
    margin-top: 8px;
  }
  
  summary {
    cursor: pointer;
    color: rgba(255, 255, 255, 0.6);
    font-size: 12px;
  }
  
  pre {
    background: rgba(0, 0, 0, 0.5);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 4px;
    padding: 8px;
    margin-top: 8px;
    font-size: 11px;
    overflow-x: auto;
    color: rgba(255, 255, 255, 0.8);
  }
</style>