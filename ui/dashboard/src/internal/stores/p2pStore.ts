import { writable, get } from 'svelte/store'
import type { Writable } from 'svelte/store'
//import * as api from '$internal/api'

export const messages: Writable<any[]> = writable([])
export const miningNodes: any = writable({})
export const wsUrl: Writable<URL | string> = writable('')
export const error: Writable<any> = writable(null)
export const sock: Writable<any> = writable(null)
export const connectionAttempts: Writable<number> = writable(0)
// Hashmap to store block hash -> miner name mapping
export const blockHashToMiner: Writable<Map<string, string>> = writable(new Map())
// Create a simple store for current node peer ID (no localStorage)
function createCurrentNodePeerIDStore() {
  const { subscribe, set, update } = writable<string | null>(null)
  
  return {
    subscribe,
    set: (value: string | null) => {
      set(value)
    },
    clear: () => {
      set(null)
    }
  }
}

export const currentNodePeerID = createCurrentNodePeerIDStore() // Track our own node's peer ID

const maxMessages = 500
const MAX_RECONNECT_ATTEMPTS = 5
const BASE_RECONNECT_DELAY = 2000 // Start with 2 seconds
const PEER_EXPIRY_TIME = 60000 // 60 seconds in milliseconds
const MAX_BLOCK_HASH_CACHE = 1000 // Maximum entries in block hash -> miner cache

// Track if we've received the first node_status message for this session
let firstNodeStatusReceived = false

// Cleanup interval reference
let cleanupInterval: any = null

// Function to clean up expired peers
function cleanupExpiredPeers() {
  const now = new Date().getTime()
  const miningNodeSet: any = get(miningNodes)
  let hasChanges = false
  
  // Check each peer and remove if last update was more than 30 seconds ago
  for (const nodeKey in miningNodeSet) {
    const node = miningNodeSet[nodeKey]
    if (node.receivedAt) {
      const lastUpdate = new Date(node.receivedAt).getTime()
      if (now - lastUpdate > PEER_EXPIRY_TIME) {
        console.log(`Removing expired peer: ${nodeKey} (last seen: ${new Date(node.receivedAt).toISOString()})`)
        delete miningNodeSet[nodeKey]
        hasChanges = true
      }
    }
  }
  
  // Only update the store if we made changes
  if (hasChanges) {
    miningNodes.set(miningNodeSet)
  }
}

// Start the cleanup interval
function startCleanupInterval() {
  // Clear any existing interval
  stopCleanupInterval()
  
  // Run cleanup every 5 seconds
  cleanupInterval = setInterval(() => {
    cleanupExpiredPeers()
  }, 5000)
  
  console.log('Started peer cleanup interval (checking every 5 seconds)')
}

// Stop the cleanup interval
function stopCleanupInterval() {
  if (cleanupInterval) {
    clearInterval(cleanupInterval)
    cleanupInterval = null
    console.log('Stopped peer cleanup interval')
  }
}

export async function connectToP2PServer() {
  // Reset connection attempts when manually connecting
  connectionAttempts.set(0)
  // Reset first node status flag for new connection
  firstNodeStatusReceived = false
  if (!import.meta.env.SSR && window && window.location) {
    try {
      // Fetch WebSocket configuration from the backend
      const response = await fetch('/api/config/websocket')
      if (!response.ok) {
        throw new Error(`Failed to fetch WebSocket config: ${response.statusText}`)
      }

      const config = await response.json()
      const url = new URL(config.websocketUrl)

      console.log(`Attempting to connect to P2P WebSocket at: ${url.toString()}`)
      wsUrl.set(url)
      error.set(null)

      let socket: any
      try {
        socket = new WebSocket(url)

        socket.onerror = (event: any) => {
          error.set(event)
          console.log('WebSocket Error:', event)
        }

        socket.onopen = () => {
          error.set(null)
          sock.set(socket)
          // Reset firstNodeStatusReceived flag for new connection
          firstNodeStatusReceived = false
          // Start the cleanup interval when connection is established
          startCleanupInterval()
          // This is required to trigger connect on server side since server expects
          // initial connect request from a WebSocket unidirectional client.
          socket.send(JSON.stringify({}))
          console.log(`p2pWS connection opened to ${url}`)
        }

        socket.onmessage = async (event: any) => {
          try {
            const data = await event.data
            const json: any = JSON.parse(data)

            if (json.connect) {
              const clientID = json.connect.client
              const subscriptions: string[] = []
              const subs = json.connect.subs
              if (subs) {
                for (const m in subs) {
                  if (Object.prototype.hasOwnProperty.call(subs, m)) {
                    subscriptions.push(m)
                  }
                }
              }
              console.log(
                '🟢 connected with client ID ' +
                  clientID +
                  ' and subscriptions: ' +
                  JSON.stringify(subscriptions),
              )
              return
            }

            // Handle both wrapped (Centrifuge) and unwrapped messages
            let jsonData
            if (json?.pub?.data) {
              jsonData = json.pub.data
            } else if (json?.type === 'node_status') {
              // Unwrapped messages: initial node_status
              jsonData = json
            } else {
              return
            }

            jsonData.receivedAt = new Date()

            const miningNodeSet: any = get(miningNodes)
            if (jsonData.type === 'node_status') {
              // Handle node_status messages - these provide comprehensive node information
              const nodeKey = jsonData.peer_id

              // The very first node_status message we receive should be from our own node
              // (sent immediately upon WebSocket connection by the backend)
              let currentPeerID = get(currentNodePeerID)
              if (!firstNodeStatusReceived) {
                // Set the current node from the first node_status message
                currentNodePeerID.set(jsonData.peer_id)
                currentPeerID = jsonData.peer_id
                firstNodeStatusReceived = true
                console.log(`Current node identified: ${jsonData.peer_id}`)
              }

              const isCurrentNode = jsonData.peer_id === currentPeerID

              const existingNode = miningNodeSet[nodeKey]
              const newNode = {
                ...miningNodeSet[nodeKey],
                ...jsonData,
                receivedAt: new Date(),
                isCurrentNode: isCurrentNode,
              }
              
              // Update block hash -> miner mapping if available
              if (jsonData.best_block_hash && jsonData.miner_name) {
                blockHashToMiner.update(map => {
                  map.set(jsonData.best_block_hash, jsonData.miner_name)
                  // Keep cache size manageable
                  if (map.size > MAX_BLOCK_HASH_CACHE) {
                    const firstKey = map.keys().next().value
                    map.delete(firstKey)
                  }
                  return map
                })
              }
              
              // Only update if significant data changed (excluding receivedAt)
              const hasChanges = !existingNode ||
                existingNode.best_height !== newNode.best_height ||
                existingNode.best_block_hash !== newNode.best_block_hash ||
                existingNode.chain_work !== newNode.chain_work ||
                existingNode.version !== newNode.version ||
                existingNode.fsm_state !== newNode.fsm_state ||
                existingNode.tx_count !== newNode.tx_count ||
                existingNode.subtree_count !== newNode.subtree_count ||
                existingNode.uptime !== newNode.uptime ||
                existingNode.listen_mode !== newNode.listen_mode ||
                existingNode.client_name !== newNode.client_name ||
                existingNode.miner_name !== newNode.miner_name ||
                existingNode.start_time !== newNode.start_time ||
                existingNode.isCurrentNode !== newNode.isCurrentNode ||
                existingNode.min_mining_tx_fee !== newNode.min_mining_tx_fee ||
                existingNode.connected_peers_count !== newNode.connected_peers_count
              
              if (hasChanges || !existingNode) {
                miningNodeSet[nodeKey] = newNode
                miningNodes.set(miningNodeSet)
              } else {
                // Just update timestamp without triggering store update
                if (existingNode) {
                  existingNode.receivedAt = new Date()
                }
              }
              // Don't return here - let it fall through to add to messages array
            } else if (jsonData.type === 'block') {
              const nodeKey = jsonData.peer_id
              const currentPeerID = get(currentNodePeerID)
              const existingNode = miningNodeSet[nodeKey]
              
              // Update block hash -> miner mapping if available
              if (jsonData.hash && jsonData.miner) {
                blockHashToMiner.update(map => {
                  map.set(jsonData.hash, jsonData.miner)
                  // Keep cache size manageable
                  if (map.size > MAX_BLOCK_HASH_CACHE) {
                    const firstKey = map.keys().next().value
                    map.delete(firstKey)
                  }
                  return map
                })
              }
              
              if (!existingNode) {
                miningNodeSet[nodeKey] = {
                  base_url: jsonData.base_url || '',
                  peer_id: jsonData.peer_id,
                  isCurrentNode: jsonData.peer_id === currentPeerID,
                  hash: jsonData.hash,
                  height: jsonData.height,
                  timestamp: jsonData.timestamp,
                  miner: jsonData.miner,
                  receivedAt: new Date(),
                }
                miningNodes.set(miningNodeSet)
              } else {
                // Only update if block data changed
                const hasChanges = 
                  existingNode.hash !== jsonData.hash ||
                  existingNode.height !== jsonData.height ||
                  existingNode.timestamp !== jsonData.timestamp ||
                  existingNode.miner !== jsonData.miner
                
                if (hasChanges) {
                  miningNodeSet[nodeKey] = {
                    ...existingNode,
                    hash: jsonData.hash,
                    height: jsonData.height,
                    timestamp: jsonData.timestamp,
                    miner: jsonData.miner,
                    receivedAt: new Date(),
                  }
                  miningNodes.set(miningNodeSet)
                } else {
                  // Just update timestamp
                  existingNode.receivedAt = new Date()
                }
              }
            } else if (jsonData.peer_id) {
              const nodeKey = jsonData.peer_id
              const currentPeerID = get(currentNodePeerID)
              if (!miningNodeSet[nodeKey]) {
                miningNodeSet[nodeKey] = {
                  peer_id: jsonData.peer_id,
                  receivedAt: new Date(),
                  isCurrentNode: jsonData.peer_id === currentPeerID,
                }
                miningNodes.set(miningNodeSet)
              } else {
                // Just update timestamp without triggering store update
                miningNodeSet[nodeKey].receivedAt = new Date()
              }
            }

            // Use update to modify the existing array instead of replacing it
            messages.update((currentMessages) => {
              // Add new message at the beginning and keep only maxMessages
              return [jsonData, ...currentMessages].slice(0, maxMessages)
            })
          } catch (error) {
            console.error('p2pWS: Error parsing WebSocket data:', error)
          }
        }

        socket.onclose = () => {
          error.set(new Error('closed'))
          console.log(`p2pWS connection closed by server (${url})`)
          socket = null
          sock.set(null)
          // Stop the cleanup interval when connection is closed
          stopCleanupInterval()
          // Reset the first node status flag so we can detect it again on reconnection
          firstNodeStatusReceived = false

          const attempts = get(connectionAttempts) + 1
          connectionAttempts.set(attempts)

          if (attempts <= MAX_RECONNECT_ATTEMPTS) {
            // Implement exponential backoff
            const reconnectDelay = Math.min(
              30000,
              BASE_RECONNECT_DELAY * Math.pow(1.5, attempts - 1),
            )
            console.log(
              `Attempting to reconnect (${attempts}/${MAX_RECONNECT_ATTEMPTS}) in ${reconnectDelay / 1000} seconds`,
            )

            setTimeout(() => {
              connectToP2PServer()
            }, reconnectDelay)
          } else {
            console.log(
              'Maximum reconnection attempts reached. Please refresh the page to try again.',
            )
          }
        }
      } catch (err: any) {
        console.error('Error creating WebSocket connection:', err)
        error.set(err)

        const attempts = get(connectionAttempts) + 1
        connectionAttempts.set(attempts)

        if (attempts <= MAX_RECONNECT_ATTEMPTS) {
          const reconnectDelay = Math.min(30000, BASE_RECONNECT_DELAY * Math.pow(1.5, attempts - 1))
          console.log(
            `Error connecting. Retrying (${attempts}/${MAX_RECONNECT_ATTEMPTS}) in ${reconnectDelay / 1000} seconds`,
          )

          setTimeout(() => {
            connectToP2PServer()
          }, reconnectDelay)
        }
      }
    } catch (err: any) {
      console.error('Error fetching WebSocket configuration:', err)
      error.set(err)

      // If we can't fetch the config, fall back to the default behavior
      const attempts = get(connectionAttempts) + 1
      connectionAttempts.set(attempts)

      if (attempts <= MAX_RECONNECT_ATTEMPTS) {
        const reconnectDelay = Math.min(30000, BASE_RECONNECT_DELAY * Math.pow(1.5, attempts - 1))
        console.log(
          `Error fetching config. Retrying (${attempts}/${MAX_RECONNECT_ATTEMPTS}) in ${reconnectDelay / 1000} seconds`,
        )

        setTimeout(() => {
          connectToP2PServer()
        }, reconnectDelay)
      }
    }
  }
}

// Export cleanup function for manual use if needed
export function cleanupPeers() {
  cleanupExpiredPeers()
}

// Clean up on module unload (for HMR in development)
if (typeof window !== 'undefined') {
  window.addEventListener('beforeunload', () => {
    stopCleanupInterval()
  })
}
