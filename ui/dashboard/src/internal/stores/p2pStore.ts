import { writable, get } from 'svelte/store'
import type { Writable } from 'svelte/store'
//import * as api from '$internal/api'

export const messages = writable([])
export const miningNodes: any = writable({})
export const wsUrl: Writable<URL | string> = writable('')
export const error: Writable<any> = writable(null)
export const sock: Writable<any> = writable(null)
export const connectionAttempts: Writable<number> = writable(0)
// Create a persistent store for current node peer ID
function createCurrentNodePeerIDStore() {
  const STORAGE_KEY = 'teranode_current_node_peer_id'
  
  // Initialize from localStorage if available
  let initialValue: string | null = null
  if (typeof window !== 'undefined') {
    try {
      const stored = localStorage.getItem(STORAGE_KEY)
      initialValue = stored || null
    } catch (e) {
      console.warn('Failed to read currentNodePeerID from localStorage:', e)
    }
  }
  
  const { subscribe, set, update } = writable<string | null>(initialValue)
  
  return {
    subscribe,
    set: (value: string | null) => {
      set(value)
      // Persist to localStorage
      if (typeof window !== 'undefined') {
        try {
          if (value) {
            localStorage.setItem(STORAGE_KEY, value)
          } else {
            localStorage.removeItem(STORAGE_KEY)
          }
        } catch (e) {
          console.warn('Failed to save currentNodePeerID to localStorage:', e)
        }
      }
    },
    clear: () => {
      set(null)
      if (typeof window !== 'undefined') {
        try {
          localStorage.removeItem(STORAGE_KEY)
        } catch (e) {
          console.warn('Failed to clear currentNodePeerID from localStorage:', e)
        }
      }
    }
  }
}

export const currentNodePeerID = createCurrentNodePeerIDStore() // Track our own node's peer ID persistently

const maxMessages = 500
const MAX_RECONNECT_ATTEMPTS = 5
const BASE_RECONNECT_DELAY = 2000 // Start with 2 seconds

// Track if we've received the first node_status message for this session
let firstNodeStatusReceived = false

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
          // Don't reset the current node peer ID on reconnection - it should persist
          // unless we're connecting to a different backend
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

            let baseUrl = jsonData.base_url
            if (!jsonData.base_url.includes('localhost') && jsonData.base_url.includes('http:')) {
              baseUrl = baseUrl.replace('http:', 'https:')
            }

            const miningNodeSet: any = get(miningNodes)

            if (jsonData.type === 'mining_on') {
              const nodeKey = jsonData.peer_id
              const currentPeerID = get(currentNodePeerID)
              miningNodeSet[nodeKey] = {
                ...miningNodeSet[nodeKey],
                ...jsonData,
                base_url: baseUrl,
                receivedAt: new Date(),
                isCurrentNode: jsonData.peer_id === currentPeerID,
              }
              miningNodes.set(miningNodeSet)
            } else if (jsonData.type === 'node_status') {
              // Handle node_status messages - these provide comprehensive node information
              const nodeKey = jsonData.peer_id
              
              // The very first node_status message we receive should be from our own node
              // (sent immediately upon WebSocket connection by the backend)
              let currentPeerID = get(currentNodePeerID)
              if (!firstNodeStatusReceived && !currentPeerID) {
                currentNodePeerID.set(jsonData.peer_id)
                currentPeerID = jsonData.peer_id
                firstNodeStatusReceived = true
                console.log('🎯 Auto-detected current node from first node_status:', jsonData.peer_id)
              }
              
              const isCurrentNode = jsonData.peer_id === currentPeerID
              
              miningNodeSet[nodeKey] = {
                ...miningNodeSet[nodeKey],
                ...jsonData,
                base_url: baseUrl,
                receivedAt: new Date(),
                isCurrentNode: isCurrentNode,
              }
              miningNodes.set(miningNodeSet)
              // Don't return here - let it fall through to add to messages array
            } else if (jsonData.type === 'block') {
              const nodeKey = jsonData.peer_id
              const currentPeerID = get(currentNodePeerID)
              if (!miningNodeSet[nodeKey]) {
                miningNodeSet[nodeKey] = { 
                  base_url: baseUrl, 
                  peer_id: jsonData.peer_id,
                  isCurrentNode: jsonData.peer_id === currentPeerID,
                }
              }
              // Update only the specific block fields, preserving existing mining_on data
              miningNodeSet[nodeKey] = {
                ...miningNodeSet[nodeKey],
                hash: jsonData.hash,
                height: jsonData.height,
                timestamp: jsonData.timestamp,
                miner: jsonData.miner,
                receivedAt: new Date(),
              }
              miningNodes.set(miningNodeSet)
            } else if (baseUrl && jsonData.peer_id) {
              const nodeKey = jsonData.peer_id
              const currentPeerID = get(currentNodePeerID)
              if (!miningNodeSet[nodeKey]) {
                miningNodeSet[nodeKey] = {
                  base_url: baseUrl,
                  peer_id: jsonData.peer_id,
                  receivedAt: new Date(),
                  isCurrentNode: jsonData.peer_id === currentPeerID,
                }
                miningNodes.set(miningNodeSet)
              } else {
                miningNodeSet[nodeKey].receivedAt = new Date()
                miningNodes.set(miningNodeSet)
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
          // Don't reset the current node peer ID on connection close - it should persist
          // across reconnections unless we're connecting to a different backend
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
