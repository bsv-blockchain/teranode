import { writable, get } from 'svelte/store'
import type { Writable } from 'svelte/store'
//import * as api from '$internal/api'

export const messages = writable([])
export const miningNodes: any = writable({})
export const wsUrl: Writable<any> = writable('')
export const error: Writable<any> = writable(null)
export const sock: Writable<any> = writable(null)
export const connectionAttempts: Writable<number> = writable(0)

const maxMessages = 100
const MAX_RECONNECT_ATTEMPTS = 5
const BASE_RECONNECT_DELAY = 2000 // Start with 2 seconds

export function connectToP2PServer() {
  // Reset connection attempts when manually connecting
  connectionAttempts.set(0)
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    url.protocol = url.protocol === 'http:' ? 'ws' : 'wss'

    // Determine the correct WebSocket URL based on the current environment
    if (url.host.includes('localhost:517') || url.host.includes('localhost:417')) {
      // Development environment
      url.protocol = 'ws:'
      url.host = 'localhost'
      url.port = '9906'
    } else if (url.host.includes('localhost')) {
      // Local environment but different port
      url.protocol = 'ws:'
      // Keep the host as localhost but ensure we have the right port for the WebSocket
      // Use port 9906 for the WebSocket connection (P2P_HTTP_PORT)
      url.port = '9906'
    }

    url.pathname = '/p2p-ws'

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
        // This is required to trigger connect on server side since server expects
        // initial connect request from a WebSocket unidirectional client.
        socket.send(JSON.stringify({}));
        console.log(`p2pWS connection opened to ${url}`)
      }

      socket.onmessage = async (event: any) => {
        try {
          const data = await event.data
          const json: any = JSON.parse(data)

          if (json.connect) {
            const clientID = json.connect.client;
            const subscriptions: string[] = [];
            const subs = json.connect.subs;
            if (subs) {
              for (const m in subs) {
                if (Object.prototype.hasOwnProperty.call(subs, m)) {
                  subscriptions.push(m);
                }
              }
            }
            console.log("🟢 connected with client ID " + clientID + " and subscriptions: " + JSON.stringify(subscriptions));
            return
          }

          if (!json?.pub?.data) {
            console.log('p2pWS: Received data:', json)
            return
          }

          const jsonData = json.pub.data

          jsonData.receivedAt = new Date()

          let baseUrl = jsonData.base_url
          if (!jsonData.base_url.includes('localhost') && jsonData.base_url.includes('http:')) {
            baseUrl = baseUrl.replace('http:', 'https:')
          }

          const miningNodeSet: any = get(miningNodes)
          if (jsonData.type === 'mining_on') {
            miningNodeSet[baseUrl] = jsonData
            miningNodes.set(miningNodeSet)
          } else if (baseUrl && !miningNodeSet[baseUrl]) {
            miningNodeSet[baseUrl] = {
              base_url: baseUrl,
            }
            // const res: any = await api.getBestBlockHeaderFromServer({ baseUrl })
            // if (res.ok && res.data) {
            //   miningNodeSet[baseUrl] = {
            //     ...res.data,
            //     tx_count: res.data.txCount,
            //     size_in_bytes: res.data.sizeInBytes,
            //     base_url: baseUrl,
            //   }
            // }
            miningNodes.set(miningNodeSet)
          } else if (miningNodeSet[baseUrl]) {
            miningNodeSet[baseUrl].receivedAt = new Date()
            miningNodes.set(miningNodeSet)
          }
          //console.log('miningNodes', miningNodes)

          let m: any = get(messages)
          m = [jsonData, ...m].slice(0, maxMessages)

          messages.set(m)
        } catch (error) {
          console.error('p2pWS: Error parsing WebSocket data:', error)
        }
      }

      socket.onclose = () => {
        error.set(new Error('closed'))
        console.log(`p2pWS connection closed by server (${url})`)
        socket = null
        sock.set(null)

        const attempts = get(connectionAttempts) + 1
        connectionAttempts.set(attempts)

        if (attempts <= MAX_RECONNECT_ATTEMPTS) {
          // Implement exponential backoff
          const reconnectDelay = Math.min(30000, BASE_RECONNECT_DELAY * Math.pow(1.5, attempts - 1))
          console.log(`Attempting to reconnect (${attempts}/${MAX_RECONNECT_ATTEMPTS}) in ${reconnectDelay/1000} seconds`)

          setTimeout(() => {
            connectToP2PServer()
          }, reconnectDelay)
        } else {
          console.log('Maximum reconnection attempts reached. Please refresh the page to try again.')
        }
      }
    } catch (err: any) {
      console.error('Error creating WebSocket connection:', err)
      error.set(err)

      const attempts = get(connectionAttempts) + 1
      connectionAttempts.set(attempts)

      if (attempts <= MAX_RECONNECT_ATTEMPTS) {
        const reconnectDelay = Math.min(30000, BASE_RECONNECT_DELAY * Math.pow(1.5, attempts - 1))
        console.log(`Error connecting. Retrying (${attempts}/${MAX_RECONNECT_ATTEMPTS}) in ${reconnectDelay/1000} seconds`)

        setTimeout(() => {
          connectToP2PServer()
        }, reconnectDelay)
      }
    }
  }
}
