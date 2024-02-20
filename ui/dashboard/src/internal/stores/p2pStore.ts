import { writable, get } from 'svelte/store'
import type { Writable } from 'svelte/store'

export const messages = writable([])
export const miningNodes: any = writable([])
export const wsUrl: Writable<any> = writable('')
export const error: Writable<any> = writable(null)
export const sock: Writable<any> = writable(null)

const maxMessages = 100

export function connectToP2PServer() {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    url.protocol = url.protocol === 'http:' ? 'ws' : 'wss'

    if (url.hostname.includes('ubsv.dev')) {
      url.port = url.protocol === 'ws:' ? '9906' : '9904'
    }

    if (url.host.includes('localhost:517') || url.host.includes('localhost:417')) {
      url.protocol = 'ws:'
      url.host = 'localhost'
      url.port = '9906'
    }

    url.pathname = '/p2p-ws'

    wsUrl.set(url)
    error.set(null)

    let socket: any = new WebSocket(url)

    socket.onerror = (event: any) => {
      error.set(event)
      console.log('WebSocket Error:', event)
    }

    socket.onopen = () => {
      error.set(null)
      sock.set(socket)
      console.log(`p2pWS connection opened to ${url}`)
    }

    socket.onmessage = async (event: any) => {
      try {
        const data = await event.data
        const json: any = JSON.parse(data)

        json.receivedAt = new Date()

        if (json.base_url) {
          const uniqueNodes: any = {}
          const mn: any = get(miningNodes)

          for (const node of mn) {
            uniqueNodes[node.base_url] = node
          }

          uniqueNodes[json.base_url] = json

          // miningNodes.set(Object.values(uniqueNodes))

          const sorted = Object.values(uniqueNodes).sort((a: any, b: any) => {
            if (a.base_url < b.base_url) {
              return -1
            } else if (a.base_url > b.base_url) {
              return 1
            } else {
              return 0
            }
          })

          miningNodes.set(sorted)
          console.log('miningNodes', sorted)
        }

        let m: any = get(messages)
        m = [json, ...m].slice(0, maxMessages)

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

      setTimeout(() => {
        connectToP2PServer()
      }, 5000) // Adjust the delay as necessary
    }
  }
}
