import { writable, get } from 'svelte/store'

export const messages = writable([])
export const miningNodes = writable([])
export const wsUrl = writable('')
export const error = writable(null)

const maxMessages = 100

export function connectToP2PServer() {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    url.protocol = url.protocol === 'http:' ? 'ws' : 'wss'
    url.port = '9906'
    url.pathname = '/ws'

    wsUrl.set(url)
    error.set(null)

    let socket = new WebSocket(url)

    socket.onerror = (event) => {
      error.set(event)
      console.log("WebSocket Error:", event);
    }

    socket.onopen = () => {
      error.set(null)
      console.log(`p2pWS connection opened to ${url}`)
    }

    socket.onmessage = async (event) => {
      try {
        const data = await event.data
        let json = JSON.parse(data)

        json.receivedAt = new Date()

        if (json.type === 'mining_on') {
          const uniqueNodes = {}
          let mn = get(miningNodes)

          for (const node of mn) {
            uniqueNodes[node.base_url] = node
          }

          uniqueNodes[json.base_url] = json

          const sorted = Object.values(uniqueNodes).sort((a, b) => {
            if (a.base_url < b.base_url) {
              return -1
            } else if (a.base_url > b.base_url) {
              return 1
            } else {
              return 0
            }
          })

          miningNodes.set(sorted)
        }

        let m = get(messages)
        m = ([json, ...m]).slice(0, maxMessages)

        messages.set(m)

      } catch (error) {
        console.error('p2pWS: Error parsing WebSocket data:', error)
      }
    }


    socket.onclose = () => {
      error.set(new Error("closed"))
      console.log(`p2pWS connection closed by server (${url})`)
      socket = null

      setTimeout(() => {
        connectToP2PServer()
      }, 5000) // Adjust the delay as necessary
    }
  }
}
