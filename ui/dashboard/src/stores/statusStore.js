import { writable, get } from 'svelte/store'

export const messages = writable([])
export const wsUrl = writable('')
export const error = writable(null)

const maxMessages = 100

export function connectToStatusServer() {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    url.protocol = url.protocol === 'http:' ? 'ws' : 'wss'
    url.port = '9908'
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
