import { writable, get } from 'svelte/store'

export const messages = writable([])

const maxMessages = 100

export function connectToP2PServer() {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    url.protocol = url.protocol === 'http:' ? 'ws' : 'wss'
    url.port = '9906'
    url.pathname = '/ws'

  let socket = new WebSocket(url)

  socket.onopen = () => {
    console.log(`p2pWS connection opened to ${url}`)
  }

  socket.onmessage = async (event) => {
    try {
      const data = await event.data
      const json = JSON.parse(data)

      json.receivedAt = new Date()

      console.log('p2p', json)
      let m = get(messages)
      m = [...m, json].slice(-maxMessages)

      messages.set(m)

    } catch (error) {
      console.error('p2pWS: Error parsing WebSocket data:', error)
    }
  }


  socket.onclose = () => {
    console.log(`p2pWS connection closed by server (${wsUrl})`)
    socket = null

    setTimeout(() => {
      connectToP2PServer()
    }, 5000) // Adjust the delay as necessary
  }
}
}
