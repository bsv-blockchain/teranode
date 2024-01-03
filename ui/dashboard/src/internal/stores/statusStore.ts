import { writable, get } from 'svelte/store'
import type { Writable } from 'svelte/store'

export const messages: Writable<any[]> = writable([])
export const wsUrl: Writable<any> = writable('')
export const error: Writable<any> = writable(null)
export const sock: Writable<any> = writable(null)

const maxMessages = 100

export function connectToStatusServer() {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    url.protocol = url.protocol === 'http:' ? 'ws' : 'wss'
    url.port = '9908'
    url.pathname = '/ws'

    wsUrl.set(url)
    error.set(null)

    let socket: any = new WebSocket(url)

    socket.onerror = (event) => {
      error.set(event)
      console.log('WebSocket Error:', event)
    }

    socket.onopen = () => {
      error.set(null)
      sock.set(socket)
      console.log(`p2pWS connection opened to ${url}`)
    }

    socket.onmessage = async (event) => {
      try {
        const data = await event.data
        const json = JSON.parse(data)

        // json is sometimes an array and sometimes an object
        const jsonArr = Array.isArray(json) ? json : [json]
        const msgs = get(messages)

        const output = jsonArr
          .map((item) => ({ ...item, receivedAt: new Date() }))
          .concat(msgs)
          .slice(0, maxMessages)

        messages.set(output)
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
        connectToStatusServer()
      }, 5000) // Adjust the delay as necessary
    }
  }
}
