import { writable, get } from 'svelte/store'

export const messages = writable([])
export const wsUrl = writable('')

const maxMessages = 100

export function connectToP2PServer() {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    url.protocol = url.protocol === 'http:' ? 'ws' : 'wss'
    url.port = '9906'
    url.pathname = '/ws'

    wsUrl.set(url)

    let socket = new WebSocket(url)

    socket.onerror = (event) => {
      console.log("WebSocket Error:", event);
    }

    socket.onopen = () => {
      console.log(`p2pWS connection opened to ${url}`)
    }

    socket.onmessage = async (event) => {
      try {
        const data = await event.data
        let json = JSON.parse(data)

        json.receivedAt = new Date()

        // if (json.type === 'block') {
        //   const loc = `${json.base_url}/header/${json.hash}/json`
        //   try {
        //     const res2 = await fetch(loc)
        //     const json2 = await res2.json()
        //     json = { ...json, ...json2 }
        //   } catch (error) {
        //     console.error(`p2pWS: Error fetching block header (${loc}):`, error)
        //   }
        // }

        let m = get(messages)
        m = [json, ...m].slice(0, maxMessages)

        messages.set(m)

      } catch (error) {
        console.error('p2pWS: Error parsing WebSocket data:', error)
      }
    }


    socket.onclose = () => {
      console.log(`p2pWS connection closed by server (${url})`)
      socket = null

      setTimeout(() => {
        connectToP2PServer()
      }, 5000) // Adjust the delay as necessary
    }
  }
}
