import { writable } from 'svelte/store'

export const lastUpdated = writable(new Date())

let cancelFunction = null

export function connectToWebSocket(
  blobServerHTTPAddress,
  localMode,
  updateFn
) {
  if (typeof WebSocket === 'undefined') {
    return
  }

  if (!blobServerHTTPAddress) {
    return
  }

  if (cancelFunction) {
    cancelFunction()
    cancelFunction = null
  }

  const url = new URL(blobServerHTTPAddress)
  const wsUrl = localMode ? 'wss://localhost:8090/ws' : `wss://${url.host}/ws`

  let socket = new WebSocket(wsUrl)

  socket.onopen = () => {
    console.log(`WebSocket connection opened to ${wsUrl}`)
    lastUpdated.set(new Date())
  }

  socket.onmessage = async (event) => {
    try {
      const data = await event.data.text()
      const json = JSON.parse(data)
      if (json.type === 'Block') {
        lastUpdated.set(new Date())
        if (updateFn) {
          updateFn()
        }
      }
    } catch (error) {
      console.error('Error parsing WebSocket data:', error)
    }
  }

  socket.onclose = () => {
    console.log(`WebSocket connection closed by server (${wsUrl})`)
    socket = null
    // Reconnect logic can be added here if needed
  }

  cancelFunction = () => {
    if (socket) {
      console.log(`WebSocket connection closed by client (${wsUrl})`)
      socket.close()
    }
  }
}
