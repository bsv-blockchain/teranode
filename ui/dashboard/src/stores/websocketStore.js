import { writable } from 'svelte/store'

export const lastUpdated = writable(new Date())

let cancelFunction = null

let updateFns = []


export function addSubscriber(fn) {
  updateFns.push(fn)
}

export function removeSubscriber(fn) {
  updateFns = updateFns.filter((f) => f !== fn)
}

const updateFn = debounce((json) => {
  lastUpdated.set(new Date())
  updateFns.forEach((fn) => fn(json))
}, 1000)

export function connectToWebSocket(blobServerHTTPAddress, localMode) {
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

      updateFn(json)

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

function debounce(func, delay) {
  let timeoutId

  return function () {
    clearTimeout(timeoutId)
    const context = this
    const args = arguments

    timeoutId = setTimeout(() => {
      func.apply(context, args)
    }, delay)
  }
}
