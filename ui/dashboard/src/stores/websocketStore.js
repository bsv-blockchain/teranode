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

export function connectToWebSocket(blobServerHTTPAddress) {
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

  // In a url x.scaling.ubsv.dev, replace x with bootstrap
  const parts = url.hostname.split('.')
  const host = 'bootstrap' + parts.slice(1).join('.')
  const wsUrl = `${url.protocol === 'http:' ? 'ws' : 'wss'}://${
    host
  }:8099/ws`

  let socket = new WebSocket(wsUrl)

  socket.onopen = () => {
    console.log(`WebSocket connection opened to ${wsUrl}`)
    lastUpdated.set(new Date())
  }

  socket.onmessage = async (event) => {
    try {
      const data = await event.data
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
