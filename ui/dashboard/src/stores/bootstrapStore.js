import { writable, get } from 'svelte/store'
import { nodes } from './nodeStore'

export const lastUpdated = writable(new Date())

let cancelFunction = null

let updateFns = []

export function addSubscriber(fn) {
  updateFns.push(fn)
}

export function removeSubscriber(fn) {
  updateFns = updateFns.filter((f) => f !== fn)
}

const updateFn = (json) => {
  lastUpdated.set(new Date())
  updateFns.forEach((fn) => fn(json))
}

export function connectToBootstrap(blobServerHTTPAddress) {
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
  let host = url.hostname

  const parts = url.hostname.split('.')
  if (parts.length > 1) {
    host = 'bootstrap.' + parts.slice(1).join('.')
  }
  const wsUrl = `${url.protocol === 'http:' ? 'ws' : 'wss'}://${host}:8099/ws`

  let socket = new WebSocket(wsUrl)

  socket.onopen = () => {
    console.log(`WebSocket connection opened to ${wsUrl}`)
    lastUpdated.set(new Date())
  }

  socket.onmessage = async (event) => {
    try {
      const data = await event.data
      const json = JSON.parse(data)


      if (json.type === 'ADD') {
        await decorateNodesWithHeaders(json)

        console.log('Websocket1', json)
        let nodesData = get(nodes)
        if (nodesData.find((node) => node.id === json.id)) {
          nodesData = nodesData.filter((node) => node.id !== json.id)
        }
        nodesData.push(json)

        nodes.set(nodesData)
      }

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

async function decorateNodesWithHeaders(node) {
  if (node.blobServerHTTPAddress) {
    try {
      const header = await Promise.race([
        getBestBlockHeader(node.blobServerHTTPAddress),
        timeout(1000),
      ])
      node.header = header || { error: 'timeout' }
    } catch (err) {
      const error = `Error fetching header for node ${node.blobServerHTTPAddress}: ${err.message}`
      node.header = { error }
    }
  } else {
    node.header = {}
  }
}

async function getBestBlockHeader(address) {
  const url = `${address}/bestblockheader/json`
  const response = await fetch(url)

  if (!response.ok) {
    throw new Error(`HTTP error! Status: ${response.status}`)
  }

  return await response.json()
}

// Promise to resolve after a certain time for timeout handling
function timeout(ms) {
  return new Promise((resolve, reject) =>
    setTimeout(() => reject(new Error('Promise timed out')), ms)
  )
}
