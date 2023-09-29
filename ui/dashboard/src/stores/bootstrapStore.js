import { writable, get } from 'svelte/store'

export const lastUpdated = writable(new Date())
export const nodes = writable([])

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

export const selectedNode = writable('', (set) => {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    if (url.host.includes('localhost:517')) {
      url.protocol = 'http:'
      url.host = 'localhost'
      url.port = '8090'
    }
    if (url.port === '') {
      set(`${url.protocol}//${url.hostname}`)
    } else {
      set(`${url.protocol}//${url.hostname}:${url.port}`)
    }
  }

  return set
})

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

  // In a url x.scaling-miner.ubsv.dev, replace x with bootstrap
  let host = url.hostname

  const parts = url.hostname.split('.')
  if (parts.length > 1) {
    host = 'bootstrap.' + parts.slice(1).join('.')
  }
  const wsUrl = `${url.protocol === 'http:' ? 'ws' : 'wss'}://${host}:8099/ws`

  let socket = new WebSocket(wsUrl)

  socket.onopen = () => {
    console.log(`BootstrapWS connection opened to ${wsUrl}`)
    lastUpdated.set(new Date())
  }

  socket.onmessage = async (event) => {
    try {
      const data = await event.data
      const json = JSON.parse(data)

      if (json.type === 'ADD') {
        const header = await getNodeHeader(json.blobServerHTTPAddress)
        json.header = header

        console.log('BootstrapWS', json)
        let nodesData = get(nodes)
        if (!nodesData.find((node) => node.ip === json.ip)) {
          nodesData.push(json)
        }

        // sort the nodesData by name
        nodesData = nodesData.sort((a, b) => {
          if (a.name < b.name) {
            return -1
          } else if (a.name > b.name) {
            return 1
          } else {
            return 0
          }
        })

        nodes.set(nodesData)
      }

      updateFn(json)
    } catch (error) {
      console.error('BootstrapWS: Error parsing WebSocket data:', error)
    }
  }

  socket.onclose = () => {
    console.log(`BootstrapWS connection closed by server (${wsUrl})`)
    socket = null
    // Reconnect logic can be added here if needed

    setTimeout(() => {
      connectToBootstrap(blobServerHTTPAddress)
    }, 5000) // Adjust the delay as necessary
  }

  cancelFunction = () => {
    if (socket) {
      console.log(`BootstrapWS connection closed by client (${wsUrl})`)
      socket.close()
    }
  }
}

export async function getNodeHeader(address) {
  if (address) {
    try {
      const header = await Promise.race([
        getBestBlockHeader(address),
        timeout(1000),
      ])
      return header || { error: 'timeout' }
    } catch (err) {
      const error = `Error fetching header for node ${address}: ${err.message}`
      return { error }
    }
  } else {
    return {}
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

// Save the selected node to local storage
