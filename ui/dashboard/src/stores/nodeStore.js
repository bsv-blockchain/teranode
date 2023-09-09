import { writable, get } from 'svelte/store'

// Create writable stores
export const nodes = writable([])
export const lastUpdated = writable(new Date())
export const loading = writable(false)
export const error = writable('')

// Create a writeable store for localMode with local storage handling
export const localMode = (() => {
  const savedLocalMode = loadLocalModeFromLocalStorage()
  const { subscribe, set } = writable(savedLocalMode)

  return {
    subscribe,
    set: (newValue) => {
      saveLocalModeToLocalStorage(newValue)
      set(newValue)
    },
  }
})()

// Create writable store for selectedNode with local storage handling
export const selectedNode = (() => {
  const savedSelectedNode = loadSelectedNodeFromLocalStorage()
  const { subscribe, set } = writable(savedSelectedNode)

  return {
    subscribe,
    set: (newValue) => {
      saveSelectedNodeToLocalStorage(newValue)
      set(newValue)
    },
  }
})()

export async function getNodes() {
  try {
    error.set('')
    loading.set(true)

    const bootstrapServer = get(localMode)
      ? 'https://localhost:8099'
      : 'https://bootstrap.ubsv.dev:8099'

    const response = await fetch(`${bootstrapServer}/nodes`)

    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`)
    }

    let nodesData = await response.json()

    await decorateNodesWithHeaders(nodesData)

    nodesData = nodesData.sort((a, b) => a.name.localeCompare(b.name))

    nodes.set(nodesData)
    lastUpdated.set(new Date())
  } catch (err) {
    console.error('Error fetching nodes:', err.message)
  } finally {
    loading.set(false)

    setTimeout(getNodes, 10000) // Try again in 10s
  }
}

// Promise to resolve after a certain time for timeout handling
function timeout(ms) {
  return new Promise((resolve, reject) =>
    setTimeout(() => reject(new Error('Promise timed out')), ms)
  )
}

async function decorateNodesWithHeaders(nodesData) {
  await Promise.all(
    nodesData.map(async (node) => {
      if (node.blobServerHTTPAddress) {
        try {
          const header = await Promise.race([
            getBestBlockHeader(node.blobServerHTTPAddress),
            timeout(1000),
          ])
          node.header = header || { error: 'timeout' }
        } catch (error) {
          console.error(
            `Error fetching header for node ${node.blobServerHTTPAddress}:`,
            error.message
          )
          node.header = { error: 'timeout' }
        }
      } else {
        node.header = {}
      }
    })
  )
}

async function getBestBlockHeader(address) {
  const url = `${address}/bestblockheader/json`
  const response = await fetch(url)

  if (!response.ok) {
    throw new Error(`HTTP error! Status: ${response.status}`)
  }

  return await response.json()
}

// Create a IEFE to start the websocket connection
;(async () => {
  await getNodes()
})()

// Save the selected node to local storage
function saveSelectedNodeToLocalStorage(nodeId) {
  if (typeof window !== 'undefined') {
    localStorage.setItem('selectedNode', nodeId)
  }
}

// Load the selected node from local storage
function loadSelectedNodeFromLocalStorage() {
  if (typeof window !== 'undefined') {
    return localStorage.getItem('selectedNode')
  }
}

function saveLocalModeToLocalStorage(localMode) {
  if (typeof window !== 'undefined') {
    return localStorage.setItem('localMode', localMode ? 'true' : 'false')
  }
}

function loadLocalModeFromLocalStorage() {
  if (typeof window !== 'undefined') {
    return localStorage.getItem('localMode') === 'true'
  }
}
