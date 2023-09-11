import { writable, get } from 'svelte/store'

// Create writable stores
export const nodes = writable([])
export const lastUpdated = writable(new Date())
export const loading = writable(false)
export const error = writable('')

// Create writable store for selectedNode with local storage handling
export const selectedNode = (() => {
  let savedSelectedNode = loadSelectedNodeFromLocalStorage()
  // if the saveSelectedNode does not exist in our list or nodes, calculate it from the url.location
  if (
    !(
      savedSelectedNode &&
      get(nodes).find((node) => node.id === savedSelectedNode)
    )
  ) {
    if (!import.meta.env.SSR) {
      // Extract the node id from the URL
      if (window && window.location) {
        const url = new URL(window.location.href)
        savedSelectedNode = `${url.protocol}//${url.hostname}:${url.port}`
      }
    }
  }

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

    if (!import.meta.env.SSR) {
      // Extract the domain from the URL of this page
      if (window && window.location) {
        const url = new URL(window.location.href)
        let protocol = 'wss:'
        if (url.protocol === 'http:') {
          protocol = 'ws:'
        }

        const wsUrl = `${protocol}//${url.hostname}:8099/ws`

        let socket = new WebSocket(wsUrl)

        socket.onopen = () => {
          console.log(`WebSocket2 connection opened to ${wsUrl}`)
        }

        socket.onmessage = async (event) => {
          try {
            const data = await event.data.text()
            const json = JSON.parse(data)

            console.log('Websocket2', json)

            if (json.type === 'ADD') {
              await decorateNodesWithHeaders(json)

              let nodesData = get(nodes)
              nodesData = nodesData.concat(json)
              nodesData = nodesData.sort((a, b) => a.name.localeCompare(b.name))

              nodes.set(nodesData)
              lastUpdated.set(new Date())
            }
          } catch (error) {
            console.error('Error2 parsing WebSocket data:', error)
          }
        }

        socket.onclose = () => {
          console.log(`WebSocket2 connection closed by server (${wsUrl})`)
          socket = null
        }
      }
    }
  } catch (err) {
    console.error('Error fetching nodes:', err.message)
  } finally {
    loading.set(false)
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
