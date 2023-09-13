import { writable, get } from 'svelte/store'
import { getNodeHeaders }  from '@stores/bootstrapStore.js'

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

export function connectToBlobServer(blobServerHTTPAddress) {
  const url = new URL(blobServerHTTPAddress)
  const wsUrl = `${url.protocol === 'http:' ? 'ws' : 'wss'}://${
    url.hostname
  }:8090/ws`

  let socket = new WebSocket(wsUrl)

  socket.onopen = () => {
    console.log(`BlobserverWS connection opened to ${wsUrl}`)
  }

  socket.onmessage = async (event) => {
    try {
      const data = await event.data
      const json = JSON.parse(data)

      console.log('BlobserverWS', json)

      if (json.type === 'Block') {
        // Get the node from the list of nodes
        const headers = await getNodeHeaders(json.base_url)

        // Update the node with the new block
        const nodesData = get(nodes)
        const index = nodesData.findIndex(
          (node) => node.blobServerHTTPAddress === json.base_url
        )

        nodesData[index].headers = headers
        nodes.set(nodesData)
      }

    } catch (error) {
      console.error('BlobserverWS: Error parsing WebSocket data:', error)
    }
  }

  socket.onclose = () => {
    console.log(`BlobserverWS connection closed by server (${wsUrl})`)
    socket = null
  }
}

// Promise to resolve after a certain time for timeout handling
function timeout(ms) {
  return new Promise((resolve, reject) =>
    setTimeout(() => reject(new Error('Promise timed out')), ms)
  )
}


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
