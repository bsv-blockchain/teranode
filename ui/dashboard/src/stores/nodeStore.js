import { writable, get } from 'svelte/store'
import { getNodeHeader } from '@stores/bootstrapStore.js'
import { nodes } from '@stores/bootstrapStore.js'

// Create writable stores
export const lastUpdated = writable(new Date())
export const loading = writable(false)

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
        const header = await getNodeHeader(json.base_url)

        // Update the node with the new block
        const nodesData = get(nodes)
        const index = nodesData.findIndex(
          (node) => node.blobServerHTTPAddress === json.base_url
        )

        nodesData[index].header = header

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
      console.error('BlobserverWS: Error parsing WebSocket data:', error)
    }
  }

  socket.onclose = () => {
    console.log(`BlobserverWS connection closed by server (${wsUrl})`)
    socket = null

    setTimeout(() => {
       connectToBlobServer(blobServerHTTPAddress)
    }, 5000); // Adjust the delay as necessary

  }
}
