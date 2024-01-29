import { writable, get } from 'svelte/store'
import type { Writable } from 'svelte/store'
import { nodes, getNodeHeader } from './bootstrapStore'
import { onMessage } from './chainStore'

// Create writable stores
export const lastUpdated = writable(new Date())
export const loading = writable(false)
export const sock: Writable<any> = writable(null)

let updateFns = [onMessage]

export function addSubscriber(fn: any) {
  updateFns.push(fn)
}

export function removeSubscriber(fn: any) {
  updateFns = updateFns.filter((f) => f !== fn)
}

const updateFn = (json: string) => {
  lastUpdated.set(new Date())
  updateFns.forEach(async (fn) => await fn(json))
}

export const assetHTTPAddress = writable('', (set: any) => {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    // let dev and preview mode connect to server
    if (url.host.includes('localhost:517') || url.host.includes('localhost:417')) {
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

export function connectToAsset(assetHTTPAddress: string) {
  const url = new URL(assetHTTPAddress)
  const port = url.port || (url.protocol === 'http:' ? '80' : '443')
  const wsUrl = `${url.protocol === 'http:' ? 'ws' : 'wss'}://${url.hostname}:${port}/ws`

  let socket: any = new WebSocket(wsUrl)

  socket.onopen = () => {
    sock.set(socket)
    console.log(`AssetWS connection opened to ${wsUrl}`)
  }

  socket.onmessage = async (event: any) => {
    try {
      const data = await event.data
      const json = JSON.parse(data)

      if (json.type === 'Block') {
        // Get the node from the list of nodes
        const header = await getNodeHeader(json.base_url)

        // Update the node with the new block
        let nodesData: any[] = get(nodes)
        const index = nodesData.findIndex((node) => node.assetHTTPAddress === json.base_url)

        if (index === -1) {
          json.header = header
          nodesData.push(json)
        } else {
          nodesData[index].header = header
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
      console.error('AssetWS: Error parsing WebSocket data:', error)
    }
  }

  socket.onclose = () => {
    console.log(`AssetWS connection closed by server (${wsUrl})`)
    socket = null
    sock.set(null)

    setTimeout(() => {
      connectToAsset(assetHTTPAddress)
    }, 5000) // Adjust the delay as necessary
  }
}
