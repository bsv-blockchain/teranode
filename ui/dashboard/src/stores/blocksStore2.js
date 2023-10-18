import { writable, get } from 'svelte/store'

export const blocks = writable([])
export const error = writable('')
export const loading = writable(false)
export const wsUrl = writable('')

const numberOfBlocks = 100

const serviceName = 'BlocksService'
const uniqueData = new Set();

export function startWebSocket() {
  if (typeof WebSocket === 'undefined') {
    return
  }


  let socket = new WebSocket('ws:///ws')

  socket.onopen = () => {
    console.log(`${serviceName} connection opened to ${wsUrl}`)
  }

  socket.onmessage = async (event) => {
    try {
      const data = await event.data
      const json = JSON.parse(data)

      if (json.type === 'Block') {
        // Get the node from the list of nodes
        const header = await getNodeHeader(json.base_url)

        // Update the node with the new block
        let nodesData = get(nodes)
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

    } catch (error) {
      console.error(`${serviceName}: Error parsing WebSocket data:`, error)
    }
  }

  socket.onclose = () => {
    console.log(`${serviceName} connection closed by server (${wsUrl})`)
    socket = null

    setTimeout(() => {
      startWS()
    }, 5000) // Adjust the delay as necessary
  }
}

fetchData()

async function fetchData() {
  try {
    loading.set(true)
    error.set('')

    const url = `/lastblocks?n=${numberOfBlocks + 1}` // Get 1 more block than we need to calculate the delta time

    const res = await fetch(url)
    if (!res.ok) {
      throw new Error(`HTTP error! Status: ${res.status}`)
    }

    const b = await res.json()

    // Calculate delta time which is the time between blocks
    b.forEach((block, i) => {
      if (i === b.length - 1) {
        return
      }

      const prevBlock = b[i + 1]
      const prevBlockTime = new Date(prevBlock.timestamp)
      const blockTime = new Date(block.timestamp)
      const diff = blockTime - prevBlockTime
      block.deltaTime = getHumanReadableTime(diff)
    })

    // Calculate the age of the block
    b.forEach((block) => {
      const blockTime = new Date(block.timestamp)
      const now = new Date()
      const diff = now - blockTime
      block.age = getHumanReadableTime(diff)
    })

    blocks.set(b.slice(0, numberOfBlocks)) // Only show the last 10 blocks
  } catch (err) {
    error.set(err.message)
    console.error(err)
  } finally {
    loading.set(false)
  }
}



// Start by fetching initial data
fetchData();

// To access the unique data, you can convert the Set to an array:
blocks.set([...uniqueData])
