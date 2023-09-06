import { writable, get } from 'svelte/store'
import { nodes } from '@stores/nodeStore.js'

export const blocks = writable([])
export const error = writable('')
export const loading = writable(false)

// Promise to resolve after a certain time for timeout handling
function timeout(ms) {
  return new Promise((resolve, reject) =>
    setTimeout(() => reject(new Error('Promise timed out')), ms)
  )
}

// Retry fetchData after a delay if already loading
async function retryFetchData() {
  if (!get(loading)) {
    setTimeout(fetchData, 10000) // Try again in 10s
  }
}

export async function fetchData(force = false) {
  if (!force && get(loading)) {
    retryFetchData()
    return
  }

  try {
    loading.set(true)

    const bestBlocks = await getBestBlocks(nodes)

    // Update stores
    blocks.set(bestBlocks)
    error.set('')

    // Schedule next automatic fetchData call
    setTimeout(fetchData, 10000)
  } catch (err) {
    console.error(err)
    error.set(err.message)
  } finally {
    loading.set(false)
  }
}

async function getBestBlocks(nodesData) {
  const hashesToAddresses = {}

  nodesData.forEach((node) => {
    if (node.header?.hash && node.blobServerHTTPAddress) {
      hashesToAddresses[node.header.hash] = node.blobServerHTTPAddress
    }
  })

  const blocksData = await Promise.all(
    Object.entries(hashesToAddresses).map(async ([hash, addr]) => {
      try {
        const blocks = await Promise.race([
          getLast10Blocks(hash, addr),
          timeout(1000),
        ])

        return {
          hash,
          blocks,
        }
      } catch (error) {
        console.error(`Error fetching blocks for hash ${hash}:`, error.message)
        return {
          hash,
          blocks: { error: 'timeout' },
        }
      }
    })
  )

  const blockObject = blocksData.reduce((acc, { hash, blocks }) => {
    acc[hash] = blocks
    return acc
  }, {})

  return blockObject
}

async function getLast10Blocks(hash, address) {
  const url = `${address}/headers/${hash}/json?n=10`
  const response = await fetch(url)

  if (!response.ok) {
    throw new Error(`HTTP error! Status: ${response.status}`)
  }

  return await response.json()
}
