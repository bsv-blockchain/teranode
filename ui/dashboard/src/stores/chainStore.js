import { writable, get } from 'svelte/store'
import { blobServerHTTPAddress } from '@stores/nodeStore.js'

export const blocks = writable([])

export async function loadLastBlocks() {
  const url = `${get(blobServerHTTPAddress)}/lastblocks?n=10&includeOrphans=true`

  try {
    const res = await fetch(url)
    if (!res.ok) {
      throw new Error(`HTTP error! Status: ${res.status}`)
    }

    const json = await res.json()

    // sort the blocks by height
    const sorted = json.sort((a, b) => {
      if (a.height < b.height) return -1
      if (a.height > b.height) return 1
      return 0
    })

    console.log("sorted:", sorted)
    blocks.set(sorted)
  } catch (err) {
    console.error(err)
  }
}


export async function onMessage(data) {
  if (data.type !== 'Block') return

  // Add the new block to the end of the list unless it already exists
  if (get(blocks).find((block) => block.hash === data.hash)) return

  let b = [...get(blocks)]

  // Now fetch the block header
  const url = get(blobServerHTTPAddress) + '/header/' + data.hash + '/json'

  try {
    const res = await fetch(url)
    if (!res.ok) {
      throw new Error(`HTTP error! Status: ${res.status}`)
    }

    const json = await res.json()

    // Slice off all but the last 30 blocks
    if (b.length > 20) {
      b = b.slice(-20)
    }

    blocks.set([...b, json])
  } catch (err) {
    console.error(err)
  }
}
