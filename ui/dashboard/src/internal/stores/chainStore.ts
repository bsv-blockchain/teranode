import { writable, get } from 'svelte/store'
import type { Writable } from 'svelte/store'
import { assetHTTPAddress } from './nodeStore'

export const blocks: Writable<any[]> = writable([])

export async function loadLastBlocks() {
  const url = `${get(assetHTTPAddress)}/lastblocks?n=10&includeOrphans=true`

  try {
    const res = await fetch(url)
    if (!res.ok) {
      throw new Error(`HTTP error! Status: ${res.status}`)
    }

    const json = await res.json()

    // sort the blocks by height - THIS IS IMPORTANT
    const sorted = json.sort((a: any, b: any) => {
      if (a.height < b.height) return -1
      if (a.height > b.height) return 1
      return 0
    })

    blocks.set(sorted)
  } catch (err) {
    console.error(err)
  }
}

export async function onMessage(data: any) {
  if (data.type !== 'Block') return

  // Add the new block to the end of the list unless it already exists
  if (get(blocks).find((block) => block.hash === data.hash)) return

  let b = [...get(blocks)]

  // Now fetch the block header
  const url = get(assetHTTPAddress) + '/header/' + data.hash + '/json'

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
