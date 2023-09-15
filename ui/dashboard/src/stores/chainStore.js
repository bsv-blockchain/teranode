import { writable, get } from 'svelte/store'
import { selectedNode } from '@stores/bootstrapStore.js'

export const blocks = writable([])

export async function onMessage(data) {
  if (data.type !== 'Block') return

  // Add the new block to the end of the list unless it already exists
  if (get(blocks).find((block) => block.hash === data.hash)) return

  let b = [...get(blocks)]

  // Now fetch the block header
  const url = get(selectedNode) + '/header/' + data.hash + '/json'

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
