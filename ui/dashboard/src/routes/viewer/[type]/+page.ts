import { DetailType } from '$internal/utils/urls'

/** @type {import('./$types').EntryGenerator} */
export function entries() {
  return [
    { type: DetailType.block },
    { type: DetailType.subtree },
    { type: DetailType.tx },
    { type: DetailType.utxo },
  ]
}

export const prerender = true
