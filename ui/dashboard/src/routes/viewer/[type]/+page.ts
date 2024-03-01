import { error } from '@sveltejs/kit'
import type { PageLoad } from './$types'
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

/** @type {import('./$types').EntryGenerator} */
export const load: PageLoad = ({ params }) => {
  if (params.type && !DetailType[params.type]) {
    error(404, { message: 'Not found' })
  }
}

export const prerender = true
