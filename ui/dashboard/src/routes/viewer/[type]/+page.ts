/** @type {import('./$types').EntryGenerator} */
export function entries() {
  return [{ type: 'block' }, { type: 'subtree' }, { type: 'tx' }]
}

export const prerender = true
