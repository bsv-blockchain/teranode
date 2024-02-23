// import { error } from '@sveltejs/kit'
import { goto } from '$app/navigation'
import type { PageLoad } from './$types'
import posts from '$internal/assets/blog/index.json'

/** @type {import('./$types').EntryGenerator} */
export function entries() {
  return posts.posts.map((item) => ({ slug: item.slug }))
}

/** @type {import('./$types').EntryGenerator} */
export const load: PageLoad = async ({ params }) => {
  if (posts.posts.filter((item) => item.slug === params.slug).length === 0) {
    // error(404, { message: 'Not found' })
    goto('/notifications')
  }
}

export const prerender = true
