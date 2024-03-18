import type { LayoutLoad } from './$types'
import posts from '$internal/assets/blog/index.json'

export const load: LayoutLoad = async () => {
  return posts
}
