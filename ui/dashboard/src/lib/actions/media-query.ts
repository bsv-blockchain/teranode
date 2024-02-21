import { readable } from 'svelte/store'
import { browser } from '$app/environment'

export function query(query) {
  if (!browser) {
    return null
  }

  // eslint-ignore-next-line
  const mediaQueryList = window.matchMedia(query)

  const mediaStore = readable(mediaQueryList.matches, (set) => {
    const handleChange = () => set(mediaQueryList.matches)

    mediaQueryList.addEventListener('change', handleChange)

    return () => mediaQueryList.removeEventListener('change', handleChange)
  })

  return mediaStore
}
