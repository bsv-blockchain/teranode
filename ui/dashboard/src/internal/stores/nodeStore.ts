import { writable } from 'svelte/store'

// TODO get this from the server / config
const apiPathname = '/api/v1'

export const assetHTTPAddress = writable('', (set: any) => {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    // let dev and preview mode connect to server
    if (url.host.includes('localhost:517') || url.host.includes('localhost:417')) {
      url.protocol = 'http:'
      url.host = 'localhost'
      url.port = '8090'
    }

    if (url.port === '') {
      set(`${url.protocol}//${url.hostname}${apiPathname}`)
    } else {
      set(`${url.protocol}//${url.hostname}:${url.port}${apiPathname}`)
    }
  }

  return set
})

