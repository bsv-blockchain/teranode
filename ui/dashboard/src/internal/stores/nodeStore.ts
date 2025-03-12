import { writable } from 'svelte/store'

// Get this from the server configuration
const apiPathname = '/api/v1'

export const assetHTTPAddress = writable('', (set: any) => {
  if (!import.meta.env.SSR && window && window.location) {
    const url = new URL(window.location.href)
    
    // Special handling for development mode
    if (url.host.includes('localhost:517') || url.host.includes('localhost:417')) {
      // For development, use port 8090 which is where the asset service runs
      set(`http://localhost:8090${apiPathname}`)
      console.log('Using development API URL:', `http://localhost:8090${apiPathname}`)
      return set
    }

    // For production or other environments
    // Always use the current page's origin (including port) to avoid CORS issues
    // This ensures we use the correct port (18090, 28090, 38090, etc.) when deployed in Docker
    const currentPort = url.port || '80';
    set(`${url.protocol}//${url.hostname}:${currentPort}${apiPathname}`)
    console.log('Using API URL:', `${url.protocol}//${url.hostname}:${currentPort}${apiPathname}`)
  }

  return set
})
