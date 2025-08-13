import { writable } from 'svelte/store'
import { getBaseUrl } from '../utils/apiUtils'

// API path prefix for regular API calls
const apiPathname = '/api/v1'

export const assetHTTPAddress = writable('', (set: any) => {
  if (!import.meta.env.SSR && window && window.location) {
    // Use the common utility function to get the base URL
    const baseUrl = getBaseUrl(apiPathname)
    set(baseUrl)

    // console.log('Using API URL:', baseUrl)
  }

  return set
})
