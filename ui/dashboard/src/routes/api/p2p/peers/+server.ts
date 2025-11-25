import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { dev } from '$app/environment'

/**
 * GET handler for /api/p2p/peers
 * Proxies requests to the Asset service's peers endpoint
 * (which in turn proxies to P2P's gRPC service)
 */
export const GET: RequestHandler = async ({ url }) => {
  try {
    let assetUrl: string

    if (dev) {
      // In development, Asset HTTP service runs on localhost:8090
      assetUrl = 'http://localhost:8090/api/v1/peers'
    } else {
      // In production, construct URL based on current request
      const protocol = url.protocol === 'https:' ? 'https:' : 'http:'
      const host = url.hostname
      const port = process.env.ASSET_HTTP_PORT || '8090'
      assetUrl = `${protocol}//${host}:${port}/api/v1/peers`
    }

    const response = await fetch(assetUrl)

    if (!response.ok) {
      throw new Error(`Asset service returned ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return json(data)
  } catch (error) {
    console.error('Peers proxy error:', error)
    return json(
      {
        error: 'Failed to fetch peer data',
        details: error instanceof Error ? error.message : 'Unknown error',
        // Return empty peers list on error
        peers: [],
        count: 0,
      },
      { status: 500 },
    )
  }
}
