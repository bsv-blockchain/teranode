import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { dev } from '$app/environment'

/**
 * GET handler for /api/p2p/peers
 * Proxies requests to the P2P service's peers endpoint
 */
export const GET: RequestHandler = async () => {
  try {
    let p2pUrl: string

    if (dev) {
      // In development, P2P HTTP service runs on localhost:9906
      p2pUrl = 'http://localhost:9906/peers'
    } else {
      // In production, construct URL based on configuration
      const port = process.env.P2P_HTTP_PORT || '9906'
      p2pUrl = `http://localhost:${port}/peers`
    }

    const response = await fetch(p2pUrl)

    if (!response.ok) {
      throw new Error(`P2P service returned ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return json(data)
  } catch (error) {
    console.error('P2P peers proxy error:', error)
    return json(
      {
        error: 'Failed to fetch peer data',
        details: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 },
    )
  }
}
