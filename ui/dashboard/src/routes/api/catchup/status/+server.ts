import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { dev } from '$app/environment'

/**
 * GET handler for /api/catchup/status
 * Proxies requests to the Asset service's catchup status HTTP endpoint
 * (which in turn proxies to BlockValidation's gRPC service)
 */
export const GET: RequestHandler = async ({ url }) => {
  try {
    let assetUrl: string

    if (dev) {
      // In development, Asset HTTP service runs on localhost:8090
      assetUrl = 'http://localhost:8090/api/v1/catchup/status'
    } else {
      // In production, construct URL based on current request
      const protocol = url.protocol === 'https:' ? 'https:' : 'http:'
      const host = url.hostname
      const port = process.env.ASSET_HTTP_PORT || '8090'
      assetUrl = `${protocol}//${host}:${port}/api/v1/catchup/status`
    }

    const response = await fetch(assetUrl)

    if (!response.ok) {
      throw new Error(`Asset service returned ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return json(data)
  } catch (error) {
    console.error('Catchup status proxy error:', error)
    return json(
      {
        error: 'Failed to fetch catchup status',
        details: error instanceof Error ? error.message : 'Unknown error',
        // Return a default "not catching up" status on error
        status: {
          is_catching_up: false,
        },
      },
      { status: 500 },
    )
  }
}
