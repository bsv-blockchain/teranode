import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { dev } from '$app/environment'

/**
 * GET handler for /api/config/node
 * Returns the node configuration including the base URL
 */
export const GET: RequestHandler = async ({ url }) => {
  try {
    let nodeBaseUrl: string

    if (dev) {
      // In development, use localhost with the asset service port
      nodeBaseUrl = 'http://localhost:8090'
    } else {
      // In production, construct node URL based on current request
      const protocol = url.protocol === 'https:' ? 'https:' : 'http:'
      const host = url.hostname
      const port = process.env.ASSET_HTTP_PORT || '8090'
      nodeBaseUrl = `${protocol}//${host}:${port}`
    }

    return json({
      nodeBaseUrl,
    })
  } catch (error) {
    console.error('Node config error:', error)
    return json(
      {
        error: 'Failed to get node configuration',
      },
      { status: 500 },
    )
  }
}
