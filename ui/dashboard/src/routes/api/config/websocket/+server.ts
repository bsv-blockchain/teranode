import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { dev } from '$app/environment'

/**
 * GET handler for /api/config/websocket
 * Returns the WebSocket configuration for connecting to the Centrifuge service
 */
export const GET: RequestHandler = async ({ url }) => {
  try {
    let websocketUrl: string

    if (dev) {
      // In development, Centrifuge WebSocket runs on the Asset service port (8090)
      websocketUrl = 'ws://localhost:8090/connection/websocket'
    } else {
      // In production, construct WebSocket URL based on current request host
      // Use the same host as the current request but with the Asset service port
      const protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
      const host = url.hostname
      const port = process.env.ASSET_HTTP_PORT || '8090'
      websocketUrl = `${protocol}//${host}:${port}/connection/websocket`
    }

    return json({
      websocketUrl,
    })
  } catch (error) {
    console.error('WebSocket config error:', error)
    return json(
      {
        error: 'Failed to get WebSocket configuration',
      },
      { status: 500 },
    )
  }
}
