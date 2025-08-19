import { json } from '@sveltejs/kit'
import { dev } from '$app/environment'
import type { RequestHandler } from './$types'

/**
 * API endpoint to get the block locator from the blockchain service
 * This calls the asset server's API which in turn calls the blockchain gRPC service
 */
export const GET: RequestHandler = async ({ fetch }) => {
  try {
    // In development, proxy to the Asset Server running on localhost:8090
    // In production, use the relative path (same origin)
    const assetServerUrl = dev 
      ? 'http://localhost:8090/api/v1/block_locator'
      : '/api/v1/block_locator'
    
    const response = await fetch(assetServerUrl, {
      headers: {
        'Accept': 'application/json'
      }
    })
    
    if (!response.ok) {
      console.error(`Failed to get block locator: ${response.status} ${response.statusText}`)
      return json(
        { error: 'Failed to get block locator from blockchain service' },
        { status: response.status }
      )
    }
    
    const data = await response.json()
    
    // The response should contain the block locator array
    // Format: { block_locator: string[] } or just string[]
    if (Array.isArray(data)) {
      return json({ block_locator: data })
    } else if (data.block_locator && Array.isArray(data.block_locator)) {
      return json({ block_locator: data.block_locator })
    } else {
      // If the format is different, return what we got
      return json(data)
    }
    
  } catch (error) {
    console.error('Error getting block locator:', error)
    return json(
      { error: error instanceof Error ? error.message : 'Failed to get block locator' },
      { status: 500 }
    )
  }
}