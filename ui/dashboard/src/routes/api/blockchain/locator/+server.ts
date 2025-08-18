import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'

/**
 * API endpoint to get the block locator from the blockchain service
 * This calls the asset server's API which in turn calls the blockchain gRPC service
 */
export const GET: RequestHandler = async () => {
  try {
    // Get the node base URL from environment or config
    const nodeBaseUrl = process.env.NODE_BASE_URL || 'http://localhost:8090'
    
    // Call the asset server to get the block locator
    // The asset server will call the blockchain gRPC service's GetBlockLocator method
    const response = await fetch(`${nodeBaseUrl}/api/v1/block_locator`, {
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