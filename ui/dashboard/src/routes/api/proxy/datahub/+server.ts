import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'

export const POST: RequestHandler = async ({ request }) => {
  try {
    const { targetUrl } = await request.json()
    
    if (!targetUrl) {
      return json({ error: 'Target URL is required' }, { status: 400 })
    }
    
    // Make the request to the target URL
    const response = await fetch(targetUrl, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      },
    })
    
    // Get the response body as text first
    const responseText = await response.text()
    
    // If not OK, return the error with the same status
    if (!response.ok) {
      // Try to parse as JSON for better error messages
      try {
        const errorData = JSON.parse(responseText)
        return json(errorData, { status: response.status })
      } catch {
        // If not JSON, return as plain text error
        return json({ error: responseText }, { status: response.status })
      }
    }
    
    // Parse and return the successful response
    try {
      const data = JSON.parse(responseText)
      return json(data)
    } catch {
      // If response is not JSON, return as text
      return json({ data: responseText })
    }
  } catch (error) {
    console.error('Proxy error:', error)
    return json(
      { error: error instanceof Error ? error.message : 'Proxy request failed' },
      { status: 500 }
    )
  }
}