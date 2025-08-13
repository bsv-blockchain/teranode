import { json } from '@sveltejs/kit'
import type { RequestHandler } from './$types'
import { dev } from '$app/environment'

/**
 * POST handler for /api/auth/logout
 * Logs out the user by clearing the session cookie
 */
export const POST: RequestHandler = async ({ cookies, fetch }) => {
  try {
    // Forward the logout request to the asset service
    const assetServiceUrl = dev ? 'http://localhost:8090/api/auth/logout' : '/api/auth/logout'

    // Attempt to notify the asset service about the logout
    try {
      await fetch(assetServiceUrl, {
        method: 'POST',
        credentials: 'include',
      })
    } catch (error) {
      console.warn('Failed to notify asset service about logout:', error)
      // Continue with logout even if the asset service request fails
    }

    // Clear the session cookie
    cookies.delete('session', {
      path: '/',
      httpOnly: true,
      secure: !dev,
      sameSite: 'strict',
    })

    // Return success response
    return json({ success: true })
  } catch (error) {
    console.error('Logout error:', error)
    return json(
      {
        success: false,
        error: 'An unexpected error occurred',
      },
      { status: 500 },
    )
  }
}
