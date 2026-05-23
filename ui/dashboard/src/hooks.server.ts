import type { Handle } from '@sveltejs/kit'

/**
 * Server-side middleware to protect routes that require authentication
 */
export const handle: Handle = async ({ event, resolve }) => {
  // List of protected routes that require authentication
  const protectedRoutes = ['/admin', '/settings', '/profile']

  // Check if the current path is a protected route
  const isProtectedRoute = protectedRoutes.some((route) => event.url.pathname.startsWith(route))

  if (isProtectedRoute) {
    // Check if the user is authenticated
    const sessionCookie = event.cookies.get('session')

    if (!sessionCookie) {
      // Redirect to login page with the original URL as the redirect parameter
      return new Response(null, {
        status: 302,
        headers: {
          Location: `/login?redirect=${encodeURIComponent(event.url.pathname)}`,
        },
      })
    }

    // If there's a session cookie, we'll assume the user is authenticated
    // The actual verification will happen in the API call
  }

  // Add security headers to all responses
  const response = await resolve(event)

  // Add security headers
  if (response.headers) {
    // Prevent clickjacking
    response.headers.set('X-Frame-Options', 'DENY')

    // Enable XSS protection
    response.headers.set('X-XSS-Protection', '1; mode=block')

    // Prevent MIME type sniffing
    response.headers.set('X-Content-Type-Options', 'nosniff')

    // Build dynamic connect-src based on the request host so that the CSP works
    // regardless of whether the dashboard is accessed via localhost or a remote hostname.
    const host = event.request.headers.get('host') ?? 'localhost:8090'
    const httpOrigin = `http://${host}`
    const wsOrigin = `ws://${host}`
    const wssOrigin = `wss://${host}`

    // Content Security Policy with allowances for the asset service, centrifuge websocket, and external teranode instances
    // Allow connections to any https:// URL for teranode instances, but restrict other resource types
    response.headers.set(
      'Content-Security-Policy',
      "default-src 'self'; " +
        "script-src 'self' 'unsafe-inline'; " +
        "style-src 'self' 'unsafe-inline'; " +
        "img-src 'self' data:; " +
        `connect-src 'self' ${httpOrigin} ${wsOrigin} ${wssOrigin} https: wss:;`,
    )

    // Referrer policy
    response.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin')
  }

  return response
}
