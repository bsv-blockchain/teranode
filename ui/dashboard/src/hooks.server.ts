import type { Handle } from '@sveltejs/kit'

/**
 * The Content-Security-Policy served with the dashboard.
 *
 * This hook does NOT run in production: the dashboard is built with adapter-static and served by
 * the Go asset service, so the authoritative copy is the constant in
 * services/asset/httpimpl/http.go. Fix that one; this copy is kept byte-identical to it so
 * development and production cannot drift apart, and it is exported so the browser test asserts the
 * same string rather than a fourth transcription of it.
 *
 * It is defence in depth, not a strict policy: 'unsafe-inline' is required by the three inline
 * scripts the built dashboard carries, so inline event handlers still fire, and connect-src stays
 * wide because the dashboard drives remote teranode instances.
 *
 * ws: is listed explicitly rather than left to 'self'. The dashboard does open its live feed over
 * ws:// when it is served over plain http - routes/api/config/websocket/+server.ts picks the scheme
 * from the page's own protocol - and whether 'self' covers a same-origin ws:// URL is a CSP3
 * refinement rather than something the directive plainly says. Naming the scheme costs nothing here
 * and removes the dependence on that refinement. The request host is deliberately not interpolated.
 */
export const CONTENT_SECURITY_POLICY =
  "default-src 'self'; " +
  "script-src 'self' 'unsafe-inline'; " +
  "style-src 'self' 'unsafe-inline'; " +
  "img-src 'self' data:; " +
  "font-src 'self' data:; " +
  "object-src 'none'; " +
  "base-uri 'self'; " +
  "form-action 'self'; " +
  "frame-ancestors 'none'; " +
  "connect-src 'self' https: wss: ws:"

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

    // Content Security Policy. See CONTENT_SECURITY_POLICY above for why this copy exists and
    // what the policy does and does not buy.
    response.headers.set('Content-Security-Policy', CONTENT_SECURITY_POLICY)

    // Referrer policy
    response.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin')
  }

  return response
}
