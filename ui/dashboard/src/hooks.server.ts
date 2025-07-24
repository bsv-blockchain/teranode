import type { Handle } from '@sveltejs/kit';

/**
 * Server-side middleware to protect routes that require authentication
 */
export const handle: Handle = async ({ event, resolve }) => {
  // List of protected routes that require authentication
  const protectedRoutes = [
    '/admin',
    '/settings',
    '/profile',
  ];

  // Check if the current path is a protected route
  const isProtectedRoute = protectedRoutes.some(route =>
    event.url.pathname.startsWith(route)
  );

  if (isProtectedRoute) {
    // Check if the user is authenticated
    const sessionCookie = event.cookies.get('session');

    if (!sessionCookie) {
      // Redirect to login page with the original URL as the redirect parameter
      return new Response(null, {
        status: 302,
        headers: {
          Location: `/login?redirect=${encodeURIComponent(event.url.pathname)}`
        }
      });
    }

    // If there's a session cookie, we'll assume the user is authenticated
    // The actual verification will happen in the API call
  }

  // Add security headers to all responses
  const response = await resolve(event);

  // Add security headers
  if (response.headers) {
    // Prevent clickjacking
    response.headers.set('X-Frame-Options', 'DENY');

    // Enable XSS protection
    response.headers.set('X-XSS-Protection', '1; mode=block');

    // Prevent MIME type sniffing
    response.headers.set('X-Content-Type-Options', 'nosniff');

    // Strict Content Security Policy with allowances for the asset service and centrifuge websocket
    response.headers.set(
      'Content-Security-Policy',
      "default-src 'self'; " +
      "script-src 'self' 'unsafe-inline'; " +
      "style-src 'self' 'unsafe-inline'; " +
      "img-src 'self' data:; " +
      "connect-src 'self' http://localhost:8090 ws://localhost:8090 wss://localhost:8090;"
    );

    // Referrer policy
    response.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');
  }

  return response;
};
