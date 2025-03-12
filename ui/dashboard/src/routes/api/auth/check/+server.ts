import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { dev } from '$app/environment';

/**
 * GET handler for /api/auth/check
 * Checks if the user is authenticated by verifying the session cookie
 */
export const GET: RequestHandler = async ({ cookies, fetch }) => {
  try {
    // Check if the session cookie exists
    const sessionToken = cookies.get('session');
    
    if (!sessionToken) {
      return json({ 
        authenticated: false 
      }, { status: 401 });
    }
    
    // Forward the authentication check to the asset service
    const assetServiceUrl = dev 
      ? 'http://localhost:8090/api/auth/check' 
      : '/api/auth/check';
    
    const response = await fetch(assetServiceUrl, {
      method: 'GET',
      credentials: 'include',
    });
    
    if (!response.ok) {
      // If the asset service says the session is invalid, clear the cookie
      cookies.delete('session', {
        path: '/',
        httpOnly: true,
        secure: !dev,
        sameSite: 'strict',
      });
      
      return json({ 
        authenticated: false 
      }, { status: 401 });
    }
    
    // If we get here, the user is authenticated
    return json({ 
      authenticated: true 
    });
  } catch (error) {
    console.error('Authentication check error:', error);
    return json({ 
      authenticated: false,
      error: 'An unexpected error occurred' 
    }, { status: 500 });
  }
};
