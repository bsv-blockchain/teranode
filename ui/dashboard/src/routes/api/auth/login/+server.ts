import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { dev } from '$app/environment';

/**
 * POST handler for /api/auth/login
 * Authenticates the user with the provided credentials
 */
export const POST: RequestHandler = async ({ request, cookies, fetch }) => {
  try {
    // Parse the form data
    const formData = await request.formData();
    const username = formData.get('username')?.toString();
    const password = formData.get('password')?.toString();
    const csrfToken = formData.get('csrfToken')?.toString();
    
    // Validate CSRF token from the request body
    if (!csrfToken) {
      console.error('CSRF token validation failed - token missing');
      return json({ 
        success: false, 
        error: 'Invalid request' 
      }, { status: 403 });
    }
    
    // Validate required fields
    if (!username || !password) {
      return json({ 
        success: false, 
        error: 'Username and password are required' 
      }, { status: 400 });
    }
    
    // Forward the authentication request to the asset service
    // The asset service is responsible for validating the credentials
    const assetServiceUrl = dev 
      ? 'http://localhost:8090/api/auth/login' 
      : '/api/auth/login';
    
    const response = await fetch(assetServiceUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ username, password }),
    });
    
    // Get the response data
    const responseData = await response.json();
    
    if (!response.ok) {
      return json({ 
        success: false, 
        error: responseData.error || 'Authentication failed' 
      }, { status: response.status });
    }
    
    // If authentication was successful, set the session cookie
    if (responseData.success) {
      // Set the session cookie with secure attributes
      const sessionToken = responseData.token;
      if (sessionToken) {
        cookies.set('session', sessionToken, {
          path: '/',
          httpOnly: true,
          secure: !dev, // Only use secure in production
          sameSite: 'strict',
          maxAge: 60 * 60 * 24, // 24 hours
        });
      }
      
      // Return success response
      return json({ 
        success: true 
      });
    }
    
    // If we get here, something went wrong
    return json({ 
      success: false, 
      error: 'Authentication failed' 
    }, { status: 401 });
  } catch (error) {
    console.error('Login error:', error);
    return json({ 
      success: false, 
      error: 'An unexpected error occurred' 
    }, { status: 500 });
  }
};
