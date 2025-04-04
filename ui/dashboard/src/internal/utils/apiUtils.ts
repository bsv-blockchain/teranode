/**
 * Common utility functions for API URL handling
 */

/**
 * Gets the base URL for API calls with the specified path prefix
 * 
 * @param pathPrefix The API path prefix (e.g., '/api/v1', '/api/auth')
 * @returns The complete base URL for the specified API path
 */
export function getBaseUrl(pathPrefix: string = '/api/v1') {
  if (typeof window === 'undefined' || !window.location) {
    return '';
  }
  
  const url = new URL(window.location.href);
  
  // Development mode handling - force port 8090 for API calls
  if (url.port === '5173' || url.host.includes('localhost:517') || url.host.includes('localhost:417')) {
    return `http://${url.hostname}:8090${pathPrefix}`;
  }
  
  // Production handling
  if (url.port === '') {
    return `${url.protocol}//${url.hostname}${pathPrefix}`;
  } else {
    return `${url.protocol}//${url.hostname}:${url.port}${pathPrefix}`;
  }
}
