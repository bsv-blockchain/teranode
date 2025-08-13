/**
 * URL validation utilities to prevent open redirect vulnerabilities
 */

/**
 * Validates a URL to prevent open redirect vulnerabilities
 * Only allows relative URLs or absolute URLs to the same origin
 *
 * @param url The URL to validate
 * @returns The validated URL if valid, or null if invalid
 */
export function validateUrl(url: string): string | null {
  if (!url) {
    return null
  }

  // Allow relative URLs that start with a slash and don't contain protocol separators
  if (url.startsWith('/') && !url.includes('://')) {
    // Prevent path traversal attacks
    const normalizedUrl = url.replace(/\/\.\.\//g, '/')
    if (normalizedUrl !== url) {
      console.warn('Potential path traversal attempt detected in URL:', url)
      return null
    }
    return url
  }

  // For absolute URLs, verify they point to the same origin
  try {
    // If we're in a browser environment
    if (typeof window !== 'undefined') {
      const currentOrigin = window.location.origin
      const urlObj = new URL(url, currentOrigin)

      // Check if the URL has the same origin as the current page
      if (urlObj.origin === currentOrigin) {
        return url
      }
    }
  } catch (error) {
    console.error('Error validating URL:', error)
  }

  // If we get here, the URL is invalid or points to a different origin
  console.warn('Invalid redirect URL detected:', url)
  return null
}

/**
 * Sanitizes a URL by removing potentially dangerous characters
 *
 * @param url The URL to sanitize
 * @returns The sanitized URL
 */
export function sanitizeUrl(url: string): string {
  if (!url) {
    return ''
  }

  // Remove any script tags, HTML entities, or other potentially dangerous content
  return url.replace(/<script.*?>.*?<\/script>/gi, '').replace(/[<>"']/g, '')
}
