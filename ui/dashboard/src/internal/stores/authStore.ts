import { writable } from 'svelte/store'
import { getBaseUrl } from '../utils/apiUtils'

// Authentication store to manage login state
export const isAuthenticated = writable(false)
export const authError = writable('')

// Authentication API path prefix
const authPathname = '/api/auth'

// Check if the user is authenticated by making a request to the server
export async function checkAuthentication() {
  try {
    // Use the common utility function to get the base URL
    const apiUrl = getBaseUrl(authPathname) + '/check'

    const response = await fetch(apiUrl, {
      method: 'GET',
      credentials: 'include', // Important: include cookies in the request
      headers: {
        Accept: 'application/json',
      },
    })

    if (response.ok) {
      isAuthenticated.set(true)
      return true
    } else {
      isAuthenticated.set(false)
      return false
    }
  } catch (error) {
    console.error('Authentication check failed:', error)
    isAuthenticated.set(false)
    return false
  }
}

// Login function that uses the same authentication as the RPC server
export async function login(username: string, password: string, csrfToken: string) {
  try {
    // Create URLSearchParams directly instead of using FormData
    const params = new URLSearchParams()
    params.append('username', username)
    params.append('password', password)
    params.append('csrfToken', csrfToken) // Include CSRF token in the body

    // Use the common utility function to get the base URL
    const apiUrl = getBaseUrl(authPathname) + '/login'

    // Log the API URL in development mode to help with debugging
    if (import.meta.env.DEV) {
      console.log('Login API URL:', apiUrl)
    }

    const response = await fetch(apiUrl, {
      method: 'POST',
      credentials: 'include', // Important: include cookies in the request
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        // Removed X-CSRF-Token header to avoid CORS issues
      },
      body: params,
    })

    if (!response.ok) {
      // In development mode, provide more detailed error information
      if (import.meta.env.DEV) {
        console.error('Login failed:', response.status, response.statusText)
        try {
          const errorText = await response.text()
          console.error('Error response:', errorText)
        } catch (e) {
          console.error('Could not read error response')
        }
      }

      // Use generic error messages to avoid revealing implementation details
      if (response.status === 401) {
        authError.set('Invalid username or password')
      } else if (response.status === 429) {
        authError.set('Too many login attempts. Please try again later.')
      } else {
        authError.set('Login failed. Please try again.')
      }
      isAuthenticated.set(false)
      return false
    }

    try {
      const data = await response.json()

      if (data.success) {
        isAuthenticated.set(true)
        authError.set('')

        // After successful login, check authentication to verify cookie is working
        setTimeout(() => {
          checkAuthentication().then(() => {
            console.log('Authentication verification completed')
          })
        }, 500)

        return true
      } else {
        // Use a generic error message
        authError.set('Authentication failed. Please check your credentials.')
        isAuthenticated.set(false)
        return false
      }
    } catch (jsonError) {
      authError.set('Login failed. Please try again.')
      isAuthenticated.set(false)
      return false
    }
  } catch (error) {
    authError.set('Login failed. Please try again.')
    isAuthenticated.set(false)
    return false
  }
}

// Logout function - just handle the API call and state update
// Let the calling component handle any navigation
export function logout() {
  // Use the common utility function to get the base URL
  const apiUrl = getBaseUrl(authPathname) + '/logout'

  return fetch(apiUrl, {
    method: 'POST',
    credentials: 'include', // Important: include cookies in the request
  }).finally(() => {
    isAuthenticated.set(false)
  })
}
