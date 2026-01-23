import { browser } from '$app/environment'
import { goto } from '$app/navigation'
import { checkAuthentication } from '$internal/stores/authStore'

export const prerender = false

/** @type {import('./$types').PageLoad} */
export async function load() {
  if (browser) {
    const isAuthenticated = await checkAuthentication()
    if (!isAuthenticated) {
      goto('/login?redirect=/settings')
    }
  }

  return {}
}
