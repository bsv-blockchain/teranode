import { browser } from '$app/environment'
import { redirect } from '@sveltejs/kit'
import { checkAuthentication } from '$internal/stores/authStore'

export const prerender = false
export const ssr = false

/** @type {import('./$types').PageLoad} */
export async function load() {
  if (browser) {
    const isAuthenticated = await checkAuthentication()
    if (!isAuthenticated) {
      throw redirect(302, '/login?redirect=/settings')
    }
  }

  return {}
}
