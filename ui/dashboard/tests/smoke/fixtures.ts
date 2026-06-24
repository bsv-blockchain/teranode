import { test as base, expect, type Page, type ConsoleMessage } from '@playwright/test'
import { installApiMocks } from './mocks/api'
import { installWsMocks } from './mocks/ws'

// Allowlist of console messages we do not consider failures.
// Patterns are matched as substrings against the message text.
// Keep narrow: a bare substring like "echarts" would swallow genuine
// ECharts errors. Each entry must anchor on a log marker we know is benign.
const CONSOLE_ALLOWLIST: string[] = ['[vite] connected', '[vite] hot updated']

// Browser-emitted network log when /api/auth/check returns 401. Only benign
// on unauthenticated routes (e.g. /login) where 401 is the expected response;
// on authenticated routes a 401 is a real bug, so this is NOT globally
// allowlisted — only added for tests that opt into `authenticated: false`.
const UNAUTH_401 = 'Failed to load resource: the server responded with a status of 401 (Unauthorized)'

type SmokeFixtures = {
  authenticated: boolean
  smokePage: Page
  consoleErrors: string[]
}

export const test = base.extend<SmokeFixtures>({
  authenticated: [true, { option: true }],

  smokePage: async ({ page, context, authenticated }, use) => {
    if (authenticated) {
      await context.addCookies([
        {
          name: 'auth',
          value: 'smoke-test-token',
          domain: 'localhost',
          path: '/',
          httpOnly: false,
          secure: false,
          sameSite: 'Lax',
        },
      ])
    }

    await installApiMocks(page, { authenticated })
    await installWsMocks(page)

    await use(page)
  },

  consoleErrors: async ({ smokePage, authenticated }, use) => {
    const errors: string[] = []

    const allowlist = authenticated ? CONSOLE_ALLOWLIST : [...CONSOLE_ALLOWLIST, UNAUTH_401]
    const isAllowed = (text: string) => allowlist.some((needle) => text.includes(needle))

    const onConsole = (msg: ConsoleMessage) => {
      if (msg.type() !== 'error' && msg.type() !== 'warning') return
      const text = msg.text()
      if (isAllowed(text)) return
      // Treat Svelte deprecation warnings as failures.
      if (text.includes('[svelte]') || text.toLowerCase().includes('deprecat')) {
        errors.push(`[${msg.type()}] ${text}`)
        return
      }
      if (msg.type() === 'error') errors.push(`[error] ${text}`)
    }

    const onPageError = (err: Error) => {
      errors.push(`[pageerror] ${err.message}`)
    }

    smokePage.on('console', onConsole)
    smokePage.on('pageerror', onPageError)

    await use(errors)

    smokePage.off('console', onConsole)
    smokePage.off('pageerror', onPageError)
  },
})

export { expect }
