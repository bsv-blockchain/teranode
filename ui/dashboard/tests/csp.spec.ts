/**
 * The production Content-Security-Policy, asserted by a browser that is actually enforcing it.
 *
 * The Go unit test compares the served header against the package constant; that proves the string
 * is emitted, not that a browser does what the string is meant to make it do. These tests fulfil a
 * real document with the real header from the preview origin, then exercise the two behaviours the
 * policy is supposed to have (bitcoin-sv/teranode#4844):
 *
 *  - a ws:// websocket to ANOTHER host is NOT blocked. The dashboard drives remote teranode
 *    instances, and a cross-origin ws:// URL is covered only by the listed ws: scheme, never by
 *    'self' or https:. So this assertion fails if ws: is dropped from connect-src. (A same-origin
 *    target would not: Chromium treats 'self' as covering a same-origin ws:// URL.) wss: is listed
 *    only as belt and braces over https:, which CSP3 scheme matching lets cover wss:// as well, so
 *    no browser check could fail when it is dropped; that token is guarded as a string instead.
 *  - a remote module import IS blocked, BY THE POLICY. The module is served from the test itself,
 *    with the CORS header a cross-origin module import requires, and a separate positive control
 *    shows it genuinely loading when no policy is served — so "blocked" cannot be satisfied by a
 *    network or CORS failure. The refusal is further confirmed by a securitypolicyviolation event
 *    naming script-src. That import is the amplification step a coinbase-sized payload needs, and
 *    blocking it is the main thing this policy buys against the reported attack.
 *
 * Runs under `npm run test:integration`, NOT `npm run test:unit`. CI must run it.
 */
import { test, expect, type Page } from '@playwright/test'
import { CONTENT_SECURITY_POLICY } from '../src/hooks.server'

// The policy asserted here is the dashboard's copy. A Go test
// (services/asset/httpimpl/security_headers_test.go, TestContentSecurityPolicy_MatchesDashboardCopy)
// enforces that it is byte-identical to the one the asset service serves in production, so a drift
// between the two fails the build rather than quietly making this file assert the wrong string.
const REMOTE_MODULE_URL = 'https://audit.invalid/payload.js'

/** Serves a page from the real origin carrying the real policy. */
async function openWithProductionCSP(page: Page) {
  await page.route('**/csp-fixture', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'text/html',
      headers: { 'Content-Security-Policy': CONTENT_SECURITY_POLICY },
      body: '<!doctype html><html><body><div id="host"></div></body></html>',
    })
  })

  await page.goto('/csp-fixture')
}

test('the policy does not block a websocket to another host', async ({ page }) => {
  await openWithProductionCSP(page)

  // A connect-src violation makes the WebSocket constructor throw SecurityError synchronously. A
  // policy-permitted socket that simply cannot connect fails later, asynchronously, so a throw here
  // means the POLICY refused it and nothing else. The target host differs from the page's, so
  // 'self' cannot be what permits it, and ws:// is not a secure scheme, so https: cannot either.
  const outcome = await page.evaluate(() => {
    const otherHost = location.hostname === '127.0.0.1' ? 'localhost:1' : '127.0.0.1:1'

    let blocked = false
    try {
      const ws = new WebSocket(`ws://${otherHost}/connection/websocket`)
      ws.close()
    } catch (e) {
      blocked = (e as Error).name === 'SecurityError'
    }

    return { otherHost, pageHost: location.host, blocked }
  })

  expect(outcome.otherHost, 'fixture precondition: the target is another host').not.toBe(
    outcome.pageHost,
  )
  expect(outcome.blocked, 'ws:// to another host must not be blocked by the policy').toBe(false)
})

test('the policy still names wss: in connect-src', () => {
  // A string check, not a browser one: https: already lets CSP3 scheme matching cover wss://, so
  // dropping wss: would not change what a browser allows. The token is kept as belt and braces.
  const connectSrc = CONTENT_SECURITY_POLICY.split(';')
    .map((directive) => directive.trim())
    .find((directive) => directive.startsWith('connect-src '))

  expect(connectSrc?.split(' ')).toContain('wss:')
})

/**
 * Serves the cross-origin module the way a real attacker-controlled host would have to: a 200 with a
 * JavaScript content type AND the CORS header a cross-origin module import requires. Without that
 * header the import fails on CORS whatever the policy says, so the counterfactual would be
 * unfalsifiable — "blocked" would prove nothing about CSP. The positive control below runs the same
 * route with no policy and requires it to LOAD, which is what makes this route trustworthy.
 */
async function serveRemoteModule(page: Page, onFetch?: () => void) {
  await page.route(REMOTE_MODULE_URL, async (route) => {
    onFetch?.()

    await route.fulfill({
      status: 200,
      contentType: 'text/javascript',
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: 'export const payload = 1',
    })
  })
}

test('the remote module loads when no policy is served (positive control)', async ({ page }) => {
  // Establishes the counterfactual the next test depends on. If this ever fails, the "CSP blocked
  // it" assertion below is meaningless and must not be believed.
  await serveRemoteModule(page)

  await page.route('**/no-csp-fixture', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'text/html',
      body: '<!doctype html><html><body></body></html>',
    })
  })

  await page.goto('/no-csp-fixture')

  const loaded = await page.evaluate(async (remote) => {
    try {
      await import(/* @vite-ignore */ remote)
      return true
    } catch {
      return false
    }
  }, REMOTE_MODULE_URL)

  expect(loaded, 'without a policy the remote module must genuinely load').toBe(true)
})

test('the policy blocks a remote module import, and CSP is what blocked it', async ({ page }) => {
  // The same module, served the same way, now behind the production policy.
  let remoteWasFetched = false

  await serveRemoteModule(page, () => {
    remoteWasFetched = true
  })

  await openWithProductionCSP(page)

  const outcome = await page.evaluate(async (remote) => {
    // Record the policy's own report of the refusal. This is what proves CSP caused it rather than
    // the network: the event only fires when a directive actually blocks something.
    const violations: string[] = []
    document.addEventListener('securitypolicyviolation', (e) => {
      violations.push((e as SecurityPolicyViolationEvent).violatedDirective)
    })

    let loaded = false
    try {
      // The specifier is held in a variable so TypeScript does not try to resolve the remote module
      // at check time; the browser resolves it at run time, which is the whole point.
      await import(/* @vite-ignore */ remote)
      loaded = true
    } catch {
      loaded = false
    }

    // The violation event is dispatched asynchronously relative to the import rejection.
    await new Promise((resolve) => setTimeout(resolve, 100))

    return { loaded, violations }
  }, REMOTE_MODULE_URL)

  expect(outcome.loaded).toBe(false)
  expect(outcome.violations.join(',')).toContain('script-src')

  // Blocked before the request left the page, so the module this test was ready to serve was never
  // even asked for.
  expect(remoteWasFetched).toBe(false)
})

test('the policy keeps script-src free of remote origins', async ({ page }) => {
  await openWithProductionCSP(page)

  const served = await page.evaluate(
    () =>
      document
        .querySelector('meta[http-equiv="Content-Security-Policy"]')
        ?.getAttribute('content') ?? null,
  )

  // The policy is served as a header, not a meta tag, so nothing should be shadowing it in-document.
  expect(served).toBeNull()

  const scriptSrc = CONTENT_SECURITY_POLICY.split(';')
    .map((directive) => directive.trim())
    .find((directive) => directive.startsWith('script-src '))

  expect(scriptSrc).toBeDefined()
  expect(scriptSrc).not.toContain('http')
  expect(scriptSrc).not.toContain('*')
})
