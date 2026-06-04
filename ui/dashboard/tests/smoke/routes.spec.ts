import { test, expect } from './fixtures'

type Route = {
  path: string
  selector: string // visible after hydration
  authenticated?: boolean // default true
}

const PAGE_ROOT = '[data-test-id="page-root"]'

// `/` re-exports `/home/+page.svelte`, so both paths exercise the same
// component — covering both proves the alias route too.
const ROUTES: Route[] = [
  { path: '/', selector: PAGE_ROOT },
  { path: '/home', selector: PAGE_ROOT },
  { path: '/network', selector: PAGE_ROOT },
  { path: '/p2p', selector: PAGE_ROOT },
  { path: '/peers', selector: PAGE_ROOT },
  { path: '/forks', selector: PAGE_ROOT },
  { path: '/ancestors', selector: PAGE_ROOT },
  { path: '/viewer', selector: PAGE_ROOT },
  // /api is a server-only route group (only +server.ts handlers, no
  // +page.svelte), not a navigable page. Smoke does not exercise it.
  { path: '/settings', selector: PAGE_ROOT },
  { path: '/admin', selector: PAGE_ROOT },
  { path: '/wstest', selector: PAGE_ROOT },
  { path: '/login', selector: PAGE_ROOT, authenticated: false },
]

for (const route of ROUTES) {
  test.describe(`smoke: ${route.path}`, () => {
    test.use({ authenticated: route.authenticated ?? true })

    test('renders with no console errors', async ({ smokePage, consoleErrors }) => {
      const response = await smokePage.goto(route.path)
      expect(response?.ok(), `${route.path} should respond 2xx`).toBe(true)
      await expect(smokePage.locator(route.selector)).toBeVisible()
      await smokePage.waitForTimeout(1000)
      expect(consoleErrors, `console errors on ${route.path}:\n${consoleErrors.join('\n')}`).toHaveLength(0)
    })
  })
}

// Click-through smoke: /viewer renders a block list, clicking a hash navigates
// to /viewer/block/?hash=<hash> and the detail page renders without errors.
// Catches regressions in:
//   - blocks-list fetch + table population
//   - block-detail page mount + 3-endpoint compose (block + header + lastblocks)
//   - URL/router behaviour for the /viewer/[type] dynamic route
const FIRST_BLOCK_HASH = '0000000000000000000004aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'

test.describe('smoke: /viewer click-through to block detail', () => {
  test('opens block detail when first block hash is clicked', async ({ smokePage, consoleErrors }) => {
    await smokePage.goto('/viewer')
    await expect(smokePage.locator(PAGE_ROOT)).toBeVisible()

    // First block hash link in the blocks table.
    const firstBlockLink = smokePage.locator('a[href^="/viewer/block/?hash="]').first()
    await expect(firstBlockLink).toBeVisible()
    await firstBlockLink.click()

    await smokePage.waitForURL(/\/viewer\/block\/\?hash=[a-fA-F0-9]+/)

    // Detail-page-unique assertion: the BlockDetailsCard renders the full
    // 64-char hash in its subtitle. Asserting the hash text confirms the
    // detail card mounted with the right data, not just that any page-root
    // is visible (the list page also has page-root and could briefly be
    // mounted during transition).
    await expect(smokePage.getByText(FIRST_BLOCK_HASH, { exact: false })).toBeVisible()

    await smokePage.waitForTimeout(1000)
    expect(consoleErrors, `console errors on /viewer click-through:\n${consoleErrors.join('\n')}`).toHaveLength(0)
  })
})

// The /viewer/[type] dynamic route renders four detail components, all using
// the same Svelte 4 patterns the runes migration (#977) targets. The
// click-through above only exercises `block`. These cover the other three by
// direct navigation with a fixed hash, so the migration net guards all four.
const DETAIL_HASH = '0000000000000000000000000000000000000000000000000000000000000aaa'

const DETAIL_TYPES = ['subtree', 'tx', 'utxo'] as const

for (const type of DETAIL_TYPES) {
  test.describe(`smoke: /viewer/${type} detail`, () => {
    test('renders with no console errors', async ({ smokePage, consoleErrors }) => {
      const response = await smokePage.goto(`/viewer/${type}/?hash=${DETAIL_HASH}`)
      expect(response?.ok(), `/viewer/${type} should respond 2xx`).toBe(true)
      await expect(smokePage.locator(PAGE_ROOT)).toBeVisible()
      await smokePage.waitForTimeout(1000)
      expect(
        consoleErrors,
        `console errors on /viewer/${type}:\n${consoleErrors.join('\n')}`,
      ).toHaveLength(0)
    })
  })
}
