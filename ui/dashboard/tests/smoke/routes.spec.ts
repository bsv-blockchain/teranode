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
      expect(
        consoleErrors,
        `console errors on ${route.path}:\n${consoleErrors.join('\n')}`,
      ).toHaveLength(0)
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
  test('opens block detail when first block hash is clicked', async ({
    smokePage,
    consoleErrors,
  }) => {
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
    expect(
      consoleErrors,
      `console errors on /viewer click-through:\n${consoleErrors.join('\n')}`,
    ).toHaveLength(0)
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

// The /peers page renders two tables from one payload: libp2p peers in the
// Teranode table, and wire-protocol peers in the Legacy Peers card. This
// asserts the transport split actually happened, which "no console errors"
// cannot show.
test.describe('smoke: /peers legacy card', () => {
  test('renders wire-protocol peers separately from libp2p peers', async ({
    smokePage,
    consoleErrors,
  }) => {
    const response = await smokePage.goto('/peers')
    expect(response?.ok(), '/peers should respond 2xx').toBe(true)
    await expect(smokePage.locator(PAGE_ROOT)).toBeVisible()

    const legacyTable = smokePage.locator('.legacy-table')
    await expect(legacyTable).toBeVisible()

    // The connected peer and the recently disconnected one appear. The libp2p
    // peer does not: it belongs to the Teranode table.
    const rows = legacyTable.locator('tbody tr')
    await expect(rows).toHaveCount(2)
    await expect(legacyTable).toContainText('203.0.113.7:8333')
    await expect(legacyTable).toContainText('198.51.100.9:8333')
    await expect(legacyTable).not.toContainText('12D3KooW')

    // The recency bound drops a peer that is both disconnected and stale, so a
    // flapping inbound peer cannot accumulate rows indefinitely. The fixture
    // serves this one two hours old.
    await expect(legacyTable).not.toContainText('198.51.100.55:8333')

    // The libp2p peer still reaches the Teranode table, which shares the same
    // payload. This guards the transport split from the other side. Match on
    // its client name, not its ID: that table's cell renderer shows
    // client_name when present and keeps the ID in the tooltip.
    await expect(smokePage.locator(PAGE_ROOT)).toContainText('teranode/1.0')
    await expect(legacyTable).not.toContainText('teranode/1.0')

    // The sync peer is badged, and the disconnected peer's cells are dimmed.
    await expect(legacyTable.locator('.badge.sync')).toHaveCount(1)
    await expect(
      legacyTable.locator('tbody tr', { has: smokePage.locator('.dimmed') }),
    ).toHaveCount(1)

    // Wire-protocol-only fields are surfaced.
    await expect(legacyTable).toContainText('70016')
    await expect(legacyTable).toContainText('42.0 ms')
    await expect(legacyTable).toContainText('-3s')

    await smokePage.waitForTimeout(1000)
    expect(
      consoleErrors,
      `console errors on /peers legacy card:\n${consoleErrors.join('\n')}`,
    ).toHaveLength(0)
  })
})
