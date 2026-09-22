import type { Page } from '@playwright/test'
import { test, expect } from './fixtures'
import subtreeDetail from './mocks/fixtures/subtree-detail.json' with { type: 'json' }

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

// Both subtree headings must show the subtree's zero-based position inside the
// block, which is what the block's subtree table already displays. The subtree
// fixture gives every subtree the same Merkle height (4), so a heading that
// still interpolates that height cannot tell two subtrees apart, and a heading
// that reads position 4 is showing height. Numbering needs both a block hash
// and a usable index in the page URL; anything else renders generic headings.
const SECOND_SUBTREE_HASH = 'b'.repeat(64)

// Card renders its heading as a .title div, not a semantic heading element.
// This selector also keeps the breadcrumb "Subtree Details" out of the match.
const CARD_TITLES = '.tui-card > .header > .title-container > .title'

const subtreeTitles = (page: Page) => page.locator(CARD_TITLES)

// The application's own loading overlay is its request-quiescence signal. The
// layout renders <Spinner> while $spinCount > 0, and that count is incremented
// and decremented around every API call, so the spinner is present for exactly
// as long as work is outstanding. Svelte keeps the element mounted until its
// out-transition finishes, which makes a zero count proof that the work ended
// and rendered rather than proof that it is on its way out. Asserting console
// errors only after that point stops the check from running while a fetch
// started by the page is still in flight. Neither route polls, so the quiet
// state holds. This is a state assertion, not a timer.
const LOADING_SPINNER = 'svg.svelte-spinner'

async function expectNoConsoleErrorsOnceSettled(page: Page, consoleErrors: string[]) {
  await page.waitForLoadState('networkidle')
  await expect(page.locator(LOADING_SPINNER)).toHaveCount(0)
  expect(consoleErrors, `console errors:\n${consoleErrors.join('\n')}`).toHaveLength(0)
}

async function expectSubtreeTitles(page: Page, index?: number) {
  const suffix = index === undefined ? '' : ` #${index}`
  await expect(subtreeTitles(page).filter({ hasText: /^Subtree Details/ })).toHaveText(
    `Subtree Details${suffix}`,
  )
  await expect(subtreeTitles(page).filter({ hasText: /^Transactions for Subtree/ })).toHaveText(
    `Transactions for Subtree${suffix}`,
  )
}

function subtreeUrl(index?: string, withBlock = true) {
  const params = new URLSearchParams({ hash: DETAIL_HASH })
  if (withBlock) params.set('blockHash', FIRST_BLOCK_HASH)
  if (index !== undefined) params.set('index', index)
  return `/viewer/subtree/?${params}`
}

async function clickSubtreeNavigation(page: Page, href: string) {
  // A real anchor click goes through SvelteKit's delegated client router, so
  // the subtree component stays mounted and only reactivity can update the
  // headings. location.assign, raw pushState or another page.goto would
  // remount or reload and prove nothing. The anchor is appended inside the
  // application root because that is where the router listens for clicks.
  await page.locator(PAGE_ROOT).evaluate((root, target) => {
    const anchor = document.createElement('a')
    anchor.id = 'smoke-subtree-navigation'
    anchor.href = target
    anchor.textContent = 'Navigate subtree test context'
    anchor.style.cssText =
      'position:fixed;top:0;left:0;z-index:2147483647;background:white;color:black'
    root.append(anchor)
  }, href)
  await page.locator('#smoke-subtree-navigation').click()
  await expect(page).toHaveURL(new URL(href, page.url()).href)
  await page.locator('#smoke-subtree-navigation').evaluateAll((anchors) => {
    anchors.forEach((anchor) => anchor.remove())
  })
}

test.describe('smoke: /viewer/subtree position', () => {
  test('both block-table links carry the zero-based API index', async ({
    smokePage,
    consoleErrors,
  }) => {
    // The API rows arrive in reverse index order, so a link built from the
    // visible row offset would carry 0 where the API says 1 and vice versa.
    await smokePage.route(
      /\/api\/(?:[^/?#]+\/)?block\/[a-fA-F0-9]+\/subtrees\/json(?:\?|$)/,
      (route) =>
        route.fulfill({
          json: {
            data: [
              { index: 1, hash: SECOND_SUBTREE_HASH, txCount: 0, fee: 5000, size: 248123 },
              { index: 0, hash: DETAIL_HASH, txCount: 0, fee: 5000, size: 248123 },
            ],
            pagination: { limit: 10, offset: 0, totalRecords: 2 },
          },
        }),
    )
    await smokePage.goto(`/viewer/block/?hash=${FIRST_BLOCK_HASH}`)

    // Both the index cell and the hash cell link to the same subtree, and both
    // must carry hash, block hash and index. Parse the URLs so parameter order
    // is irrelevant.
    for (const [hash, index] of [
      [DETAIL_HASH, 0],
      [SECOND_SUBTREE_HASH, 1],
    ] as const) {
      const links = smokePage.locator(`a[href^="/viewer/subtree/"][href*="hash=${hash}"]`)
      await expect(links).toHaveCount(2)
      for (const link of await links.all()) {
        const href = await link.getAttribute('href')
        expect(href).not.toBeNull()
        const url = new URL(href ?? '', smokePage.url())
        expect(url.searchParams.get('hash')).toBe(hash)
        expect(url.searchParams.get('blockHash')).toBe(FIRST_BLOCK_HASH)
        expect(url.searchParams.getAll('index')).toEqual([String(index)])
      }
    }

    // Index-cell path, position zero.
    const indexLink = smokePage.locator(
      `a.num[href^="/viewer/subtree/"][href*="hash=${DETAIL_HASH}"]`,
    )
    await expect(indexLink).toHaveText('0')
    await indexLink.click()
    await expectSubtreeTitles(smokePage, 0)
    expect(new URL(smokePage.url()).searchParams.get('hash')).toBe(DETAIL_HASH)
    await expect(smokePage.locator('.subtitle .hash')).toHaveText(DETAIL_HASH)

    // Hash-cell path, position one, reached through the details card's block
    // link so the whole round trip stays client-side.
    await smokePage.locator(`a[href="/viewer/block/?hash=${FIRST_BLOCK_HASH}"]`).click()
    const hashLink = smokePage.locator(
      `a:not(.num)[href^="/viewer/subtree/"][href*="hash=${SECOND_SUBTREE_HASH}"]`,
    )
    await expect(hashLink).toBeVisible()
    await hashLink.click()
    await expectSubtreeTitles(smokePage, 1)
    const finalUrl = new URL(smokePage.url())
    expect(finalUrl.searchParams.get('hash')).toBe(SECOND_SUBTREE_HASH)
    expect(finalUrl.searchParams.get('blockHash')).toBe(FIRST_BLOCK_HASH)
    expect(finalUrl.searchParams.get('index')).toBe('1')
    await expect(smokePage.locator('.subtitle .hash')).toHaveText(SECOND_SUBTREE_HASH)
    await expectNoConsoleErrorsOnceSettled(smokePage, consoleErrors)
  })

  const GENERIC_CONTEXTS: [string, string][] = [
    ['hash only', subtreeUrl(undefined, false)],
    ['block without index', subtreeUrl()],
    ['index without block', subtreeUrl('0', false)],
    ['invalid index with block', subtreeUrl('-1')],
  ]

  for (const [name, url] of GENERIC_CONTEXTS) {
    test(`renders generic headings for ${name}`, async ({ smokePage, consoleErrors }) => {
      await smokePage.goto(url)
      await expectSubtreeTitles(smokePage)
      // Hash identity survives the missing position.
      await expect(smokePage.locator('.subtitle .hash')).toHaveText(DETAIL_HASH)
      await expectNoConsoleErrorsOnceSettled(smokePage, consoleErrors)
    })
  }

  test('mounted headings follow index-only navigation and browser history', async ({
    smokePage,
    consoleErrors,
  }) => {
    const zero = subtreeUrl('0')
    const seven = subtreeUrl('7')
    const absent = subtreeUrl()
    const duplicate = `${subtreeUrl('7')}&index=7`

    await smokePage.goto(zero)
    await expectSubtreeTitles(smokePage, 0)

    // Test-owned sentinels, not product hooks: the title attribute disappears
    // if the card remounts, the html attribute disappears if the document
    // reloads. Both surviving means reactivity, not a fresh page, changed the
    // headings. Hash and block hash never change here, so the existing fetch
    // guard does not refetch and cannot be the explanation either.
    const detailsTitle = subtreeTitles(smokePage).filter({ hasText: /^Subtree Details/ })
    await smokePage.locator('html').evaluate((element) => {
      element.setAttribute('data-subtree-document', 'retained')
    })
    await detailsTitle.evaluate((element) => {
      element.setAttribute('data-subtree-mounted', 'retained')
    })

    const transitions: [string, number | undefined][] = [
      [seven, 7],
      [absent, undefined],
      [duplicate, undefined],
    ]
    for (const [url, index] of transitions) {
      await clickSubtreeNavigation(smokePage, url)
      await expectSubtreeTitles(smokePage, index)
      await expect(detailsTitle).toHaveAttribute('data-subtree-mounted', 'retained')
      await expect(smokePage.locator('html')).toHaveAttribute('data-subtree-document', 'retained')
      await expect(smokePage.locator('.subtitle .hash')).toHaveText(DETAIL_HASH)
    }

    await smokePage.goBack()
    await expect(smokePage).toHaveURL(new URL(absent, smokePage.url()).href)
    await expectSubtreeTitles(smokePage)
    await smokePage.goBack()
    await expect(smokePage).toHaveURL(new URL(seven, smokePage.url()).href)
    await expectSubtreeTitles(smokePage, 7)
    await smokePage.goBack()
    await expect(smokePage).toHaveURL(new URL(zero, smokePage.url()).href)
    await expectSubtreeTitles(smokePage, 0)
    await smokePage.goForward()
    await expectSubtreeTitles(smokePage, 7)
    await smokePage.goForward()
    await expect(smokePage).toHaveURL(new URL(absent, smokePage.url()).href)
    await expectSubtreeTitles(smokePage)
    await expect(detailsTitle).toHaveAttribute('data-subtree-mounted', 'retained')
    await expect(smokePage.locator('html')).toHaveAttribute('data-subtree-document', 'retained')
    await expectNoConsoleErrorsOnceSettled(smokePage, consoleErrors)
  })

  test('tab changes retain index, hash, and API Height', async ({ smokePage, consoleErrors }) => {
    await smokePage.route(
      /\/api\/(?:[^/?#]+\/)?merkle_proof\/[a-fA-F0-9]+\/json(?:\?|$)/,
      (route) =>
        route.fulfill({
          json: {
            blockHeight: 800004,
            path: [],
            subtreeIndex: 0,
            subtreeRoot: DETAIL_HASH,
            merkleRoot: DETAIL_HASH,
            blockProof: [],
          },
        }),
    )
    await smokePage.goto(subtreeUrl('0'))
    await expectSubtreeTitles(smokePage, 0)

    const detailsTitle = subtreeTitles(smokePage).filter({ hasText: /^Subtree Details/ })
    const tabs: [string, string][] = [
      ['JSON', 'json'],
      ['Overview', 'overview'],
    ]
    for (const [label, tab] of tabs) {
      await smokePage.getByRole('button', { name: label, exact: true }).click()
      await expect.poll(() => new URL(smokePage.url()).searchParams.get('tab')).toBe(tab)
      const params = new URL(smokePage.url()).searchParams
      expect(params.get('hash')).toBe(DETAIL_HASH)
      expect(params.get('blockHash')).toBe(FIRST_BLOCK_HASH)
      expect(params.get('index')).toBe('0')
      await expect(detailsTitle).toHaveText('Subtree Details #0')
      await expect(smokePage.locator('.subtitle .hash')).toHaveText(DETAIL_HASH)
      if (tab === 'json') {
        // The API's Merkle height is presented unchanged; only the heading
        // stops borrowing it.
        await expect(smokePage.locator('.json-tree').first()).toContainText(
          new RegExp(`Height:\\s*${subtreeDetail.data.Height}\\b`),
        )
      } else {
        await expectSubtreeTitles(smokePage, 0)
      }
    }
    await expectNoConsoleErrorsOnceSettled(smokePage, consoleErrors)
  })
})

// Old subtree proof links must remain usable without requesting a tx-only proof.
test('subtree proof links fall back to overview', async ({ smokePage }) => {
  const proofRequests: string[] = []
  smokePage.on('request', (request) => {
    if (request.url().includes('/merkle_proof/')) proofRequests.push(request.url())
  })
  await smokePage.goto(`/viewer/subtree/?hash=${DETAIL_HASH}&tab=merkleproof`)
  await expect(smokePage.getByRole('button', { name: 'Overview', exact: true })).toBeVisible()
  await expect(smokePage.getByRole('button', { name: /merkle proof/i })).toHaveCount(0)
  await expect(smokePage.locator('.fields')).toBeVisible()
  await smokePage.getByRole('button', { name: 'JSON', exact: true }).click()
  await expect(smokePage.locator('.json')).toBeVisible()
  expect(proofRequests).toEqual([])
})
