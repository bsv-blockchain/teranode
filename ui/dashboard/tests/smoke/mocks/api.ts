import type { Page, Route } from '@playwright/test'
import blockchainInfo from './fixtures/blockchain-info.json' with { type: 'json' }
import blockstats from './fixtures/blockstats.json' with { type: 'json' }
import peers from './fixtures/peers.json' with { type: 'json' }
import settings from './fixtures/settings.json' with { type: 'json' }
import blocks from './fixtures/blocks.json' with { type: 'json' }
import blockDetail from './fixtures/block-detail.json' with { type: 'json' }
import subtreeDetail from './fixtures/subtree-detail.json' with { type: 'json' }
import txDetail from './fixtures/tx-detail.json' with { type: 'json' }
import txmetaDetail from './fixtures/txmeta-detail.json' with { type: 'json' }

type MockOptions = {
  authenticated?: boolean
}

const EMPTY_OK = { ok: true, data: [] }

export async function installApiMocks(page: Page, opts: MockOptions = {}): Promise<void> {
  const authenticated = opts.authenticated ?? true

  // Catch-all for /api/* requests so nothing hangs. Registered FIRST so the
  // specific routes below take precedence (Playwright matches handlers LIFO).
  await page.route('**/api/**', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(EMPTY_OK) }),
  )

  await page.route('**/api/auth/check', (route: Route) =>
    route.fulfill({
      status: authenticated ? 200 : 401,
      contentType: 'application/json',
      body: JSON.stringify({ authenticated }),
    }),
  )

  // Asset HTTP API requests in prod/preview hit http://<host>:8090/api/v1/<path>,
  // while dev/auth requests stay on the same origin via the SvelteKit proxy
  // mounted at /api/<path>. We anchor with /api/ to avoid matching the SPA's
  // own page routes (e.g. /peers, /settings) and use a wildcard segment for v1.
  await page.route('**/api/**/blockchain/info', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(blockchainInfo) }),
  )
  await page.route('**/api/blockchain/info', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(blockchainInfo) }),
  )

  await page.route('**/api/**/blockstats**', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(blockstats) }),
  )
  await page.route('**/api/blockstats**', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(blockstats) }),
  )

  await page.route('**/api/**/peers**', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(peers) }),
  )
  await page.route('**/api/peers**', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(peers) }),
  )

  await page.route('**/api/**/settings**', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(settings) }),
  )
  await page.route('**/api/settings**', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(settings) }),
  )

  await page.route('**/api/config/websocket', (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ websocketUrl: 'ws://localhost:4173/ws-mock' }),
    }),
  )

  // /ancestors expects either { block_locator: string[] } or a bare array.
  // Production URL is /api/v1/block_locator; dev proxy mounts at
  // /api/blockchain/locator. Match both.
  const locatorBody = JSON.stringify({ block_locator: [] })
  await page.route('**/api/**/block_locator', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: locatorBody }),
  )
  await page.route('**/api/blockchain/locator', (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: locatorBody }),
  )

  // /admin reads { events: [...] } from /api/v1/fsm/events on the asset host.
  await page.route('**/api/**/fsm/events', (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ events: [] }),
    }),
  )

  // /viewer blocks list. Regex to avoid matching /blockstats. Matches
  // /api/blocks and /api/<segment>/blocks with optional query string.
  await page.route(/\/api\/(?:[^/?#]+\/)?blocks(?:\?|$)/, (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(blocks) }),
  )

  // /viewer/block detail page fetches three endpoints. Return the same block
  // body for /block/<hash>/json and /header/<hash>/json (the page merges them).
  await page.route(/\/api\/(?:[^/?#]+\/)?(?:block|header)\/[a-fA-F0-9]+\/json(?:\?|$)/, (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(blockDetail) }),
  )

  // /viewer/block subtrees pagination — empty list, valid pagination shape.
  // Note: server response is unwrapped; the api client wraps it as
  // `{ok: true, data: <body>}` before handing back to the caller.
  await page.route(/\/api\/(?:[^/?#]+\/)?block\/[a-fA-F0-9]+\/subtrees\/json(?:\?|$)/, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ data: [], pagination: { limit: 20, offset: 0, totalRecords: 0 } }),
    }),
  )

  // /viewer/block "latest block" lookup. Returns a raw array — client wraps it.
  await page.route(/\/api\/(?:[^/?#]+\/)?lastblocks(?:\?|$)/, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([blockDetail]),
    }),
  )

  // /viewer/subtree detail. getSubtreeNodes hits /subtree/<hash>/json and the
  // SubtreeTxsCard hits /subtree/<hash>/txs/json. The two patterns are
  // disjoint (…/json vs …/txs/json), so registration order between them is
  // irrelevant. Both return a raw body the client wraps in {ok, data}.
  await page.route(/\/api\/(?:[^/?#]+\/)?subtree\/[a-fA-F0-9]+\/json(?:\?|$)/, (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(subtreeDetail) }),
  )
  await page.route(/\/api\/(?:[^/?#]+\/)?subtree\/[a-fA-F0-9]+\/txs\/json(?:\?|$)/, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ data: [], pagination: { limit: 20, offset: 0, totalRecords: 0 } }),
    }),
  )

  // /viewer/tx detail composes /tx/<hash>/json (getItemData tx) and
  // /txmeta/<hash>/json (getItemData txmeta). The /tx/ literal does not match
  // /txmeta/, so the two patterns are disjoint.
  await page.route(/\/api\/(?:[^/?#]+\/)?tx\/[a-fA-F0-9]+\/json(?:\?|$)/, (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(txDetail) }),
  )
  await page.route(/\/api\/(?:[^/?#]+\/)?txmeta\/[a-fA-F0-9]+\/json(?:\?|$)/, (route: Route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(txmetaDetail) }),
  )
}
