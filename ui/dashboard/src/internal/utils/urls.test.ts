import { describe, expect, it, vi } from 'vitest'
import { DetailTab, DetailType, getDetailsUrl, getSubtreeIndex } from './urls'

// urls.ts imports goto for setQueryParam. Neither the parser nor the URL
// builder under test navigates, so the SvelteKit runtime stays out of this
// pure unit test.
vi.mock('$app/navigation', () => ({ goto: vi.fn() }))

// A usable subtree position is exactly one query occurrence of one or more
// ASCII decimal digits that converts to a non-negative safe integer, next to a
// non-empty block hash. Leading zeros normalize. Anything else is untrusted
// URL text and must not number a heading.
const USABLE_INDEX_VALUES: [string, number][] = [
  ['0', 0],
  ['1', 1],
  ['17', 17],
  ['0007', 7],
  ['9007199254740991', Number.MAX_SAFE_INTEGER],
]

// Permissive parsing (parseInt, Number, truthiness) accepts most of these.
// '0x10' and the full-width digit fail only if the parser demands ASCII
// decimal digits; '9007199254740992' fails only on the safe-integer check.
const MALFORMED_INDEX_VALUES: string[] = [
  '',
  '-1',
  '+1',
  '1.5',
  '1e2',
  '1x',
  ' ',
  ' 1',
  '1 ',
  'NaN',
  'Infinity',
  '9007199254740992',
  '9'.repeat(400),
  '１',
  '0x10',
]

// Numbering needs block context as well as an index, and a repeated parameter
// is ambiguous even when both occurrences agree.
const UNUSABLE_CONTEXT_QUERIES: string[] = [
  '',
  'blockHash=block',
  'index=0',
  'blockHash=&index=0',
  'blockHash=block&index=0&index=1',
  'blockHash=block&index=0&index=0',
  'blockHash=block&index=&index=1',
]

const OMITTED_INDEX_VALUES: (number | undefined)[] = [
  undefined,
  -1,
  1.5,
  NaN,
  Infinity,
  Number.MAX_SAFE_INTEGER + 1,
]

describe('getSubtreeIndex', () => {
  it.each(USABLE_INDEX_VALUES)('accepts decimal %s with block context', (raw, expected) => {
    expect(getSubtreeIndex(new URLSearchParams({ blockHash: 'block', index: raw }))).toBe(expected)
  })

  it.each(MALFORMED_INDEX_VALUES)('rejects malformed index %j', (index) => {
    expect(getSubtreeIndex(new URLSearchParams({ blockHash: 'block', index }))).toBeUndefined()
  })

  it.each(UNUSABLE_CONTEXT_QUERIES)('rejects absent or ambiguous context %j', (query) => {
    expect(getSubtreeIndex(new URLSearchParams(query))).toBeUndefined()
  })
})

describe('getDetailsUrl index context', () => {
  it.each([0, 1, 17, Number.MAX_SAFE_INTEGER])('preserves API index %s', (index) => {
    const href = getDetailsUrl(DetailType.subtree, 'subtree', {
      blockHash: 'block',
      tab: DetailTab.json,
      index,
    })
    const url = new URL(href, 'https://example.test')
    expect(url.pathname).toBe('/viewer/subtree/')
    expect(url.searchParams.get('hash')).toBe('subtree')
    expect(url.searchParams.get('blockHash')).toBe('block')
    expect(url.searchParams.get('tab')).toBe(DetailTab.json)
    // Zero survives serialization, and the parameter appears exactly once.
    expect(url.searchParams.getAll('index')).toEqual([String(index)])
  })

  it.each(OMITTED_INDEX_VALUES)('omits invalid numeric index %s', (index) => {
    const href = getDetailsUrl(DetailType.subtree, 'subtree', { blockHash: 'block', index })
    const url = new URL(href, 'https://example.test')
    expect(url.searchParams.has('index')).toBe(false)
    expect(url.searchParams.get('blockHash')).toBe('block')
  })

  it('preserves existing URLs when callers omit index', () => {
    expect(getDetailsUrl(DetailType.subtree, 'subtree')).toBe('/viewer/subtree/?hash=subtree')
    const withContext = getDetailsUrl(DetailType.subtree, 'subtree', {
      tab: DetailTab.json,
      blockHash: 'block',
    })
    expect(withContext).toBe('/viewer/subtree/?hash=subtree&tab=json&blockHash=block')
  })
})
