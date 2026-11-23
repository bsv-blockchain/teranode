import { describe, expect, it, vi } from 'vitest'
import { buildHashBadges, hueForHash } from './data'

vi.mock('$app/navigation', () => ({ goto: vi.fn() }))

const A = 'a'.repeat(64)
const B = '1'.repeat(64)
const C = '7'.repeat(64)

const nodes = (...hashes: string[]) => hashes.map((h) => ({ best_block_hash: h }))

describe('buildHashBadges', () => {
  it('gives nodes on the same tip the same colour and counts them', () => {
    const badges = buildHashBadges(nodes(A, A, B))

    expect(badges.get(A)).toMatchObject({ count: 2, total: 3 })
    expect(badges.get(B)).toMatchObject({ count: 1, total: 3 })
    expect(badges.get(A)!.color).not.toBe(badges.get(B)!.color)
  })

  it('keeps the colour of a tip when other nodes move between tips', () => {
    const before = buildHashBadges(nodes(A, A, A, A, B))
    const after = buildHashBadges(nodes(A, B, B, B, B))

    expect(after.get(A)!.color).toBe(before.get(A)!.color)
    expect(after.get(B)!.color).toBe(before.get(B)!.color)
  })

  it('does not depend on the order of the nodes', () => {
    expect(buildHashBadges(nodes(A, B, C)).get(B)!.color).toBe(
      buildHashBadges(nodes(C, B, A)).get(B)!.color,
    )
  })

  it('falls back to the hash field used by mining_on messages', () => {
    const badges = buildHashBadges([{ hash: A }, { best_block_hash: A }])

    expect(badges.get(A)).toMatchObject({ count: 2, total: 2 })
  })

  it('ignores nodes without a hash and handles empty input', () => {
    expect(buildHashBadges([{}, { best_block_hash: '' }]).size).toBe(0)
    expect(buildHashBadges(undefined as any).size).toBe(0)
  })
})

describe('hueForHash', () => {
  it('uses the trailing characters, because block hashes start with proof-of-work zeros', () => {
    const lead = '00000000000000000000'
    const x = lead + '1'.repeat(44)
    const y = lead + '2'.repeat(43) + '9'

    expect(hueForHash(x)).not.toBe(hueForHash(y))
    expect(hueForHash(x)).toBe(hueForHash('f'.repeat(20) + '1'.repeat(44)))
  })

  it('stays clear of the red range used for error state', () => {
    for (const h of ['0'.repeat(64), 'f'.repeat(64), '0'.repeat(56) + 'fffffff0', A, B, C]) {
      const hue = hueForHash(h)

      expect(hue).toBeGreaterThanOrEqual(40)
      expect(hue).toBeLessThan(320)
    }
  })

  it('returns a valid hue for a non-hex string', () => {
    expect(hueForHash('zzzz')).toBe(40)
  })
})
