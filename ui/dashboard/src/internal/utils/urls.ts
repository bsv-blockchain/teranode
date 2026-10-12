import { goto } from '$app/navigation'
import { reverseHash } from '$internal/utils/hashes'
import { shortHash } from '$lib/utils/format'

export enum DetailType {
  block = 'block',
  subtree = 'subtree',
  tx = 'tx',
  utxo = 'utxo',
}

export enum DetailTab {
  overview = 'overview',
  json = 'json',
  merkleproof = 'merkleproof',
}

export interface ExtraParams {
  tab?: DetailTab
  blockHash?: string
  index?: number
}

export const getDetailsUrl = (type: string, hash: string, extra?: ExtraParams) => {
  if (!DetailType[type]) {
    console.log('Warning, trying to nagivate to unsupported details type ', type)
  }
  // Zero is a real position, so the index cannot be gated on truthiness. A
  // value that is not a non-negative safe integer is dropped rather than
  // serialized as undefined or NaN.
  const indexParam =
    extra?.index !== undefined && Number.isSafeInteger(extra.index) && extra.index >= 0
      ? `&index=${extra.index}`
      : ''
  return `/viewer/${type}/?hash=${hash}${extra?.tab ? `&tab=${extra.tab}` : ''}${
    extra?.blockHash ? `&blockHash=${extra.blockHash}` : ''
  }${indexParam}`
}

// Presentation-only context read straight from a page URL: the subtree's
// zero-based position inside the named block. A usable value is exactly one
// occurrence of one or more ASCII decimal digits that converts to a
// non-negative safe integer, next to a non-empty block hash. Everything else
// leaves the subtree headings unnumbered. This is not proof of membership.
export const getSubtreeIndex = (params: URLSearchParams): number | undefined => {
  if (!params.get('blockHash')) {
    return undefined
  }
  const values = params.getAll('index')
  if (values.length !== 1 || !/^[0-9]+$/.test(values[0])) {
    return undefined
  }
  const index = Number(values[0])
  return Number.isSafeInteger(index) && index >= 0 ? index : undefined
}

export const getQueryParam = (key: string) => {
  return new URLSearchParams(window.location.search).get(key)
}

export const setQueryParam = (key: string, value: string, replaceState = true) => {
  const query = new URLSearchParams(window.location.search)
  query.set(key, value)
  goto(`?${query.toString()}`, { replaceState })
}

export const reverseHashParam = (hash) => {
  if (hash && hash.length === 64) {
    setQueryParam('hash', reverseHash(hash), false)
  }
}

export const getHashLinkProps = (type: string, hash: string, t, short = true) => {
  if (!DetailType[type]) {
    return {}
  }
  return {
    href: getDetailsUrl(type, hash),
    text: short ? shortHash(hash) : hash,
    external: false,
    icon: 'icon-duplicate-line',
    iconValue: hash,
    iconSize: 13,
    iconPadding: '6px 0 0 2px',
    tooltip: t('tooltip.copy-hash-to-clipboard'),
  }
}
