import { goto } from '$app/navigation'
import { reverseHash } from '$internal/utils/hashes'
import { shortHash } from '$lib/utils/format'

export enum DetailType {
  block = 'block',
  subtree = 'subtree',
  tx = 'tx',
}

export enum DetailTab {
  overview = 'overview',
  json = 'json',
}

export interface ExtraParams {
  tab?: DetailTab
  blockHash?: string
}

export const getDetailsUrl = (type: string, hash: string, extra?: ExtraParams) => {
  if (!DetailType[type]) {
    console.log('Warning, trying to nagivate to unsupported details type ', type)
  }
  return `/viewer/${type}/?hash=${hash}${extra?.tab ? `&tab=${extra.tab}` : ''}${
    extra?.blockHash ? `&blockHash=${extra.blockHash}` : ''
  }`
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
