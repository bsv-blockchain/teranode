import { writable } from 'svelte/store'
import { localStore } from './localStore'
import type { I18n } from '../types'

export enum MediaSize {
  xs = 0,
  sm = 1,
  md = 2,
  lg = 3,
  xl = 4,
}

// media queries
export const mediaSize = writable<MediaSize>(MediaSize.lg)

// theme
export const theme = localStore('theme', 'light')
export const themeNs = localStore('themeNs', '')

// i18n
export const i18n = writable<I18n>()

// icons
export const useLibIcons = writable(true)
export const iconNameOverrides = writable({})

// logos
export const injectedLogos = writable({})

// tippy
export const tippy = writable<any>(null)
