import { readable, writable } from 'svelte/store'
import { localStore } from '$lib/stores/localStore'

// nav
export const pageLinks = writable<any | null>(null)
export const productLinks = writable<any | null>(null)
export const sidebarSections = writable<any[]>([])
export const hasProducts = writable<boolean>(false)

// content
export const contentLeft = localStore('contentLeft', 212)
export const maxPageContentWidth = readable(1400)

// spinner
export const spinCount = writable(0)

// table
export const tableVariant = localStore('tableVariant', 'div')

export const headerHeight = writable(0)
