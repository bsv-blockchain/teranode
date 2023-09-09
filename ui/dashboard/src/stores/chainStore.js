import { writable } from 'svelte/store'

export const blocks = writable([])
export const isFirstMount = writable(true)
