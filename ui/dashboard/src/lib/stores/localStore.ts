import { writable } from 'svelte/store'

const setLocalValue = (key: string, value: any) => {
  let useValue = value

  if (typeof value === 'object') {
    useValue = JSON.stringify(value)
  }

  localStorage.setItem(key, useValue)
  return useValue
}

const getLocalValue = (key: string) => {
  const value = localStorage.getItem(key)

  if (!value) {
    return null
  }

  let useValue: any = value

  try {
    useValue = JSON.parse(value)
  } catch (e) {
    useValue = value
  }

  return useValue
}

const initialised: any = {}

export const localStore = (key, initial) => {
  if (!initialised[key] && getLocalValue(key) === null) {
    setLocalValue(key, initial)
    initialised[key] = true
  }

  const saved = getLocalValue(key)

  const { subscribe, set, update } = writable(saved)

  return {
    subscribe,
    set: (value) => {
      setLocalValue(key, value)
      return set(value)
    },
    update,
  }
}
