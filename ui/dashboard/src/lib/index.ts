import {
  iconNameOverrides as iconNameOverridesStore,
  useLibIcons as useLibIconsStore,
} from '$lib/stores/media'

export interface InitOptions {
  useLibIcons: boolean
  iconNameOverrides: any
}

export const init = (options: InitOptions) => {
  const { useLibIcons = true, iconNameOverrides = {} } = options

  useLibIconsStore.set(useLibIcons)
  iconNameOverridesStore.set(iconNameOverrides)
}
