import i18next from 'i18next'
import { createI18nStore } from 'svelte-i18next'

import txtEn from './common/en.json'

i18next.init({
  lng: 'en',
  resources: {
    en: {
      translation: txtEn,
    },
  },
  interpolation: {
    escapeValue: false, // not needed for svelte as it escapes by default
  },
})

const i18n = createI18nStore(i18next)

export default i18n
