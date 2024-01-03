import { success, failure } from './notifications'
import type { I18n } from '../types'

export async function copyTextToClipboard(i18n: I18n, text: string) {
  try {
    await navigator.clipboard.writeText(text)
    success(i18n.t('notifications.copy-success'))
    return { ok: true }
  } catch (error) {
    failure(i18n.t('notifications.copy-failure'))
    return { error }
  }
}

export async function copyTextToClipboardVanilla(text: string) {
  try {
    await navigator.clipboard.writeText(text)
    return { ok: true }
  } catch (error) {
    return { ok: false, error }
  }
}
