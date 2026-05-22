import { describe, expect, it } from 'vitest'

import { dark } from './dark'
import { light } from './light'

/**
 * Theme keys that carry mode-specific colours. Both themes MUST define each of
 * these as a top-level key, otherwise CSS variables fall back to `transparent`
 * (or other defaults) in one of the modes and render broken UI.
 *
 * Background: PR #790 added the light theme but forgot the `toggle` key. The
 * `clearAllCSSVariables()` pass before applying a theme then left
 * `--toggle-bg-color` unset, making the toggle invisible on dark. This guard
 * catches that class of regression at test time.
 */
const REQUIRED_TOP_LEVEL_KEYS = [
  'app',
  'comp',
  'dropdown',
  'focus',
  'footer',
  'input',
  'json',
  'link',
  'msgbox',
  'switch',
  'tab',
  'table',
  'toast',
  'toggle',
]

describe('theme key parity', () => {
  it.each(REQUIRED_TOP_LEVEL_KEYS)('dark theme defines %s', (key) => {
    expect((dark as Record<string, unknown>)[key], `dark theme missing ${key}`).toBeDefined()
  })

  it.each(REQUIRED_TOP_LEVEL_KEYS)('light theme defines %s', (key) => {
    expect((light as Record<string, unknown>)[key], `light theme missing ${key}`).toBeDefined()
  })

  it('toggle.bg.color is defined in both themes', () => {
    expect((dark as any).toggle?.bg?.color, 'dark toggle.bg.color missing').toBeTruthy()
    expect((light as any).toggle?.bg?.color, 'light toggle.bg.color missing').toBeTruthy()
  })
})
