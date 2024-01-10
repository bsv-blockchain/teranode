import { palette } from './constants/colour'

import { toUnit } from '$lib/styles/utils/css'

export const toast = {
  width: toUnit(307),
  padding: `${toUnit(16)} ${toUnit(16)} ${toUnit(18)} ${toUnit(16)}`,
  border: {
    radius: toUnit(4),
    width: toUnit(1),
    style: 'solid',
  },
  bar: {
    bg: {
      color: 'rgba(255, 255, 255, 0.2)',
    },
  },
  success: {
    bg: {
      color: 'var(--app-bg-color)', //palette.success[50],
    },
    border: {
      color: palette.success[600],
    },
  },
  failure: {
    bg: {
      color: 'var(--app-bg-color)', //palette.danger[50],
    },
    border: {
      color: palette.danger[600],
    },
  },
  warn: {
    bg: {
      color: 'var(--app-bg-color)', //palette.accent[50],
    },
    border: {
      color: palette.accent[600],
    },
  },
  info: {
    bg: {
      color: 'var(--app-bg-color)', //palette.secondary[50],
    },
    border: {
      color: palette.secondary[600],
    },
  },
}
