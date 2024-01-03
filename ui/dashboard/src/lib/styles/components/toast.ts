import { palette } from '../constants/colour'

import { toUnit } from '../utils/css'

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
      color: 'rgba(0, 0, 0, 0.1)',
    },
  },
  success: {
    bg: {
      color: palette.success[50],
    },
    border: {
      color: palette.success[600],
    },
  },
  failure: {
    bg: {
      color: palette.danger[50],
    },
    border: {
      color: palette.danger[600],
    },
  },
  warn: {
    bg: {
      color: palette.accent[50],
    },
    border: {
      color: palette.accent[600],
    },
  },
  info: {
    bg: {
      color: palette.secondary[50],
    },
    border: {
      color: palette.secondary[600],
    },
  },
}
