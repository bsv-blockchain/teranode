import { palette, white, transparent } from '../constants/colour'
import { heading } from '../constants/typography'

import { toUnit } from '../utils/css'

export const HeaderPaddingX = {
  xs: 16,
  sm: 24,
  md: 32,
  lg: 32,
  xl: 32,
}

export const HeaderPaddingY = {
  xs: 14,
  sm: 14,
  md: 14,
  lg: 14,
  xl: 14,
}

export const sizes = Object.keys(HeaderPaddingX)

export const header = {
  height: toUnit(60),
  bg: {
    color: palette.primary[600],
  },
  menu: {
    icon: {
      size: toUnit(24),
      wrapper: {
        size: toUnit(32),
        padding: toUnit(4),
        margin: {
          right: toUnit(25),
        },
      },
    },
  },
  logo: {
    color: white,
    width: toUnit(68),
    height: toUnit(28),
    margin: {
      right: toUnit(25),
    },
  },
  tab: {
    gap: toUnit(16),
    padding: `${toUnit(9)} ${toUnit(8)}`,
    bg: {
      color: palette.primary[700],
    },
    border: {
      radius: toUnit(4),
    },
    ...heading[6],
    font: {
      ...heading[6].font,
      family: heading.font.family,
      weight: heading.font.weight,
    },
    color: white,
    default: {
      enabled: {
        bg: {
          color: transparent,
        },
      },
      hover: {
        bg: {
          color: palette.primary[700],
        },
      },
    },
  },
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          padding: `${toUnit(HeaderPaddingY[size])} ${toUnit(HeaderPaddingX[size])}`,
        },
      }),
      {},
    ),
  },
}
