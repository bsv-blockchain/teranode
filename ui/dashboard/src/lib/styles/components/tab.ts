import { palette, transparent, white } from '../constants/colour'

import { toUnit } from '../utils/css'
import { heading } from '../constants/typography'
import { ComponentBorderRadius } from './defaults'

export const TabHeight = {
  sm: 32,
  md: 40,
}

export const TabHeadingSize = {
  sm: 7,
  md: 6,
}

export const TabIconSize = {
  sm: 16,
  md: 18,
}

export const TabPaddingX = {
  sm: 16,
  md: 16,
}

export const TabPaddingY = {
  sm: 7,
  md: 10,
}

const sizes = Object.keys(TabHeight)

export const tab = {
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          height: toUnit(TabHeight[size]),
          padding: `${toUnit(TabPaddingY[size])} ${toUnit(TabPaddingX[size])}`,
          border: {
            radius: toUnit(ComponentBorderRadius[size]),
          },
          icon: {
            size: toUnit(TabIconSize[size]),
          },
          ...heading[TabHeadingSize[size]],
        },
      }),
      {},
    ),
  },
  default: {
    enabled: {
      color: palette.gray[800],
      bg: {
        color: white,
      },
      border: {
        color: palette.gray[300],
      },
    },
    hover: {
      color: palette.primary[600],
      bg: {
        color: palette.primary[50],
      },
      border: {
        color: palette.primary[500],
      },
    },
    active: {
      color: palette.gray[800],
      bg: {
        color: palette.accent[800],
      },
      border: {
        color: transparent,
      },
    },
    focus: {
      color: palette.gray[800],
      bg: {
        color: white,
      },
      border: {
        color: palette.primary[500],
      },
    },
    selected: {
      color: palette.primary[600],
      bg: {
        color: palette.primary[100],
      },
      border: {
        color: palette.primary[500],
      },
    },
    disabled: {
      color: palette.gray[400],
      bg: {
        color: white,
      },
      border: {
        color: palette.gray[300],
      },
    },
  },
}
