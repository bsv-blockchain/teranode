import { palette, white, transparent } from '../constants/colour'

import { InputStyleSize } from '../types/input'
import { toUnit } from '../utils/css'

export const SwitchWidth = {
  sm: 32,
  md: 36,
  lg: 48,
}

export const SwitchHeight = {
  sm: 16,
  md: 18,
  lg: 24,
}

export const SwitchPadding = {
  sm: 2,
  md: 2,
  lg: 3,
}

export const IconSize = {
  sm: 12,
  md: 14,
  lg: 18,
}

const sizes = Object.values(InputStyleSize)

export const switchh = {
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          width: toUnit(SwitchWidth[size]),
          height: toUnit(SwitchHeight[size]),
          padding: toUnit(SwitchPadding[size]),
          icon: {
            size: toUnit(IconSize[size]),
          },
        },
      }),
      {},
    ),
  },
  default: {
    enabled: {
      color: white,
      bg: {
        color: palette.gray[600],
      },
      border: {
        color: transparent,
      },
    },
    hover: {
      color: white,
      bg: {
        color: palette.gray[600],
      },
      border: {
        color: transparent,
      },
    },
    focused: {
      color: white,
      bg: {
        color: palette.primary[600],
      },
      border: {
        color: transparent,
      },
    },
    checked: {
      color: white,
      bg: {
        color: palette.primary[600],
      },
      border: {
        color: transparent,
      },
    },
    disabled: {
      color: palette.gray[200],
      bg: {
        color: palette.gray[400],
      },
      border: {
        color: palette.gray[400],
      },
    },
    ['checked-disabled']: {
      color: palette.gray[200],
      bg: {
        color: palette.primary[400],
      },
      border: {
        color: transparent,
      },
    },
    invalid: {
      border: {
        color: palette.danger[600],
      },
    },
  },
}
