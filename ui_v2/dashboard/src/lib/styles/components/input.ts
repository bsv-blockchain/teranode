import { palette, white } from '../constants/colour'

import { ComponentBorderRadius, ComponentHeight, ComponentLetterSpacing } from './defaults'
import { toUnit } from '../utils/css'
import { InputStyleSize } from '../types/input'
import { fontFamily } from '../constants/typography'
import { body, fontWeight } from '../constants/typography'

export const InputPaddingX = {
  sm: 16,
  md: 16,
  lg: 16,
}

export const InputPaddingY = {
  sm: 1, //7,
  md: 1, //10,
  lg: 1, //14,
}

export const InputIconSize = {
  sm: 16,
  md: 18,
  lg: 18,
}

export const TypoBodySize = {
  sm: 4,
  md: 3,
  lg: 3,
}

const sizes = Object.values(InputStyleSize)

export const input = {
  font: {
    family: fontFamily.inter,
    weight: fontWeight.regular,
  },
  placeholder: {
    color: palette.gray[300],
  },
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          height: toUnit(ComponentHeight[size]),
          padding: `${toUnit(InputPaddingY[size])} ${toUnit(InputPaddingX[size])}`,
          border: {
            radius: toUnit(ComponentBorderRadius[size]),
          },
          icon: {
            size: toUnit(InputIconSize[size]),
          },
          ...body[TypoBodySize[size]],
          letter: {
            spacing: ComponentLetterSpacing,
          },
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
      color: palette.gray[800],
      bg: {
        color: white,
      },
      border: {
        color: palette.gray[300],
      },
    },
    active: {
      color: palette.gray[800],
      bg: {
        color: white,
      },
      border: {
        color: palette.primary[600],
      },
    },
    focus: {
      color: palette.gray[800],
      bg: {
        color: white,
      },
      border: {
        color: palette.primary[600],
      },
    },
    disabled: {
      color: palette.gray[500],
      bg: {
        color: palette.gray[200],
      },
      border: {
        color: palette.gray[200],
      },
    },
    invalid: {
      border: {
        color: palette.danger[600],
      },
    },
  },
}
