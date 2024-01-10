import { palette, white } from '../constants/colour'

import { ComponentLetterSpacing } from './defaults'
import { toUnit } from '../utils/css'
import { InputStyleSize } from '../types/input'
import { fontFamily } from '../constants/typography'
import { body, fontWeight } from '../constants/typography'

export const CheckboxSize = {
  sm: 12,
  md: 14,
  lg: 18,
}

export const CheckboxBorderRadius = 2

export const TypoBodySize = {
  sm: 4,
  md: 3,
  lg: 2,
}

const sizes = Object.values(InputStyleSize)

export const checkbox = {
  color: body.color,
  font: {
    family: fontFamily.inter,
    weight: fontWeight.regular,
  },
  letter: {
    spacing: ComponentLetterSpacing,
  },
  border: {
    radius: toUnit(CheckboxBorderRadius),
  },
  label: {
    color: body.color,
    font: { ...body.font },
  },
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          size: toUnit(CheckboxSize[size]),
          ...body[TypoBodySize[size]],
        },
      }),
      {},
    ),
  },
  default: {
    enabled: {
      color: white,
      bg: {
        color: white,
      },
      border: {
        color: palette.primary[600],
      },
    },
    hover: {
      color: white,
      bg: {
        color: white,
      },
      border: {
        color: palette.primary[600],
      },
    },
    focused: {
      color: white,
      bg: {
        color: white,
      },
      border: {
        color: palette.primary[600],
      },
    },
    checked: {
      color: white,
      bg: {
        color: palette.primary[600],
      },
      border: {
        color: palette.primary[600],
      },
    },
    disabled: {
      color: white,
      bg: {
        color: palette.gray[400],
      },
      border: {
        color: palette.gray[400],
      },
    },
    invalid: {
      border: {
        color: palette.danger[600],
      },
    },
  },
}
