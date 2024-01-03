import { palette, purple } from '../constants/colour'

import { toUnit } from '../utils/css'
import { body } from '../constants/typography'

export const LinkBodySize = {
  sm: 4,
  md: 3,
  lg: 2,
}

export const LinkIconSize = {
  sm: 14,
  md: 16,
  lg: 18,
}

const sizes = Object.keys(LinkBodySize)

export const link = {
  gap: toUnit(5),
  font: {
    family: body.font.family,
    weight: body.font.weight,
  },
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          icon: {
            size: toUnit(LinkIconSize[size]),
          },
          ...body[LinkBodySize[size]],
        },
      }),
      {},
    ),
  },
  default: {
    enabled: {
      color: palette.secondary[600],
    },
    hover: {
      color: palette.secondary[800],
    },
    active: {
      color: palette.secondary[800],
    },
    bold: {
      color: palette.secondary[600],
      text: {
        decoration: {
          line: 'underline',
        },
      },
    },
    visited: {
      color: purple,
    },
  },
}
