// import { palette, purple } from './constants/colour'
import { purple } from './constants/colour'

import { toUnit } from '$lib/styles/utils/css'
import { text } from './constants/typography'

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
    family: text.font.family,
    weight: text.font.weight,
  },
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          icon: {
            size: toUnit(LinkIconSize[size]),
          },
          // ...body[LinkBodySize[size]],
          ...text[size],
        },
      }),
      {},
    ),
  },
  default: {
    enabled: {
      color: '#1778FF', //palette.secondary[600],
    },
    hover: {
      color: '#1778FF', //palette.secondary[800],
    },
    active: {
      color: '#1778FF', //palette.secondary[800],
    },
    bold: {
      color: '#1778FF', //palette.secondary[600],
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
