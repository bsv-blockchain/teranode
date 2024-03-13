import { palette } from './constants/colour'

import { ComponentBorderRadius, ComponentHeight, ComponentLetterSpacing } from './defaults'
import { toUnit } from '$lib/styles/utils/css'
import { InputStyleSize } from '$lib/styles/types/input'
import { fontFamily, text, fontWeight, getTypoProps } from './constants/typography'

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
    family: fontFamily.satoshi,
    weight: fontWeight.regular,
  },
  placeholder: {
    color: '#CACACA',
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
          // ...body[TypoBodySize[size]],
          ...getTypoProps(text, size === 'md' ? 'sm' : size),
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
      color: 'rgba(255, 255, 255, 0.88)',
      bg: {
        color: '#0A1018',
      },
      border: {
        color: 'rgba(255,255,255,0.11)',
      },
    },
    hover: {
      color: 'rgba(255, 255, 255, 0.88)',
      bg: {
        color: '#0A1018',
      },
      border: {
        color: 'rgba(255, 255, 255, 0.88)',
      },
    },
    active: {
      color: 'rgba(255, 255, 255, 0.88)',
      bg: {
        color: '#0A1018',
      },
      border: {
        color: 'rgba(255,255,255,0.11)',
      },
    },
    focus: {
      color: 'rgba(255, 255, 255, 0.88)',
      bg: {
        color: '#0A1018',
      },
      border: {
        color: 'rgba(255, 255, 255, 0.88)',
      },
    },
    disabled: {
      color: 'rgba(255, 255, 255, 0.66)',
      bg: {
        color:
          'linear-gradient(0deg, rgba(239, 243, 253, 0.05) 0%, rgba(239, 243, 253, 0.05) 100%), #0A1018',
      },
      border: {
        color: 'rgba(255,255,255,0.11)',
      },
    },
    invalid: {
      border: {
        color: palette.danger[600],
      },
    },
  },
}
