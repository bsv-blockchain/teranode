import { palette, transparent } from './constants/colour'

import { toUnit } from '$lib/styles/utils/css'
import { text, getTypoProps } from './constants/typography'
import { ComponentBorderRadius } from './defaults'

export const TabHeight = {
  sm: 24,
  md: 32,
}

export const TabHeadingSize = {
  sm: 7,
  md: 6,
}

export const TabIconSize = {
  sm: 15,
  md: 17,
}

export const TabPaddingX = {
  sm: 4, //16,
  md: 4, //16,
}

export const TabPaddingY = {
  sm: 0, //7,
  md: 0, //10,
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
            width: toUnit(1),
          },
          icon: {
            size: toUnit(TabIconSize[size]),
          },
          // ...heading[TabHeadingSize[size]],
          ...getTypoProps(text, size),
        },
      }),
      {},
    ),
  },
  default: {
    enabled: {
      color: '#DDDEDF',
      bg: {
        color: transparent,
      },
      border: {
        color: transparent,
      },
    },
    hover: {
      color: '#E2E3E3',
      bg: {
        color: '#2B3035',
      },
      border: {
        color: transparent,
      },
    },
    active: {
      color: '#DDDEDF',
      bg: {
        color: palette.accent[800],
      },
      border: {
        color: transparent,
      },
    },
    focus: {
      color: '#DDDEDF',
      bg: {
        color: transparent,
      },
      border: {
        color: transparent,
      },
    },
    selected: {
      color: '#DDDEDF',
      bg: {
        color: transparent,
      },
      border: {
        color: '#DDDEDF',
      },
    },
    disabled: {
      color: '#585C61',
      bg: {
        color: transparent,
      },
      border: {
        color: transparent,
      },
    },
  },
}
