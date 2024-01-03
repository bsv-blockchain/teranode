import { palette, white } from '../constants/colour'

import { toUnit } from '../utils/css'
import { InputStyleSize } from '../types/input'
import { elevation } from '../constants/elevation'

export const ListItemPaddingX = {
  sm: 8,
  md: 8,
  lg: 8,
}

export const ListItemPaddingY = {
  sm: 0,
  md: 0,
  lg: 0,
}

const sizes = Object.values(InputStyleSize)

export const dropdown = {
  list: {
    bg: {
      color: elevation.bg.color,
    },
    border: {
      radius: elevation.border.radius,
    },
    ...elevation[2],
    padding: toUnit(8),
    item: {
      enabled: {
        bg: {
          color: white,
        },
      },
      hover: {
        bg: {
          color: palette.primary[50],
        },
      },
      selected: {
        bg: {
          color: palette.primary[100],
        },
      },
    },
  },
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          list: {
            item: {
              padding: `${toUnit(ListItemPaddingY[size])} ${toUnit(ListItemPaddingX[size])}`,
            },
          },
        },
      }),
      {},
    ),
  },
}
