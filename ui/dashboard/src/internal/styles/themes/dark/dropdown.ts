// import { palette, white } from '$lib/styles/constants/colour'

import { toUnit } from '$lib/styles/utils/css'
import { InputStyleSize } from '$lib/styles/types/input'
import { elevation } from '$lib/styles/constants/elevation'

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
      color: 'rgba(12, 17, 23, 1)', //elevation.bg.color,
    },
    border: {
      radius: elevation.border.radius,
      color: 'rgba(255,255,255,0.11)',
    },
    ...elevation[2],
    padding: toUnit(8),
    item: {
      enabled: {
        bg: {
          color: 'rgba(255, 255, 255, 0)', //white,
        },
      },
      hover: {
        bg: {
          color: 'rgba(255, 255, 255, 0.05)', //palette.primary[50],
        },
      },
      selected: {
        bg: {
          color: 'rgba(255, 255, 255, 0.2)', // palette.primary[100],
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
