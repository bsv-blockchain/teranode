import { InputStyleSize } from '../types/input'
import { toUnit } from '../utils/css'

export const RadioSize = {
  sm: 12,
  md: 14,
  lg: 18,
}

export const IconSize = {
  sm: 6,
  md: 6,
  lg: 8,
}

const sizes = Object.values(InputStyleSize)

export const radio = {
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          size: toUnit(RadioSize[size]),
          icon: {
            size: toUnit(IconSize[size]),
          },
        },
      }),
      {},
    ),
  },
}
