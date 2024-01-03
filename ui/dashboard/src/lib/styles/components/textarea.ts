import { toUnit } from '../utils/css'

export const TextAreaPaddingX = {
  sm: 16,
  md: 16,
  lg: 16,
}

export const TextAreaPaddingY = {
  sm: 7,
  md: 10,
  lg: 14,
}

const sizes = Object.keys(TextAreaPaddingX)

export const textarea = {
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          padding: `${toUnit(TextAreaPaddingX[size])} ${toUnit(TextAreaPaddingY[size])}`,
        },
      }),
      {},
    ),
  },
}
