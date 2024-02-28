import { toUnit } from '$lib/styles/utils/css'

export const footer = {
  height: toUnit(60),
  bg: {
    color: 'transparent',
  },
  border: {
    color: 'rgba(255, 255, 255, 0.08)',
  },
  link: {
    color: 'rgba(255, 255, 255, 0.66)',
    hover: {
      color: 'rgba(255, 255, 255, 0.88)',
    },
  },
}
