import { palette, transparent } from './constants/colour'

// import { InputStyleSize } from '$lib/styles/types/input'
import { toUnit } from '$lib/styles/utils/css'

export const SwitchWidth = {
  sm: 34,
  // sm: 32,
  // md: 32,
  // lg: 48,
}

export const SwitchHeight = {
  sm: 18,
  // sm: 16,
  // md: 18,
  // lg: 24,
}

export const SwitchPadding = {
  sm: 2,
  // sm: 2,
  // md: 2,
  // lg: 3,
}

export const IconSize = {
  sm: 14,
  // sm: 12,
  // md: 14,
  // lg: 18,
}

export const SwitchBorderWidth = {
  sm: 1,
  // sm: 12,
  // md: 1,
  // lg: 18,
}

export const FocusRectWidth = {
  sm: 1,
  // sm: 12,
  // md: 1,
  // lg: 18,
}

export const FocusRectPadding = {
  sm: 0,
  // sm: 12,
  // md: 0,
  // lg: 18,
}

// const sizes = Object.values(InputStyleSize)
const sizes = ['sm']

export const switchh = {
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          width: toUnit(SwitchWidth[size]),
          height: toUnit(SwitchHeight[size]),
          padding: toUnit(SwitchPadding[size]),
          icon: {
            size: toUnit(IconSize[size]),
          },
          border: {
            width: toUnit(SwitchBorderWidth[size]),
          },
          focus: {
            rect: {
              width: toUnit(FocusRectWidth[size]),
              padding: toUnit(FocusRectPadding[size]),
            },
          },
        },
      }),
      {},
    ),
  },
  default: {
    enabled: {
      color: '#dfecfe',
      bg: {
        color: '#565656',
      },
      border: {
        color: transparent,
      },
    },
    'enabled-checked': {
      color: '#dfecfe',
      bg: {
        color: '#1778FF',
      },
      border: {
        color: transparent,
      },
    },
    hover: {
      color: '#dfecfe',
      bg: {
        color: '#565656',
      },
      border: {
        color: '#dfecfe',
      },
    },
    'hover-checked': {
      color: '#dfecfe',
      bg: {
        color: '#1778FF',
      },
      border: {
        color: '#dfecfe',
      },
    },
    focused: {
      color: '#dfecfe',
      bg: {
        color: '#565656',
      },
      border: {
        color: '#dfecfe',
      },
    },
    'focused-checked': {
      color: '#dfecfe',
      bg: {
        color: '#1778FF',
      },
      border: {
        color: '#dfecfe',
      },
    },
    disabled: {
      color: '#737476',
      bg: {
        color: '#26282a',
      },
      border: {
        color: transparent,
      },
    },
    ['disabled-checked']: {
      color: '#91979f',
      bg: {
        color: '#28549e',
      },
      border: {
        color: transparent,
      },
    },
    invalid: {
      border: {
        color: palette.danger[600],
      },
    },
  },
}
