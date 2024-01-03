import { ComponentStyleSize } from '$lib/styles/types'
import { transparent } from './constants/colour'
import { toUnit } from '$lib/styles/utils/css'
import {
  ComponentFontProps,
  ComponentBorderRadius,
  ComponentFocusRectBorderRadius,
  ComponentFocusRectWidth,
  ComponentIcoPaddingX,
  ComponentIcoMarginX,
  ComponentIcoMarginY,
  ComponentHeight,
  ComponentIconSize,
  ComponentPaddingX,
  ComponentPaddingY,
} from './defaults'

const sizes = Object.values(ComponentStyleSize)

export const comp = {
  bg: {
    color: '#14181E',
  },
  color: '#ffffff',
  font: {
    family: 'Satoshi',
    weight: 400,
  },
  outline: 'none',
  focus: {
    rect: {
      color: '#ffffff', //palette.primary[500],
      width: toUnit(ComponentFocusRectWidth),
      border: {
        radius: toUnit(ComponentFocusRectBorderRadius),
      },
      padding: '0', //toUnit(0),
      bg: {
        color: '#ffffff',
      },
    },
  },
  label: {
    gap: toUnit(6),
    color: 'rgba(255,255,255,0.66)', //palette.gray[800],
    disabled: {
      color: 'rgba(255,255,255,0.4)', //palette.gray[500],
    },
  },
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          height: toUnit(ComponentHeight[size]),
          padding: `${toUnit(ComponentPaddingY[size])} ${toUnit(ComponentPaddingX[size])}`,
          ico: {
            padding: `${toUnit(0)} ${toUnit(ComponentIcoPaddingX[size])} `,
            margin: `${toUnit(ComponentIcoMarginY[size])} 0 0 ${toUnit(
              ComponentIcoMarginX[size],
            )} `,
          },
          border: {
            radius: toUnit(ComponentBorderRadius[size]),
          },
          icon: {
            size: toUnit(ComponentIconSize[size]),
          },
          ...ComponentFontProps[size],
        },
      }),
      {},
    ),
  },
  primary: {
    enabled: {
      color: '#ecf1fd',
      bg: {
        color: '#292e34',
      },
      border: {
        color: '#292e34',
      },
    },
    hover: {
      color: '#ecf1fd',
      bg: {
        color: '#393e45',
      },
      border: {
        color: '#393e45',
      },
    },
    active: {
      color: '#ecf1fd',
      bg: {
        color: '#51565f',
      },
      border: {
        color: '#51565f',
      },
    },
    focus: {
      color: '#ecf1fd',
      bg: {
        color: '#51565f',
      },
      border: {
        color: '#51565f',
      },
    },
    disabled: {
      color: '#7d818b',
      bg: {
        color: '#1c2027',
      },
      border: {
        color: '#1c2027',
      },
    },
  },
  secondary: {
    enabled: {
      color: '#ce1722',
      bg: {
        color: '#ecf1fd',
      },
      border: {
        color: '#ecf1fd',
      },
    },
    hover: {
      color: '#cb141f',
      bg: {
        color: '#ecf1fd',
      },
      border: {
        color: '#ecf1fd',
      },
    },
    active: {
      color: '#c0020e',
      bg: {
        color: '#ecf1fd',
      },
      border: {
        color: '#ecf1fd',
      },
    },
    focus: {
      color: '#c0020e',
      bg: {
        color: '#ecf1fd',
      },
      border: {
        color: '#ecf1fd',
      },
    },
    disabled: {
      color: '#ce1722',
      bg: {
        color: '#7d818b',
      },
      border: {
        color: '#7d818b',
      },
    },
  },
  tertiary: {
    enabled: {
      color: '#eff3fd',
      bg: {
        color: '#1778ff',
      },
      border: {
        color: '#1778ff',
      },
    },
    hover: {
      color: '#eff3fd',
      bg: {
        color: '#1758ff',
      },
      border: {
        color: '#1758ff',
      },
    },
    active: {
      color: '#eff3fd',
      bg: {
        color: '#0039f6',
      },
      border: {
        color: '#0039f6',
      },
    },
    focus: {
      color: '#eff3fd',
      bg: {
        color: '#0039f6',
      },
      border: {
        color: '#0039f6',
      },
    },
    disabled: {
      color: '#6d717b',
      bg: {
        color: '#14397a',
      },
      border: {
        color: '#14397a',
      },
    },
  },
  destructive: {
    enabled: {
      color: '#eff3fd',
      bg: {
        color: '#ce1722',
      },
      border: {
        color: '#ce1722',
      },
    },
    hover: {
      color: '#eff3fd',
      bg: {
        color: '#cb141f',
      },
      border: {
        color: '#cb141f',
      },
    },
    active: {
      color: '#eff3fd',
      bg: {
        color: '#c0020e',
      },
      border: {
        color: '#c0020e',
      },
    },
    focus: {
      color: '#eff3fd',
      bg: {
        color: '#c0020e',
      },
      border: {
        color: '#c0020e',
      },
    },
    disabled: {
      color: '#6d717b',
      bg: {
        color: '#5c151d',
      },
      border: {
        color: '#5c151d',
      },
    },
  },
  tool: {
    enabled: {
      color: '#ecf1fd',
      bg: {
        color: transparent,
      },
      border: {
        color: transparent,
      },
    },
    hover: {
      color: '#ecf1fd',
      bg: {
        color: 'rgba(255, 255, 255, 0.11)',
      },
      border: {
        color: transparent,
      },
    },
    active: {
      color: 'rgba(10, 16, 24, 0.88)',
      bg: {
        color: 'rgba(255, 255, 255, 0.88)',
      },
      border: {
        color: transparent,
      },
    },
    focus: {
      color: 'rgba(10, 16, 24, 0.88)',
      bg: {
        color: 'rgba(255, 255, 255, 0.88)',
      },
      border: {
        color: transparent,
      },
    },
    disabled: {
      color: '#7d818b',
      bg: {
        color: '#1c2027',
      },
      border: {
        color: transparent,
      },
    },
  },
}
