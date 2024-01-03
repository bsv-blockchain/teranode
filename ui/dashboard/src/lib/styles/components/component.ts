import { palette, transparent, white } from '../constants/colour'

import {
  ComponentBorderRadius,
  ComponentBorderWidth,
  ComponentFocusRectBorderRadius,
  ComponentFocusRectWidth,
  ComponentFontSize,
  ComponentHeight,
  ComponentIconSize,
  ComponentLineHeight,
  ComponentLetterSpacing,
  ComponentPaddingX,
  ComponentPaddingY,
} from './defaults'
import { toUnit } from '../utils/css'
import { ComponentStyleSize } from '../types'
import { fontFamily, fontWeight } from '../constants/typography'

const sizes = Object.values(ComponentStyleSize)

export const component = {
  box: {
    sizing: 'border-box',
  },
  font: {
    family: fontFamily.inter,
    weight: fontWeight.semibold,
  },
  border: {
    width: toUnit(ComponentBorderWidth),
  },
  bg: {
    color: '#ffffff',
  },
  color: '#282933',
  focus: {
    rect: {
      color: palette.primary[500],
      width: toUnit(ComponentFocusRectWidth),
      border: {
        radius: toUnit(ComponentFocusRectBorderRadius),
      },
      padding: toUnit(0),
      bg: {
        color: white,
      },
    },
  },
  label: {
    gap: toUnit(6),
    color: palette.gray[800],
    disabled: {
      color: palette.gray[500],
    },
  },
  footnote: {
    gap: toUnit(6),
    color: palette.gray[800],
    error: {
      color: palette.danger[600],
    },
    disabled: {
      color: palette.gray[500],
    },
  },
  size: {
    ...sizes.reduce(
      (acc, size) => ({
        ...acc,
        [size]: {
          height: toUnit(ComponentHeight[size]),
          padding: `${toUnit(ComponentPaddingY[size])} ${toUnit(ComponentPaddingX[size])}`,
          border: {
            radius: toUnit(ComponentBorderRadius[size]),
          },
          icon: {
            size: toUnit(ComponentIconSize[size]),
          },
          font: {
            size: toUnit(ComponentFontSize[size]),
          },
          line: {
            height: toUnit(ComponentLineHeight[size]),
          },
          letter: {
            spacing: ComponentLetterSpacing,
          },
        },
      }),
      {},
    ),
  },
  primary: {
    enabled: {
      color: palette.gray[800],
      bg: {
        color: palette.accent[600],
      },
      border: {
        color: transparent,
      },
    },
    hover: {
      color: palette.gray[800],
      bg: {
        color: palette.accent[700],
      },
      border: {
        color: transparent,
      },
    },
    active: {
      color: palette.gray[800],
      bg: {
        color: palette.accent[800],
      },
      border: {
        color: transparent,
      },
    },
    focus: {
      color: palette.gray[800],
      bg: {
        color: palette.accent[600],
      },
      border: {
        color: white,
      },
    },
    disabled: {
      color: palette.gray[500],
      bg: {
        color: palette.gray[200],
      },
      border: {
        color: transparent,
      },
    },
  },
  secondary: {
    enabled: {
      color: white,
      bg: {
        color: palette.primary[600],
      },
      border: {
        color: transparent,
      },
    },
    hover: {
      color: white,
      bg: {
        color: palette.primary[700],
      },
      border: {
        color: transparent,
      },
    },
    active: {
      color: white,
      bg: {
        color: palette.primary[800],
      },
      border: {
        color: transparent,
      },
    },
    focus: {
      color: white,
      bg: {
        color: palette.primary[600],
      },
      border: {
        color: white,
      },
    },
    disabled: {
      color: palette.gray[500],
      bg: {
        color: palette.gray[200],
      },
      border: {
        color: transparent,
      },
    },
  },
  tertiary: {
    enabled: {
      color: palette.primary[600],
      bg: {
        color: white,
      },
      border: {
        color: palette.primary[600],
      },
    },
    hover: {
      color: white,
      bg: {
        color: palette.primary[600],
      },
      border: {
        color: transparent,
      },
    },
    active: {
      color: white,
      bg: {
        color: palette.primary[700],
      },
      border: {
        color: transparent,
      },
    },
    focus: {
      color: white,
      bg: {
        color: palette.primary[600],
      },
      border: {
        color: white,
      },
    },
    disabled: {
      color: palette.gray[400],
      bg: {
        color: white,
      },
      border: {
        color: palette.gray[400],
      },
    },
  },
  ghost: {
    enabled: {
      color: palette.primary[600],
      bg: {
        color: transparent,
      },
      border: {
        color: transparent,
      },
    },
    hover: {
      color: palette.primary[600],
      bg: {
        color: palette.primary[100],
      },
      border: {
        color: transparent,
      },
    },
    active: {
      color: palette.primary[600],
      bg: {
        color: palette.primary[200],
      },
      border: {
        color: transparent,
      },
    },
    focus: {
      color: palette.primary[600],
      bg: {
        color: white,
      },
      border: {
        color: white,
      },
    },
    disabled: {
      color: palette.gray[400],
      bg: {
        color: white,
      },
      border: {
        color: transparent,
      },
    },
  },
  destructive: {
    enabled: {
      color: white,
      bg: {
        color: palette.danger[600],
      },
      border: {
        color: transparent,
      },
    },
    hover: {
      color: white,
      bg: {
        color: palette.danger[700],
      },
      border: {
        color: transparent,
      },
    },
    active: {
      color: white,
      bg: {
        color: palette.danger[800],
      },
      border: {
        color: transparent,
      },
    },
    focus: {
      color: white,
      bg: {
        color: palette.danger[600],
      },
      border: {
        color: white,
      },
    },
    disabled: {
      color: palette.gray[500],
      bg: {
        color: palette.gray[200],
      },
      border: {
        color: transparent,
      },
    },
  },
}
