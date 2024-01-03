import { rem } from 'polished'
// import { palette, white } from './colour'

export const fontFamily = {
  // inter: 'Inter',
  jetbrainsMono: 'JetBrains Mono',
  satoshi: 'Satoshi',
}

export const fontSize = {
  xxs: '9px',
  xs: '10px',
  sm: '12px',
  md: '14px',
  mmd: '16px',
  lg: '18px',
  xl: '24px',
  xxl: '32px',
  xxxl: '48px',
  xxxxl: '60px',
  xxxxxl: '72px',
}

export const fontSizeRem = Object.keys(fontSize).reduce(
  (acc, key) => ({ ...acc, [key]: rem(fontSize[key]) }),
  {},
)

export const lineHeight = {
  xxs: '12px',
  xs: '14px',
  sm: '18px',
  md: '20px',
  mmd: '24px',
  lg: '28px',
  xl: '32px',
  xxl: '40px',
  xxxl: '60px',
  xxxxl: '72px',
  xxxxxl: '80px',
}

export const lineHeightRem = Object.keys(lineHeight).reduce(
  (acc, key) => ({ ...acc, [key]: rem(lineHeight[key]) }),
  {},
)

export const fontWeight = {
  // thin: 100,
  // extraLight: 200,
  // light: 300,
  regular: 400,
  // medium: 500,
  // semibold: 600,
  bold: 700,
  // extraBold: 800,
  // black: 900,
}

export const letterSpacing = {
  sm: '0.01em',
}

///////////

export const getTypoProps = (obj: any, size: string) => {
  return {
    color: obj.color,
    font: {
      ...obj.font,
      size: obj[size].font.size,
    },
    line: {
      ...obj[size].line,
    },
    letter: {
      ...obj[size].letter,
    },
  }
}

export const title = {
  color: 'var(--app-color)',
  font: {
    family: fontFamily.satoshi,
    weight: fontWeight.bold,
  },
  h1: {
    font: {
      size: '40px',
    },
    line: {
      height: '48px',
    },
    letter: {
      spacing: '0.8px',
    },
  },
  h2: {
    font: {
      size: '32px',
    },
    line: {
      height: '48px',
    },
    letter: {
      spacing: '0.64px',
    },
  },
  h3: {
    font: {
      size: '24px',
    },
    line: {
      height: '32px',
    },
    letter: {
      spacing: '0.48px',
    },
  },
  h4: {
    font: {
      size: '22px',
    },
    line: {
      height: '28px',
    },
    letter: {
      spacing: '0.44px',
    },
  },
  h5: {
    font: {
      size: '17px',
    },
    line: {
      height: '24px',
    },
    letter: {
      spacing: '0.34px',
    },
  },
}

export const text = {
  color: 'var(--app-color)',
  font: {
    family: fontFamily.satoshi,
    weight: fontWeight.regular,
  },
  sm: {
    font: {
      size: '13px',
    },
    line: {
      height: '18px',
    },
    letter: {
      spacing: '0.26px',
    },
  },
  md: {
    font: {
      size: '15px',
    },
    line: {
      height: '24px',
    },
    letter: {
      spacing: '0.3px',
    },
  },
  lg: {
    font: {
      size: '17px',
    },
    line: {
      height: '24px',
    },
    letter: {
      spacing: '0.34px',
    },
  },
  xl: {
    font: {
      size: '22px',
    },
    line: {
      height: '28px',
    },
    letter: {
      spacing: '0.44px',
    },
  },
}

///////////

// // display

// export const display = {
//   color: 'var(--app-color)', //palette.gray[800],
//   font: {
//     family: fontFamily.satoshi,
//     weight: fontWeight.regular,
//   },
//   1: {
//     font: {
//       size: fontSize.xxxxxl,
//     },
//     line: {
//       height: lineHeight.xxxxxl,
//     },
//   },
//   2: {
//     font: {
//       size: fontSize.xxxxl,
//     },
//     line: {
//       height: lineHeight.xxxxl,
//     },
//   },
//   3: {
//     font: {
//       size: fontSize.xxl,
//     },
//     line: {
//       height: lineHeight.xxl,
//     },
//   },
// }

// // heading

// export const heading = {
//   color: 'var(--app-color)', //palette.gray[800],
//   font: {
//     family: fontFamily.satoshi,
//     weight: fontWeight.semibold,
//   },
//   1: {
//     font: {
//       size: fontSize.xxxl,
//     },
//     line: {
//       height: lineHeight.xxxl,
//     },
//   },
//   2: {
//     font: {
//       size: fontSize.xxl,
//     },
//     line: {
//       height: lineHeight.xxl,
//     },
//   },
//   3: {
//     font: {
//       size: fontSize.xl,
//     },
//     line: {
//       height: lineHeight.xl,
//     },
//   },
//   4: {
//     font: {
//       size: fontSize.lg,
//     },
//     line: {
//       height: lineHeight.mmd,
//     },
//   },
//   5: {
//     font: {
//       size: fontSize.mmd,
//     },
//     line: {
//       height: lineHeight.mmd,
//     },
//     letter: {
//       spacing: letterSpacing.sm,
//     },
//   },
//   6: {
//     font: {
//       size: fontSize.md,
//     },
//     line: {
//       height: lineHeight.md,
//     },
//     letter: {
//       spacing: letterSpacing.sm,
//     },
//   },
//   7: {
//     font: {
//       size: fontSize.sm,
//     },
//     line: {
//       height: lineHeight.sm,
//     },
//     letter: {
//       spacing: letterSpacing.sm,
//     },
//   },
//   8: {
//     font: {
//       size: fontSize.xs,
//     },
//     line: {
//       height: lineHeight.xs,
//     },
//     letter: {
//       spacing: letterSpacing.sm,
//     },
//   },
//   9: {
//     font: {
//       size: fontSize.xxs,
//     },
//     line: {
//       height: lineHeight.xxs,
//     },
//     letter: {
//       spacing: letterSpacing.sm,
//     },
//   },
// }

// // body

// export const body = {
//   color: 'var(--app-color)', //palette.gray[800],
//   font: {
//     family: fontFamily.satoshi,
//     weight: fontWeight.regular,
//   },
//   1: {
//     font: {
//       size: fontSize.lg,
//     },
//     line: {
//       height: lineHeight.lg,
//     },
//   },
//   2: {
//     font: {
//       size: fontSize.mmd,
//     },
//     line: {
//       height: lineHeight.mmd,
//     },
//   },
//   3: {
//     font: {
//       size: fontSize.md,
//     },
//     line: {
//       height: lineHeight.md,
//     },
//   },
//   4: {
//     font: {
//       size: fontSize.sm,
//     },
//     line: {
//       height: lineHeight.sm,
//     },
//   },
// }

// // numeric

// export const numeric = {
//   color: 'var(--app-color)', //palette.gray[800],
//   font: {
//     family: fontFamily.satoshi,
//     weight: fontWeight.light,
//   },
//   1: {
//     font: {
//       size: fontSize.mmd,
//     },
//     line: {
//       height: lineHeight.mmd,
//     },
//   },
//   2: {
//     font: {
//       size: fontSize.md,
//     },
//     line: {
//       height: lineHeight.md,
//     },
//   },
//   3: {
//     font: {
//       size: fontSize.sm,
//     },
//     line: {
//       height: lineHeight.sm,
//     },
//   },
// }
