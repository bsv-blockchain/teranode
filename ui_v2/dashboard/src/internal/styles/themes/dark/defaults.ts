import { getTypoProps, text } from './constants/typography'

export const ComponentHeight = {
  xs: 20,
  sm: 24,
  md: 32,
  lg: 48,
  xl: 60,
}

export const ComponentFontProps = {
  xs: getTypoProps(text, 'sm'),
  sm: getTypoProps(text, 'sm'),
  md: getTypoProps(text, 'md'),
  lg: getTypoProps(text, 'lg'),
  xl: getTypoProps(text, 'lg'),
}

export const ComponentFontSize = {
  xs: 13,
  sm: 13,
  md: 15,
  lg: 17,
  xl: 17,
}

export const ComponentLineHeight = {
  xs: 18,
  sm: 18,
  md: 24,
  lg: 24,
  xl: 24,
}

export const ComponentLetterSpacing = '0.01em'

export const ComponentIconSize = {
  xs: 13,
  sm: 13,
  md: 16,
  lg: 24,
  xl: 24,
}

export const ComponentBorderRadius = {
  xs: 4,
  sm: 4,
  md: 6,
  lg: 6,
  xl: 6,
}

export const ComponentBorderWidth = 1

export const ComponentFocusRectWidth = 1

export const ComponentFocusRectBorderRadius = 6

export const ComponentPaddingX = {
  xs: 8,
  sm: 8, //24,
  md: 24,
  lg: 16,
  xl: 16,
}

export const ComponentPaddingY = {
  xs: 0,
  sm: 0, //16,
  md: 0, //16,
  lg: 16,
  xl: 16,
}

export const ComponentIcoPaddingX = {
  xs: 2, //(ComponentHeight.xs - ComponentIconSize.xs) / 2,
  sm: (ComponentHeight.sm - ComponentIconSize.sm) / 2,
  md: (ComponentHeight.md - ComponentIconSize.md) / 2,
  lg: (ComponentHeight.lg - ComponentIconSize.lg) / 2,
  xl: (ComponentHeight.xl - ComponentIconSize.xl) / 2,
}

export const ComponentIcoMarginX = {
  xs: 1,
  sm: 1,
  md: 0,
  lg: 0,
  xl: 0,
}
export const ComponentIcoMarginY = {
  xs: -2,
  sm: -2,
  md: -4,
  lg: 0,
  xl: 0,
}
