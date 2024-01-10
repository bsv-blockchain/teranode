import { rem } from 'polished'

export const setCSSVariablesFromObj = (paths: string[] = [], obj, themeNs) => {
  for (const key in obj) {
    if (typeof obj[key] === 'string' || typeof obj[key] === 'number') {
      document.documentElement.style.setProperty(
        `--${themeNs ? `${themeNs}-` : ''}${paths.length ? `${paths.join('-')}-${key}` : key}`,
        `${obj[key]}`,
      )
    } else {
      setCSSVariablesFromObj([...paths, key], obj[key], themeNs)
    }
  }
}

export const setCSSVariables = (theme, themeNs) => {
  setCSSVariablesFromObj([], theme, themeNs)
}

export const toPx = (pxValue: number) => {
  return `${pxValue}px`
}

export const toRem = (pxValue: number) => {
  return rem(toPx(pxValue))
}

export const toUnit = (pxValue: number, unit: 'px' | 'rem' = 'px') => {
  return unit === 'px' ? toPx(pxValue) : toRem(pxValue)
}
