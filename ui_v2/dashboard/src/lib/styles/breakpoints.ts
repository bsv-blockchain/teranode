import { MediaSize } from '../stores/media'
// import { query } from '../actions'

export enum Breakpoint {
  xs = 'xs',
  sm = 'sm',
  md = 'md',
  lg = 'lg',
  xl = 'xl',
}

export const getBreakpointForMediaSize = (mediaSize: number) => {
  switch (mediaSize) {
    case MediaSize.xs:
      return Breakpoint.xs
    case MediaSize.sm:
      return Breakpoint.sm
    case MediaSize.md:
      return Breakpoint.md
    case MediaSize.lg:
      return Breakpoint.lg
    case MediaSize.xl:
      return Breakpoint.xl
  }
}

export const breakpoints = {
  xs: '375px',
  sm: '768px',
  md: '1180px',
  lg: '1440px',
  xl: '1920px',
}

export const xs = `(min-width: ${breakpoints.xs})`
export const sm = `(min-width: ${breakpoints.sm})`
export const md = `(min-width: ${breakpoints.md})`
export const lg = `(min-width: ${breakpoints.lg})`
export const xl = `(min-width: ${breakpoints.xl})`
