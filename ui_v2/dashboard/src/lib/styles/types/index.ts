export enum ComponentSize {
  xsmall = 'xsmall',
  small = 'small',
  medium = 'medium',
  large = 'large',
  xlarge = 'xlarge',
}
export type ComponentSizeType = `${ComponentSize}`

export enum ComponentStyleSize {
  xs = 'xs',
  sm = 'sm',
  md = 'md',
  lg = 'lg',
  xl = 'xl',
}
export type ComponentStyleSizeType = `${ComponentStyleSize}`

export const getStyleSizeFromComponentSize = (cs: ComponentSizeType) => {
  switch (cs) {
    case ComponentSize.xsmall:
      return ComponentStyleSize.xs
    case ComponentSize.small:
      return ComponentStyleSize.sm
    case ComponentSize.medium:
      return ComponentStyleSize.md
    case ComponentSize.large:
      return ComponentStyleSize.lg
    case ComponentSize.xlarge:
      return ComponentStyleSize.xl
  }
}

export const getComponentSizeUp = (cs: ComponentSize) => {
  switch (cs) {
    case ComponentSize.xsmall:
      return ComponentSize.small
    case ComponentSize.small:
      return ComponentSize.medium
    case ComponentSize.medium:
      return ComponentSize.large
    case ComponentSize.large:
      return ComponentSize.xlarge
    case ComponentSize.xlarge:
      return ComponentSize.xlarge
  }
}

export const getComponentSizeDown = (cs: ComponentSize) => {
  switch (cs) {
    case ComponentSize.xsmall:
      return ComponentSize.xsmall
    case ComponentSize.small:
      return ComponentSize.xsmall
    case ComponentSize.medium:
      return ComponentSize.small
    case ComponentSize.large:
      return ComponentSize.medium
    case ComponentSize.xlarge:
      return ComponentSize.large
  }
}

export enum ComponentState {
  enabled = 'enabled',
  hover = 'hover',
  active = 'active',
  focus = 'focus',
  disabled = 'disabled',
}
export type ComponentStateType = `${ComponentState}`

export enum ComponentVariant {
  primary = 'primary',
  secondary = 'secondary',
  tertiary = 'tertiary',
  ghost = 'ghost',
  destructive = 'destructive',
  tool = 'tool',
}
export type ComponentVariantType = `${ComponentVariant}`

export enum FlexDirection {
  row = 'row',
  rowReverse = 'row-reverse',
  column = 'column',
  columnReverse = 'column-reverse',
}
export type FlexDirectionType = `${FlexDirection}`

export enum LabelPlacement {
  top = 'top',
  bottom = 'bottom',
  left = 'left',
  right = 'right',
}
export type LabelPlacementType = `${LabelPlacement}`

export enum LabelAlignment {
  start = 'start',
  center = 'center',
  end = 'end',
}
export type LabelAlignmentType = `${LabelAlignment}`
