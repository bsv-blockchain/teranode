export enum InputSize {
  small = 'small',
  medium = 'medium',
  large = 'large',
}
export type InputSizeType = `${InputSize}`

export enum InputStyleSize {
  sm = 'sm',
  md = 'md',
  lg = 'lg',
}
export type InputStyleSizeType = `${InputStyleSize}`

export const getStyleSizeFromInputSize = (cs: InputSizeType) => {
  switch (cs) {
    case InputSize.small:
      return InputStyleSize.sm
    case InputSize.medium:
      return InputStyleSize.md
    case InputSize.large:
      return InputStyleSize.lg
  }
}

export enum InputState {
  enabled = 'enabled',
  hover = 'hover',
  active = 'active',
  focus = 'focus',
  disabled = 'disabled',
  invalid = 'invalid',
}

export enum InputVariant {
  default = 'default',
}
export type InputVariantType = `${InputVariant}`
