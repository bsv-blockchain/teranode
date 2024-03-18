export interface ColDef {
  id: string
  name?: string
  type?: string
  format?: string
  props: any
}

export enum TableVariant {
  div = 'div',
  standard = 'standard',
  dynamic = 'dynamic', // pseudo-variant, switching between standard and div variants around the table breakpoint
}

export type TableVariantType = `${TableVariant}`
