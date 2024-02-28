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
}

export type TableVariantType = `${TableVariant}`
