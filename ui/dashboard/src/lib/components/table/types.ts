export interface ColDef {
  id: string
  name?: string
  props: any
}

export enum TableVariant {
  div = 'div',
  standard = 'standard',
}

export type TableVariantType = `${TableVariant}`
