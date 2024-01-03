export interface I18n {
  t: (key: string, params?: any) => any
  baseKey: string
  keyMap?: any
}
