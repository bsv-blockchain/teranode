export interface SidebarSectionItem {
  icon?: string
  label: string
  path: string
  selected?: boolean
}

export interface SidebarSection {
  type: 'page-links' | 'product-links'
  variant: 'normal' | 'blue'
  title?: string
  items: SidebarSectionItem[]
}
