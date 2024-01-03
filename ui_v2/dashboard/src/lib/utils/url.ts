export const getLinkPrefix = (href: string, external: boolean) => {
  let prefix = ''
  if (external) {
    try {
      const url = new URL(href)
      if (!url.protocol) {
        prefix = 'https://'
      }
    } catch (e) {
      prefix = 'https://'
    }
  }
  return prefix
}
