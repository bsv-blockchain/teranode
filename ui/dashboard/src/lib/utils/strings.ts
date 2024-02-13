export const getInputLabel = (label, required) => {
  return `${label}${required ? ' *' : ''}`
}

export const lpad = (str: string, len: number, chr = '0') => {
  return str.length < len ? chr.repeat(len - str.length) + str : str
}

export const rpad = (str: string, len: number, chr = '0') => {
  return str.length < len ? str + chr.repeat(len - str.length) : str
}
