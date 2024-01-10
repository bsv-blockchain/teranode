export const isBool = (value) => {
  return typeof value === 'boolean'
}

export const isNull = (value) => {
  return value === null
}

export const isNum = (value) => {
  return typeof value === 'number'
}

export const isArray = (value) => {
  return Array.isArray(value)
}

export const isUndefined = (value) => {
  return typeof value === 'undefined'
}

export const valueSet = (value) => {
  return !isNull(value) && !isUndefined(value)
}

export const isValidDate = (value) => {
  return value instanceof Date && !isNaN(value as any)
}
