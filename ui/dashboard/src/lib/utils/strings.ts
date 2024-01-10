export const getInputLabel = (label, required) => {
  return `${label}${required ? ' *' : ''}`
}
