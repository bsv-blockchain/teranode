export function getColorFromDistinct(value, distinctValues) {
  let colorFactor = 0

  if (distinctValues.length > 1) {
    const index = distinctValues.indexOf(value)
    const part = 360 / distinctValues.length

    colorFactor = index * part
  }

  return 'hsl(' + colorFactor + ', 100%, 70%)'
}
