import { valueSet } from './types'

export const formatDate = (str, showSeconds = true, showTime = true) => {
  if (str === '0001-01-01T00:00:00.000Z') {
    return ''
  }
  return !showTime
    ? new Date(str).toISOString().slice(0, 10)
    : showSeconds
      ? new Date(str).toISOString().replace('T', ' ').replace('Z', '').split('.')[0]
      : new Date(str).toISOString().replace('T', ' ').slice(0, 16)
}

export const addNumCommas = (value, round = -1, defaultValue = '') => {
  if (!valueSet(value)) {
    return defaultValue
  }
  if (round !== -1) {
    value = value.toFixed(round)
  }
  return new Intl.NumberFormat().format(value)
}

export const formatNum = (value, defaultValue = '') => {
  return valueSet(value) ? addNumCommas(value) : defaultValue
}

export const formatSatoshi = (value, defaultValue = '') => {
  return valueSet(value) ? addNumCommas(value / 1e8, 8) : defaultValue
}

export const formatTransactionHash = (value, defaultValue = '') => {
  return valueSet(value)
    ? `${value.substring(0, 5)}...${value.substring(value.length - 5, value.length)}`
    : defaultValue
}

export const shortHash = (value, defaultValue = '') => {
  return valueSet(value)
    ? `${value.substring(0, 4)}...${value.substring(value.length - 8)}`
    : defaultValue
}

export const humanHash = (val) => {
  let unit = 'H/s'

  if (isNaN(val)) {
    return '-'
  }

  if (val >= 1e21) {
    val = val / 1e21
    unit = 'ZH/s'
  } else if (val >= 1e18) {
    val = val / 1e18
    unit = 'EH/s'
  } else if (val >= 1e15) {
    val = val / 1e15
    unit = 'PH/s'
  } else if (val >= 1e12) {
    val = val / 1e12
    unit = 'TH/s'
  } else if (val >= 1e9) {
    val = val / 1e9
    unit = 'GH/s'
  } else if (val >= 1e6) {
    val = val / 1e6
    unit = 'MH/s'
  } else if (val >= 1e3) {
    val = val / 1e3
    unit = 'kH/s'
  }

  return val.toFixed(2) + ' ' + unit
}

export const petaHash = (val) => {
  if (isNaN(val)) {
    return '-'
  }
  return (val / 1e15).toFixed(2) + ' PH/s'
}

export const memSize = (val) => {
  let unit = 'B'

  if (isNaN(val)) {
    return '-'
  }

  if (val >= 1.1258999068426e15) {
    val = val / 1.1258999068426e15
    unit = 'PB'
  } else if (val >= 1099511627776) {
    val = val / 1099511627776
    unit = 'TB'
  } else if (val >= 1073741824) {
    val = val / 1073741824
    unit = 'GB'
  } else if (val >= 1048576) {
    val = val / 1048576
    unit = 'MB'
  } else if (val >= 1024) {
    val = val / 1024
    unit = 'kB'
  }

  return addNumCommas(val, 2) + ' ' + unit
}

export const dataSize = (val) => {
  let unit = 'B'

  if (isNaN(val)) {
    return '-'
  }

  if (val >= 1e15) {
    val = val / 1e15
    unit = 'PB'
  } else if (val >= 1e12) {
    val = val / 1e12
    unit = 'TB'
  } else if (val >= 1e9) {
    val = val / 1e9
    unit = 'GB'
  } else if (val >= 1e6) {
    val = val / 1e6
    unit = 'MB'
  } else if (val >= 1000) {
    val = val / 1000
    unit = 'kB'
  }

  return addNumCommas(val, 2) + ' ' + unit
}

export const link = (
  href,
  text: string | null = null,
  className: string | null = null,
  external = true,
) => {
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
  return `<a href='${prefix + href}'${
    external ? " target='_blank' rel='noopener noreferrer'" : ''
  }${className ? ` class='${className}'` : ''}>${text || href}</a>`
}

const getSuperStr = (input: string) => {
  const superChars = '⁰¹²³⁴⁵⁶⁷⁸⁹'
  return [...input].map((char) => superChars[char.charCodeAt(0) - 48]).join('')
}

export const formatNumberExp = (val: number, html = true) => {
  let exp = ''

  if (val >= 1e15) {
    val = val / 1e15
    exp = html ? ' × 10<sup>15</sup>' : ' × 10¹⁵'
  } else if (val >= 1e12) {
    val = val / 1e12
    exp = html ? ' × 10<sup>12</sup>' : ' × 10¹²'
  } else if (val >= 1e9) {
    val = val / 1e9
    exp = html ? ' × 10<sup>9</sup>' : ' × 10⁹'
  } else if (val >= 1e6) {
    val = val / 1e6
    exp = html ? ' × 10<sup>6</sup>' : ' × 10⁶'
  } else if (val >= 1e3) {
    val = val / 1e3
    exp = html ? ' × 10<sup>3</sup>' : ' × 10³'
  } else if (val > 0 && val <= 1 / 1e3) {
    const str = val.toExponential().toString()
    const index = str.indexOf('e')
    if (index !== -1) {
      val = parseFloat(str.substring(0, index))
      exp = html
        ? ` × 10<sup>${'-' + str.substring(index + 2, str.length)}</sup>`
        : ` × 10${'⁻' + getSuperStr(str.substring(index + 2, str.length))}`
    }
  }

  return {
    value: Math.round(val * 1000) / 1000,
    exp,
  }
}

export const formatNumberExpStr = (val: number, html = true) => {
  const parts = formatNumberExp(val, html)
  return parts.value + parts.exp
}

export const formatLargeNumber = (val: number) => {
  let unit = ''

  if (val >= 1e15) {
    val = val / 1e15
    unit = ' P'
  } else if (val >= 1e12) {
    val = val / 1e12
    unit = ' T'
  } else if (val >= 1e9) {
    val = val / 1e9
    unit = ' G'
  } else if (val >= 1e6) {
    val = val / 1e6
    unit = ' M'
  } else if (val >= 1e3) {
    val = val / 1e3
    unit = ' K'
  }

  return {
    value: Math.round(val * 1000) / 1000,
    unit,
  }
}

export const formatLargeNumberStr = (val: number, decimals = -1) => {
  const parts = formatLargeNumber(val)
  return decimals !== -1 ? parts.value.toFixed(decimals) + parts.unit : parts.value + parts.unit
}
