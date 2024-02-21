import { formatDate, formatSatoshi, humanHash, petaHash, addNumCommas } from '$lib/utils/format'

const getMarker = (color) => {
  return `<span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background-color:${color};"></span>`
}

const formatValue = (value: any, format: string) => {
  let val: any = addNumCommas(value)
  switch (format) {
    case 'time':
    case 'createdate':
    case 'createDate':
      val = formatDate(value, false)
      break
    case 'satoshi':
      val = formatSatoshi(value)
      break
    case 'hashrate':
      val = humanHash(value)
      break
    case 'humanHash':
      val = humanHash(value)
      break
    case 'petaHash':
      val = petaHash(value)
      break
    default:
      val = addNumCommas(value)
  }
  return val
}

const formatField = (param, seriesName, mappers, value?) => {
  // get value
  let theValue = value
  if (param && param.dimensionNames) {
    const dataIndex = param.dimensionNames.indexOf(seriesName)
    theValue = param.data[dataIndex]
  }
  // get mapper
  let mapper: any = null
  const result =
    mappers && mappers.length > 0 ? mappers.filter((mapper) => mapper.series === seriesName) : null
  if (result && result.length > 0) {
    mapper = result[0]
  }
  // get name
  const name = mapper?.name ? mapper.name : seriesName
  const val = formatValue(theValue, mapper?.format)
  const color = mapper?.itemStyle?.color || 'white'
  const marker = getMarker(color)

  return { name, value: val, marker }
}

export const seriesTooltipFormatter = (mappers, params, ticket, callback) => {
  const titleObj = formatField(null, params[0].dimensionNames[0], mappers, parseInt(params[0].name))
  let res = titleObj.value
  for (let i = 0; i < params.length; i++) {
    const param = params[i]
    const fieldObj = formatField(param, param.seriesName, mappers)
    res += '<br/>' + param.marker + fieldObj.name + '：' + fieldObj.value
  }
  return res
}

export const timeSeriesTooltipFormatter = (mappers, params, ticket, callback) => {
  const obj = params[0]
  if (!obj.data) {
    return ''
  }

  const dimensionNames = obj.dimensionNames
  const fieldObjs: any[] = []

  dimensionNames.forEach((name, i) => {
    const matchResult = params.filter((param) => param.seriesName === name)
    if (name === 'Date' || name === 'Height' || matchResult.length > 0) {
      fieldObjs.push(formatField(null, name, mappers, obj.data[i]))
    }
  })

  let table = '<table><tbody>'
  for (let i = 0; i < fieldObjs.length; i++) {
    const fieldObj = fieldObjs[i]
    if (fieldObj.name) {
      table += `<tr><td>${fieldObj.marker + fieldObj.name}: </td><td>${fieldObj.value}</td></tr>`
    }
  }
  table += '</tbody></table>'
  return table
}
